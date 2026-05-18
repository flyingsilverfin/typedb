/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Planner-focused integration tests. These build a small schema, populate
//! data via a `DataSpec`-driven loader so cardinalities are easy to control,
//! run a read query, and inspect the resulting `QueryProfile` to assert that
//! the planner picked a plan with O(1) advances-per-row in every step and/or
//! the expected plan shape.
//!
//! These tests are the "live" complement to the static cost-model unit tests
//! in `compiler::executable::match_::planner::vertex` — they exercise
//! `Cost::join` and friends against real plans on real (small) data.
//!
//! The variants below cover a matrix of statistical regimes against the same
//! two-side `has`-join shape, exercising different code paths of the new
//! per-side blended-out-cost model in `Cost::join`:
//!
//! - `has_2_join_balanced`               — baseline, no waste, full coverage
//! - `has_2_join_subset_with_post_filter`— the bug case (waste + gap on one side)
//! - `has_2_join_disjoint_domains`       — waste + gap, zero-output sanity
//! - `has_2_join_fk_fanout`              — asymmetric io_ratios, both clamp to 0 gap
//! - `has_2_join_selective_against_full` — waste-on-both vs lookup-of-1
//! - `has_2_join_many_to_many`           — io_ratio >> join_size, no penalty
//! - `has_2_join_empty`                  — degenerate / empty stats

use std::{collections::HashMap, sync::Arc};

use concept::{
    thing::{statistics::Statistics, thing_manager::ThingManager},
    type_::type_manager::{TypeManager, type_cache::TypeCache},
};
use durability::DurabilitySequenceNumber;
use encoding::graph::{
    definition::definition_key_generator::DefinitionKeyGenerator, thing::vertex_generator::ThingVertexGenerator,
};
use compiler::executable::match_::planner::conjunction_executable::{ExecutionStep, IntersectionStep};
use executor::{
    ExecutionInterrupt,
    pipeline::stage::{ExecutionContext, StageIterator},
};
use executor::pipeline::pipeline::Pipeline;
use executor::pipeline::stage::{ReadPipelineStage, StageAPI};
use function::function_manager::FunctionManager;
use query::options::QueryOptions;
use query::{query_cache::QueryCache, query_manager::QueryManager};
use resource::profile::{CommitProfile, PatternProfile, QueryProfile, StepProfile, SubstepProfile};
use storage::{MVCCStorage, durability_client::WALClient, snapshot::CommittableSnapshot};
use storage::durability_client::DurabilityClient;
use storage::snapshot::{ReadSnapshot, ReadableSnapshot};
use test_utils::TempDir;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

struct Context {
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: Arc<TypeManager>,
    thing_manager: Arc<ThingManager>,
    function_manager: FunctionManager,
    query_manager: QueryManager,
    _tmp_dir: TempDir,
}

impl Context {
    fn refresh(&mut self) {
        let mut statistics = Statistics::new(DurabilitySequenceNumber::MIN);
        statistics.may_synchronise(self.storage.as_ref()).unwrap();

        let definition_key_gen = self.type_manager.definition_key_generator();
        let vertex_gen = self.type_manager.type_vertex_generator();
        let cache = Arc::new(TypeCache::new(self.storage.clone(), self.storage.snapshot_watermark()).unwrap());
        let type_manager = Arc::new(TypeManager::new(definition_key_gen, vertex_gen, Some(cache)));

        let thing_vertex_generator = Arc::new(ThingVertexGenerator::load(self.storage.clone()).unwrap());
        let thing_manager =
            Arc::new(ThingManager::new(thing_vertex_generator, type_manager.clone(), Arc::new(statistics)));

        self.type_manager = type_manager;
        self.thing_manager = thing_manager;
        self.query_manager = QueryManager::new(Some(Arc::new(QueryCache::new())));
    }
}

fn setup() -> Context {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new(Some(Arc::new(QueryCache::new())));
    Context { _tmp_dir, storage, type_manager, function_manager, query_manager, thing_manager }
}

fn define_schema(context: &mut Context, query: &str) {
    let mut snapshot = context.storage.clone().open_snapshot_schema();
    let schema_query = typeql::parse_query(query).unwrap().into_structure().into_schema();
    context
        .query_manager
        .execute_schema(
            &mut snapshot,
            &context.type_manager,
            &context.thing_manager,
            &context.function_manager,
            schema_query,
            query,
            QueryOptions::default(),
        )
        .unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    context.refresh();
}

fn commit_writes(context: &mut Context, queries: &[String]) {
    let mut snapshot = context.storage.clone().open_snapshot_write();
    for query in queries {
        let parsed_query = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
        let pipeline = context
            .query_manager
            .prepare_write_pipeline(
                snapshot,
                &context.type_manager,
                context.thing_manager.clone(),
                &context.function_manager,
                &parsed_query,
                query,
                QueryOptions::default(),
            )
            .unwrap();
        // into_rows_iterator executes eagerly
        let (_iterator, exec_context) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
        snapshot = Arc::into_inner(exec_context.snapshot).unwrap();
    }
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    context.refresh();
}

fn compile_read(context: &Context, query: &str) -> Pipeline<ReadSnapshot<WALClient>, ReadPipelineStage<ReadSnapshot<WALClient>>> {
    let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
    let parsed_query = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_read_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &parsed_query,
            query,
            QueryOptions { force_query_profile: true },
        )
        .unwrap();
    pipeline
}

fn execute_read(pipeline: Pipeline<ReadSnapshot<WALClient>, ReadPipelineStage<ReadSnapshot<WALClient>>>) -> (usize, Arc<QueryProfile>) {
    let (iterator, ExecutionContext { profile, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let rows = iterator.collect_owned().unwrap().len();
    (rows, profile)
}

// --- DataSpec: declarative test-data builder -------------------------------------------------

struct DataSpec {
    instances: Vec<InstanceSpec>,
    has: Vec<HasSpec>,
}

struct InstanceSpec {
    type_: &'static str,
    count: usize,
    /// Optionally each instance a `has` edge to a key attribute using integers `0..count`.
    key: Option<&'static str>,
}

// NOTE: only integer right now
type AttributeGenerator = Box<dyn Fn(usize) -> i64>;

fn sequential() -> AttributeGenerator {
    Box::new(|i| i as i64)
}

fn cyclic(modulus: usize) -> AttributeGenerator {
    Box::new(move |i| (i % modulus) as i64)
}

fn offset_unique(start: i64) -> AttributeGenerator {
    Box::new(move |i| i as i64 + start)
}

struct HasSpec {
    owner_type: &'static str,
    attr_type: &'static str,
    /// Per-owner cap on the number of has's produced
    count_each: usize,
    /// Total number of `has` edges to produce - given to owners round-robin.
    count_total: usize,
    /// Maps edge index `0..count_total` to an integer attribute value. Repeating values
    attribute_generator: AttributeGenerator,
}

fn load_data(context: &mut Context, spec: DataSpec) {
    // Pre-validate: every unkeyed InstanceSpec must have a matching HasSpec that
    // will create its entities (unkeyed entities can't be addressed by a later
    // `match ... insert`, so we fold their creation into the HasSpec's combined
    // `insert $o isa T, has A V` statement). If no HasSpec covers an unkeyed
    // InstanceSpec, the entities would never be created — surface that as a hard
    // error so the test author isn't surprised by silently-empty data.
    for InstanceSpec { type_, count, key } in &spec.instances {
        if *count == 0 || key.is_some() {
            continue;
        }
        let has_matching = spec.has.iter().any(|h| h.owner_type == *type_ && h.count_total > 0);
        assert!(
            has_matching,
            "InstanceSpec for unkeyed type '{type_}' has no matching HasSpec — \
             entities would never be created. Either add a HasSpec, give the InstanceSpec a key, \
             or remove the InstanceSpec.",
        );
    }

    let mut instance_counts: HashMap<&'static str, usize> = HashMap::new();
    let mut queries: Vec<String> = Vec::new();

    for InstanceSpec { type_, count, key } in &spec.instances {
        if *count == 0 {
            continue;
        }
        // Keyed entities go in their own standalone insert here; unkeyed entities
        // are deferred to the HasSpec loop below, which creates them inline with
        // their attribute via `insert $o isa T, has A V`.
        if let Some(key_label) = key {
            let mut q = String::from("insert\n");
            for i in 0..*count {
                // typeql disallows underscore-prefixed variables, so name with a letter prefix.
                q.push_str(&format!("  $x_{type_}_{i} isa {type_}, has {key_label} {i};\n"));
            }
            queries.push(q);
        }
        *instance_counts.entry(*type_).or_insert(0) += *count;
    }

    for HasSpec { owner_type, attr_type, count_each, count_total, attribute_generator } in &spec.has {
        let owner_count = instance_counts.get(owner_type).copied().unwrap_or(0);
        assert!(
            owner_count > 0,
            "HasSpec references owner_type '{owner_type}' with no prior InstanceSpec inserts",
        );
        let max_edges = owner_count.saturating_mul(*count_each);
        assert!(
            *count_total <= max_edges,
            "HasSpec count_total={count_total} exceeds owner_count*count_each={max_edges} for type '{owner_type}'",
        );
        if *count_total == 0 {
            continue;
        }
        let owner_key = spec.instances.iter().find(|i| i.type_ == *owner_type).and_then(|i| i.key);
        match owner_key {
            Some(key_label) => {
                // Keyed path: address each owner by its key value and attach attributes via match-insert.
                // Assign edges to owners round-robin: edge `e` goes to owner `e % owner_count`.
                // This respects `count_each` (since count_total <= owner_count*count_each) and
                // keeps the per-owner distribution flat.
                let mut owner_edge_counts = vec![0usize; owner_count];
                for e in 0..*count_total {
                    let owner_idx = e % owner_count;
                    owner_edge_counts[owner_idx] += 1;
                    assert!(
                        owner_edge_counts[owner_idx] <= *count_each,
                        "internal: round-robin exceeded count_each for owner {owner_idx}",
                    );
                    let value = attribute_generator(e);
                    queries.push(format!(
                        "match $o isa {owner_type}, has {key_label} {owner_idx}; \
                         insert $o has {attr_type} {value};"
                    ));
                }
            }
            None => {
                // Unkeyed path: create entity+attribute pairs together since we can't
                // address pre-existing entities without a key. Constraint: this only
                // supports the "1 attribute per entity, total = owner_count" shape,
                // which is what the noise-owner pattern needs.
                assert!(
                    *count_each == 1 && *count_total == owner_count,
                    "unkeyed HasSpec for '{owner_type}' requires count_each=1 and count_total=owner_count \
                     (got count_each={count_each}, count_total={count_total}, owner_count={owner_count})",
                );
                let mut q = String::from("insert\n");
                for e in 0..*count_total {
                    let value = attribute_generator(e);
                    q.push_str(&format!("  $o_{owner_type}_{e} isa {owner_type}, has {attr_type} {value};\n"));
                }
                queries.push(q);
            }
        }
    }

    commit_writes(context, &queries);
}

// --- Helpers for inspecting QueryProfile ----------------------------------------------------

fn worst_advances_per_row(profile: &QueryProfile) -> (f64, u64, u64, String) {
    let mut worst: (f64, u64, u64, String) = (0.0, 0, 0, String::new());
    for (_id, stage) in profile.stage_profiles().read().unwrap().iter() {
        if let Some(pattern) = stage.pattern_profile() {
            visit_steps_in_pattern(&pattern, &mut |step| update_worst(step, &mut worst));
        }
    }
    worst
}

fn visit_steps_in_pattern(pattern: &PatternProfile, visit: &mut impl FnMut(&StepProfile)) {
    for substep in pattern.substeps().read().unwrap().iter() {
        match substep {
            SubstepProfile::StepProfile(step) => visit(step),
            SubstepProfile::PatternProfile(nested) => visit_steps_in_pattern(nested, visit),
            SubstepProfile::QueryProfile { profile, .. } => {
                for (_id, stage) in profile.stage_profiles().read().unwrap().iter() {
                    if let Some(nested_pattern) = stage.pattern_profile() {
                        visit_steps_in_pattern(&nested_pattern, visit);
                    }
                }
            }
        }
    }
}

fn update_worst(step: &StepProfile, worst: &mut (f64, u64, u64, String)) {
    let Some(advances) = step.storage_counters().get_raw_advance() else { return };
    let Some(rows) = step.rows() else { return };
    let ratio = advances as f64 / rows.max(1) as f64;
    if ratio > worst.0 {
        *worst = (ratio, advances, rows, step.description().unwrap_or_default());
    }
}

/// Returns all `IntersectionStep`s across the plan that combine 2+ instructions
/// — i.e. real sort-merge intersections, not single-iterator wrappers. Structural
/// inspection of the compiled `ConjunctionExecutable`, so robust against changes
/// to step display formatting.
fn multi_iter_intersection_steps<'a>(
    pipeline: &'a Pipeline<ReadSnapshot<WALClient>, ReadPipelineStage<ReadSnapshot<WALClient>>>,
) -> Vec<&'a IntersectionStep> {
    pipeline
        .stages()
        .iter()
        .filter_map(|s| s.as_match())
        .flat_map(|m| m.executable().steps())
        .filter_map(|s| match s {
            ExecutionStep::Intersection(i) if i.instructions.len() >= 2 => Some(i),
            _ => None,
        })
        .collect()
}

// --- Tests ----------------------------------------------------------------------------------

const OWNER_1: &str = "owner_1";
const OWNER_2: &str = "owner_2";
const KEY_1: &str = "key_1";
const KEY_2: &str = "key_2";
const JOIN_ATTR: &str = "join_attr";

fn define_two_owner_schema(context: &mut Context) {
    let schema = format!(
        "define \
          entity {OWNER_1} owns {KEY_1} @key, owns {JOIN_ATTR}; \
          entity {OWNER_2} owns {KEY_2} @key, owns {JOIN_ATTR}; \
          attribute {KEY_1}, value integer; \
          attribute {KEY_2}, value integer; \
          attribute {JOIN_ATTR}, value integer;"
    );
    define_schema(context, &schema);
}

fn two_owner_join_query() -> String {
    format!(
        "match \
         $e1 isa {OWNER_1}, has {JOIN_ATTR} $join; \
         $e2 isa {OWNER_2}, has {JOIN_ATTR} $join;"
    )
}

// --- VARIANT 1: has_2_join_balanced -----------------------------------------------------------

/// Baseline: 100 entities per owner, each owning one join_attr with unique
/// values 0..99. Full overlap → 100 output rows. Storage range for either
/// `Reverse[has join_attr]` covers 200 entries (100 A + 100 B).
///
/// Best plan options:
///   1) Reverse Has iteration sequentially (indexed loop join). Cost ~= 101 seeks + 400 advances
///   2) Reverse Has intersection (merge join).                   Cost ~=   2 seeks + 500 advances
#[test]
fn has_2_join_balanced() {
    let mut context = setup();
    define_two_owner_schema(&mut context);

    const N: usize = 100;
    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N,
                attribute_generator: sequential(),
            },
            HasSpec {
                owner_type: OWNER_2,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N,
                attribute_generator: sequential(),
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());

    // Plan-shape note: both merge and sequential are reasonable for the balanced
    // case (cost is similar either way).
    let _merges = multi_iter_intersection_steps(&pipeline);

    let (rows, profile) = execute_read(pipeline);

    // Each $join value matches exactly 1 e1 and 1 e2 → N output rows.
    assert_eq!(rows, N, "balanced join should produce exactly N rows");
    let (ratio, advances, prof_rows, descr) = worst_advances_per_row(&profile);
    assert!(
        ratio < 10.0,
        "balanced join: worst step should be tight (< 10 advances/row), got {ratio:.2} ({advances}/{prof_rows}). step: {descr}",
    );
}

// --- VARIANT 2: has_2_join_subset_with_post_filter --------------------------------------------

/// The bug case — direct test of the `Cost::join` blended-out-cost fix.
/// owner_1: 100 entities with unique values 0..99; owner_2: 25 entities with
/// unique values 0..24. Subset coverage on owner_2 side. Storage range covers
/// 125 entries; owner_2's `Reverse[has]` post-filters to 25 (waste=100). Blend
/// fires on owner_2 (`p_unmatched = 0.75`) → merge intersection penalised →
/// planner should pick sequential. Output: 25 rows.
///
/// Best plan options:
///   1) Reverse Has iteration sequentially (indexed loop join). Cost ~=  26 seeks + 175 advances
///   2) Reverse Has intersection (merge join).                   Cost ~=   2 seeks + 275 advances
///   (Merge's worst-case includes a scan-past-the-cluster waste term that
///    blows total cost much higher — the regime this fix targets.)
#[test]
fn has_2_join_subset_with_post_filter() {
    let mut context = setup();
    define_two_owner_schema(&mut context);

    const N_LARGE: usize = 100;
    const N_SMALL: usize = 25;
    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N_LARGE, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_SMALL, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N_LARGE,
                attribute_generator: sequential(),
            },
            HasSpec {
                owner_type: OWNER_2,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N_SMALL,
                attribute_generator: sequential(),
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());

    // Plan-shape assertion: the cost-model fix should keep the planner from putting both
    // `Reverse[A has $join]` and `Reverse[B has $join]` in a single merge intersection
    // when one side has post-filter waste and the smaller side's domain overhangs the
    // bigger side's coverage. Sequential is the expected plan here.
    let merges = multi_iter_intersection_steps(&pipeline);
    assert!(
        merges.is_empty(),
        "subset-with-post-filter: planner should pick sequential, not a merge intersection. \
         Found {} multi-iter intersection step(s); first sort_var={:?}",
        merges.len(),
        merges.first().map(|m| m.sort_variable),
    );

    let (rows, profile) = execute_read(pipeline);

    // owner_2's 25 values are a subset of owner_1's 100 → 25 output rows.
    assert_eq!(rows, N_SMALL, "subset join should produce N_SMALL rows");

    let (ratio, advances, prof_rows, descr) = worst_advances_per_row(&profile);
    assert!(
        ratio < 10.0,
        "subset-with-post-filter: worst step expected to be bounded (< 10 advances/row); \
         got {ratio:.2} ({advances}/{prof_rows}). step: {descr}",
    );
}

// --- VARIANT 3: has_2_join_disjoint_domains ---------------------------------------------------

/// Disjoint value domains. owner_1: 100 entities with values 0..99; owner_2:
/// 100 entities with values 1000..1099. Storage covers 200 entries; each side
/// post-filters to 100 (waste=100 each). Join domain has 200 distinct values
/// → `p_unmatched = 0.5` on both sides → blend penalty applies on both.
/// Output: 0 rows (disjoint).
///
/// Best plan options:
///   1) Reverse Has iteration sequentially (indexed loop join). Cost ~= 101 seeks + 200 advances
///   2) Reverse Has intersection (merge join).                   Cost ~=   2 seeks + 400 advances
///   (Either plan correctly emits 0 rows. Test asserts correctness only.)
#[test]
fn has_2_join_disjoint_domains() {
    let mut context = setup();
    define_two_owner_schema(&mut context);

    const N: usize = 100;
    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N,
                attribute_generator: sequential(),
            },
            HasSpec {
                owner_type: OWNER_2,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N,
                attribute_generator: offset_unique(1000),
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());

    // Plan-shape note: both sides have waste + a coverage gap → blend penalty fires
    // on both. The planner is free to pick merge or sequential; correctness is the
    // primary concern here, not plan shape.
    let _merges = multi_iter_intersection_steps(&pipeline);

    let (rows, profile) = execute_read(pipeline);

    // Correctness: no overlap → 0 rows. The interesting question is whether the
    // planner picks merge or sequential; both are reasonable when the join is empty.
    assert_eq!(rows, 0, "disjoint-domain join should produce 0 rows");

    // We don't bound advances/row here because with rows=0 the per-step counters
    // divide by max(1) = 1, inflating ratios for any non-trivial scan. The point
    // of this test is correctness on the empty-join case, not plan shape.
}

// --- VARIANT 4: has_2_join_fk_fanout ----------------------------------------------------------

/// Classic FK-PK fan-out: 10 PK (unique values 0..9) × 1000 FK (cyclic over
/// 0..9, so 100 owners per value). Storage covers 1010 entries. Both sides
/// clamp `p_unmatched` to 0 (`bigger_io >> join_size`) → no blend penalty.
/// Output: 1000 rows.
///
/// Best plan options:
///   1) Reverse Has iteration sequentially (indexed loop join). Cost ~=  11 seeks + 2020 advances
///      (drive from 10 PK, each probe walks 101 entries for value's PK+FK cluster)
///   2) Reverse Has intersection (merge join).                   Cost ~=   2 seeks + 3020 advances
///      (both iters walk 1010 entries + cartesian emit 1000)
///   (Roughly tied; planner picks merge.)
#[test]
fn has_2_join_fk_fanout() {
    let mut context = setup();
    define_two_owner_schema(&mut context);

    const N_PK: usize = 10;
    const N_FK: usize = 1000;
    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N_PK, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_FK, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N_PK,
                attribute_generator: sequential(),
            },
            HasSpec {
                owner_type: OWNER_2,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N_FK,
                attribute_generator: cyclic(N_PK),
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());

    // Plan-shape assertion: this is the "classic" FK-PK case the merge intersection
    // exists for. Both sides clamp p_unmatched to 0 (io > join_size), so the blend
    // adds no penalty and the planner should still pick a 2-iter merge.
    let merges = multi_iter_intersection_steps(&pipeline);
    assert!(
        !merges.is_empty(),
        "fk_fanout: expected the planner to pick a merge intersection (classic FK-PK \
         shape with no blend penalty); none found",
    );

    let (rows, profile) = execute_read(pipeline);

    assert_eq!(rows, N_FK, "FK fanout: each FK joins to 1 PK → N_FK rows");
    let (ratio, advances, prof_rows, descr) = worst_advances_per_row(&profile);
    assert!(
        ratio < 30.0,
        "fk_fanout: worst step should be O(rows) (< 30 advances/row), got {ratio:.2} ({advances}/{prof_rows}). step: {descr}",
    );
}

// --- VARIANT 5: has_2_join_selective_against_full --------------------------------------------

/// Selective lookup vs. full scan. owner_1: 1 entity with value 0; owner_2:
/// 1000 entities with unique values 0..999. Storage covers 1001 entries.
/// A is heavily blend-penalised (`p_unmatched_A ≈ 0.999`, waste=1000); B has
/// near-zero penalty. Output: 1 row.
///
/// Best plan options:
///   1) Reverse Has iteration sequentially (indexed loop join). Cost ~=   2 seeks + 1003 advances
///      (drive from the 1 A entity, then tight-probe B once)
///   2) Reverse Has intersection (merge join).                   Cost ~=   2 seeks + 2002 advances
///      (both iters walk 1001 entries even though only 1 produces output)
///   (Sequential clearly wins; planner picks sequential.)
#[test]
fn has_2_join_selective_against_full() {
    let mut context = setup();
    define_two_owner_schema(&mut context);

    const N_BIG: usize = 1000;
    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: 1, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_BIG, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: 1,
                attribute_generator: sequential(),
            },
            HasSpec {
                owner_type: OWNER_2,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N_BIG,
                attribute_generator: sequential(),
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());

    // Plan-shape note: A has waste=1000 and p_unmatched≈1, so the blend penalises
    // A heavily — pushing the planner toward sequential (drive from selective A,
    // bound-from into B). The catastrophic plan to avoid is the merge intersection
    // putting both Reverse[has]s into a single step. Assert no such merge exists.
    let merges = multi_iter_intersection_steps(&pipeline);
    assert!(
        merges.is_empty(),
        "selective-vs-full: a 2-iter merge on $join would be O(N_BIG^2) per probe; \
         expected sequential drive-from-A. Found {} multi-iter merge step(s)",
        merges.len(),
    );

    let (rows, profile) = execute_read(pipeline);

    assert_eq!(rows, 1, "selective-vs-full: 1 A-value matches 1 B-row");

    // Justification: with 1 output row, even a healthy sequential plan that drives
    // from A and bound-from to B does ≈O(1) advances on the inner side and ≈O(N_BIG)
    // on the outer scan to locate A; worst-step ratio is whatever single step had the
    // biggest scan / 1 output row. We cap at N_BIG * 5 to give the planner room for
    // type checks, attribute walks, and per-step overhead while still catching a
    // catastrophic merge-intersection plan (which would do O(N_BIG^2) on one step).
    let (ratio, advances, prof_rows, descr) = worst_advances_per_row(&profile);
    assert!(
        ratio < (N_BIG as f64) * 5.0,
        "selective-vs-full: worst step ratio {ratio:.2} ({advances}/{prof_rows}) exceeds {} — \
         a catastrophically-bad plan is likely. step: {descr}",
        N_BIG * 5,
    );
}

// --- VARIANT 6: has_2_join_many_to_many ------------------------------------------------------

/// Many-to-many: 200 owners per side, each with 1 join_attr value cyclic over
/// 10 distinct values (so 20 owners per value per side). Storage covers 400
/// entries. `bigger_io >> join_size` so blend clamps to 0 on both sides.
/// Output: 10 values × 20 × 20 = 4000 rows (cartesian within each value).
///
/// (Implementation note: count_each=1 over 200 owners rather than count_each=2
/// over 100 owners because `has` deduplicates per-owner and the round-robin
/// distribution would otherwise give each owner two identical values.)
///
/// Best plan options:
///   1) Reverse Has iteration sequentially (indexed loop join). Cost ~= 201 seeks + 8600 advances
///      (200 outer + 200 probes × 41 advances/probe through tight per-value range)
///   2) Reverse Has intersection (merge join).                   Cost ~=   2 seeks + 4800 advances
///      (both iters walk 400 + cartesian sub-iterator amortises emit across 4000)
///   (Merge wins decisively; planner picks merge.)
#[test]
fn has_2_join_many_to_many() {
    let mut context = setup();
    define_two_owner_schema(&mut context);

    const N_OWNERS: usize = 200;
    const DISTINCT_VALUES: usize = 10;
    const OWNERS_PER_VALUE: usize = N_OWNERS / DISTINCT_VALUES; // 20
    const EXPECTED_ROWS: usize = DISTINCT_VALUES * OWNERS_PER_VALUE * OWNERS_PER_VALUE; // 4000

    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N_OWNERS, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_OWNERS, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N_OWNERS,
                attribute_generator: cyclic(DISTINCT_VALUES),
            },
            HasSpec {
                owner_type: OWNER_2,
                attr_type: JOIN_ATTR,
                count_each: 1,
                count_total: N_OWNERS,
                attribute_generator: cyclic(DISTINCT_VALUES),
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());

    // Plan-shape assertion: many-to-many with both sides clamped to p_unmatched=0
    // (io >> join_size) → blend adds nothing → planner should still pick a 2-iter
    // merge intersection, which is the natural shape for this query.
    let merges = multi_iter_intersection_steps(&pipeline);
    assert!(
        !merges.is_empty(),
        "many_to_many: expected the planner to pick a merge intersection (the canonical \
         many-to-many sort-merge); none found",
    );

    let (rows, profile) = execute_read(pipeline);

    assert_eq!(rows, EXPECTED_ROWS, "many-to-many: full cartesian within each value");
    let (ratio, advances, prof_rows, descr) = worst_advances_per_row(&profile);
    // Each output row costs ~1 storage advance on the inner side plus a small constant
    // for the outer/type-check steps; observed worst step is ~8/row, cap at 20 for headroom.
    assert!(
        ratio < 20.0,
        "many-to-many: worst step should be O(1) per row (got {ratio:.2}, {advances}/{prof_rows}). step: {descr}",
    );
}

// --- VARIANT 7: has_2_join_empty -------------------------------------------------------------

/// Degenerate empty-data case: schema defined but zero instances. Smoke test
/// for the boundary where every cardinality estimate clamps to `MIN_SCAN_SIZE`
/// and the blend math degenerates (`cost / io_ratio`, `io_ratio / join_size`).
/// Output: 0 rows.
///
/// Best plan options:
///   1) Reverse Has iteration sequentially (indexed loop join). Cost ~= 1 seek + 0 advances
///   2) Reverse Has intersection (merge join).                   Cost ~= 2 seeks + 0 advances
///   (Plans are trivial; test asserts no panic and 0 rows.)
#[test]
fn has_2_join_empty() {
    let mut context = setup();
    define_two_owner_schema(&mut context);
    // Deliberately no data: skip load_data entirely. The empty case is the test.

    let pipeline = compile_read(&context, &two_owner_join_query());

    // Smoke check: plan compiled without panicking on degenerate stats. Don't
    // assert plan shape — the choice is uninteresting when every cardinality
    // estimator floors to MIN_SCAN_SIZE.
    let _merges = multi_iter_intersection_steps(&pipeline);

    let (rows, _profile) = execute_read(pipeline);
    assert_eq!(rows, 0, "empty schema: 0 rows");
}

// --- Helpers for variants that introduce "noise" owner types --------------------------------

const NOISE_TYPES: &[&str] = &["noise_1", "noise_2", "noise_3", "noise_4", "noise_5"];

/// Schema shape for the "noise" variants: two query entity types plus N noise entity types,
/// all owning the same `join_attr`. The query types are keyed (so load_data can address
/// individual instances); the noise types are unkeyed (they only ever have the join_attr
/// and we don't reference them in queries — they exist solely to bloat the scan range
/// of `Reverse[X has $join]` and force post-filter waste on the query sides).
fn define_two_owner_with_noise_schema(context: &mut Context, n_noise_types: usize) {
    assert!(
        n_noise_types <= NOISE_TYPES.len(),
        "only {} noise types declared in NOISE_TYPES",
        NOISE_TYPES.len()
    );
    let mut schema = format!(
        "define \
          entity {OWNER_1} owns {KEY_1} @key, owns {JOIN_ATTR}; \
          entity {OWNER_2} owns {KEY_2} @key, owns {JOIN_ATTR}; \
          attribute {KEY_1}, value integer; \
          attribute {KEY_2}, value integer; \
          attribute {JOIN_ATTR}, value integer; "
    );
    for t in &NOISE_TYPES[..n_noise_types] {
        schema.push_str(&format!("entity {t} owns {JOIN_ATTR}; "));
    }
    define_schema(context, &schema);
}

/// Append InstanceSpec + HasSpec entries for noise owners to `spec`. Each noise type
/// gets `per_type` entities, each owning one `join_attr` value; values are unique
/// across all noise entities, starting at `value_start` and going up.
fn add_noise_owners(spec: &mut DataSpec, n_noise_types: usize, per_type: usize, value_start: i64) {
    let mut offset = value_start;
    for t in &NOISE_TYPES[..n_noise_types] {
        spec.instances.push(InstanceSpec { type_: t, count: per_type, key: None });
        spec.has.push(HasSpec {
            owner_type: t,
            attr_type: JOIN_ATTR,
            count_each: 1,
            count_total: per_type,
            attribute_generator: offset_unique(offset),
        });
        offset += per_type as i64;
    }
}

// --- VARIANT 8: has_2_join_waste_on_both_sides -------------------------------------------------

/// Both sides simultaneously have heavy post-filter waste AND a coverage gap.
/// 50 entities per query owner with disjoint values (owner_1: 0..49, owner_2:
/// 50..99), plus 5000 noise-owner entries with unique values 100..5099.
/// Storage covers 5100 entries; each query side post-filters to 50 (waste=5050).
/// Join domain = 5100, so `p_unmatched ≈ 0.99` on both sides → blend fires
/// heavily on both. Output: 0 rows.
///
/// Best plan options:
///   1) Reverse Has iteration sequentially (indexed loop join). Cost ~=  51 seeks +  5100 advances
///   2) Reverse Has intersection (merge join).                   Cost ~=   2 seeks + 10200 advances
///   (Sequential wins decisively even without blend; planner picks sequential.)
#[test]
fn has_2_join_waste_on_both_sides() {
    // Tuned to extreme: 10 noise types × 500 each = 5000 noise entries swamp
    // the 100 query entries → scan = 5100, waste/io = 50× on each query side.
    // Both new and old models should overwhelmingly reject merge here.
    const N_QUERY: usize = 50;
    const N_NOISE_TYPES: usize = 5;  // bounded by NOISE_TYPES.len() = 5
    const N_NOISE_PER_TYPE: usize = 1000;

    let mut context = setup();
    define_two_owner_with_noise_schema(&mut context, N_NOISE_TYPES);

    let mut spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N_QUERY, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_QUERY, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_QUERY,
                attribute_generator: sequential(),  // values 0..49
            },
            HasSpec {
                owner_type: OWNER_2, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_QUERY,
                attribute_generator: offset_unique(N_QUERY as i64),  // values 50..99 (disjoint)
            },
        ],
    };
    add_noise_owners(&mut spec, N_NOISE_TYPES, N_NOISE_PER_TYPE, 100);
    load_data(&mut context, spec);

    let pipeline = compile_read(&context, &two_owner_join_query());

    // Plan-shape assertion: blend fires heavily on both sides → merge cost is
    // ballistic → planner should pick sequential (or any non-2-iter-merge plan).
    let merges = multi_iter_intersection_steps(&pipeline);
    assert!(
        merges.is_empty(),
        "waste-on-both: expected planner to avoid a 2-iter merge; found {} merge step(s)",
        merges.len(),
    );

    let (rows, _profile) = execute_read(pipeline);
    assert_eq!(rows, 0, "waste-on-both: query values are disjoint → 0 rows");
}

// --- VARIANT 9: has_2_join_inverted_asymmetry --------------------------------------------------

/// Small side has the coverage gap; large side covers its own domain. owner_1:
/// 5 entities with values 0..4; owner_2: 500 entities with unique values 0..499.
/// 200 noise entries push the domain to ~705 distinct. Storage = 705 entries;
/// owner_1.waste = 700, owner_2.waste = 205. `p_unmatched_1 ≈ 0.99` (extreme),
/// `p_unmatched_2 ≈ 0.29` (moderate). owner_1's blend dominates. Output: 5 rows.
///
/// Best plan options:
///   1) Reverse Has iteration sequentially (indexed loop join). Cost ~=   6 seeks +  715 advances
///      (drive from 5-entity owner_1, each probe tight on its value into owner_2)
///   2) Reverse Has intersection (merge join).                   Cost ~=   2 seeks + 1410 advances
///   (Sequential wins by ~2×; planner picks sequential.)
#[test]
fn has_2_join_inverted_asymmetry() {
    // Tuned to extreme: very small side (5 entities) with values in a tiny
    // sub-range of the very large side (500 entities, values 0..499) plus 200
    // noise entries extending the domain. Small side's p_unmatched approaches
    // 1.0 and has the only meaningful waste; large side is essentially full
    // coverage. Both new and old models should drive from the small side.
    const N_SMALL: usize = 5;
    const N_LARGE: usize = 500;
    const N_NOISE_TYPES: usize = 2;
    const N_NOISE_PER_TYPE: usize = 100;

    let mut context = setup();
    define_two_owner_with_noise_schema(&mut context, N_NOISE_TYPES);

    let mut spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N_SMALL, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_LARGE, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_SMALL,
                attribute_generator: sequential(),  // 0..9
            },
            HasSpec {
                owner_type: OWNER_2, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_LARGE,
                attribute_generator: sequential(),  // 0..99 (10 of these overlap with owner_1)
            },
        ],
    };
    add_noise_owners(&mut spec, N_NOISE_TYPES, N_NOISE_PER_TYPE, 200);  // values 200..209
    load_data(&mut context, spec);

    let pipeline = compile_read(&context, &two_owner_join_query());

    // Plan-shape assertion: owner_1's blend dominates; planner should avoid merge.
    let merges = multi_iter_intersection_steps(&pipeline);
    assert!(
        merges.is_empty(),
        "inverted-asymmetry: expected planner to avoid merge; found {} merge step(s)",
        merges.len(),
    );

    let (rows, _profile) = execute_read(pipeline);
    // owner_1's 10 values ∩ owner_2's 100 values = 10 matches (values 0..9 each
    // appear once in each side).
    assert_eq!(rows, N_SMALL, "inverted-asymmetry: small side ⊂ large side → N_SMALL rows");
}

// --- VARIANT 10: has_2_join_asymmetric_clamp ---------------------------------------------------

/// Asymmetric clamp: dense side's `io > join_size` clamps `p_unmatched` to 0;
/// sparse side has a real gap. owner_1: 1000 entities cyclic over 5 values
/// (200 owners per value); owner_2: 50 unique values 0..49; 100 noise entries.
/// Storage covers 1150 entries. `p_unmatched_1 = 0` (dense covers any subset);
/// `p_unmatched_2 ≈ 0.68`. Output: 200 × 5 × 1 = 1000 rows.
///
/// Best plan options:
///   1) Reverse Has iteration sequentially (indexed loop join). Cost ~=  51 seeks +  2200 advances
///      (drive from 50 sparse, each matched probe walks 201 entries; 45 empty probes)
///   2) Reverse Has intersection (merge join).                   Cost ~=   2 seeks +  3300 advances
///      (both iters walk 1150 + cartesian 1000)
///   (Sequential cheaper; clamp must hold to avoid mis-pricing dense side.)
#[test]
fn has_2_join_asymmetric_clamp() {
    // Tuned to extreme: extreme io_dense / join_size ratio (1000 cyclic over 5
    // values = 200× io vs distinct). Sparse side has a real gap (50 unique
    // out of join_size ~155 once noise is included). Clamp should fire hard
    // on dense, gap blend should fire on sparse. Both models should agree on
    // rejecting any plan that scans dense's full range per probe.
    const N_DENSE: usize = 1000;
    const N_SPARSE: usize = 50;
    const DENSE_DISTINCT: usize = 5;
    const N_NOISE_TYPES: usize = 1;
    const N_NOISE_PER_TYPE: usize = 100;

    let mut context = setup();
    define_two_owner_with_noise_schema(&mut context, N_NOISE_TYPES);

    let mut spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N_DENSE, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_SPARSE, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_DENSE,
                attribute_generator: cyclic(DENSE_DISTINCT),  // 200 owners per value 0..4
            },
            HasSpec {
                owner_type: OWNER_2, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_SPARSE,
                attribute_generator: sequential(),  // 0..49
            },
        ],
    };
    add_noise_owners(&mut spec, N_NOISE_TYPES, N_NOISE_PER_TYPE, 50);  // values 50..149
    load_data(&mut context, spec);

    let pipeline = compile_read(&context, &two_owner_join_query());

    // Plan-shape is not asserted: both merge and sequential are reasonable here.
    // What we care about is correctness and bounded per-step work.
    let _merges = multi_iter_intersection_steps(&pipeline);

    let (rows, profile) = execute_read(pipeline);
    // owner_1 has 200 entries per value (cyclic 0..4). owner_2 has 1 entry per
    // value 0..49. Intersection at values 0..4: 200 * 1 * 5 = 1000 rows.
    let expected_rows = (N_DENSE / DENSE_DISTINCT) * DENSE_DISTINCT;
    assert_eq!(rows, expected_rows, "asymmetric-clamp: cyclic owner_1 × unique owner_2 = N_DENSE rows");
    let (ratio, advances, prof_rows, descr) = worst_advances_per_row(&profile);
    // Bound: with clamp working, owner_1 is treated as dense (no extra penalty)
    // and a healthy plan does ~O(1) advances per output row. Allow 30/row to
    // give headroom for type-check/wrapper-step overhead. A regression that
    // removes the clamp would force the planner into a very different shape
    // and likely blow past this.
    assert!(
        ratio < 30.0,
        "asymmetric-clamp: worst step should remain bounded (< 30 advances/row); got {ratio:.2} ({advances}/{prof_rows}). step: {descr}",
    );
}

// --- VARIANT 11: has_2_join_fk_pushed_to_inl ---------------------------------------------------

/// Scale-up of `fk_fanout` pushed into INL territory: 5 PK × 500 FK (each PK
/// matches 100 FK rows). Storage covers 505 entries. Output: 500 rows.
///
/// Best plan options:
///   1) Reverse Has iteration sequentially (indexed loop join). Cost ~=   6 seeks + 1010 advances
///      (drive from 5 PK, each probe walks 101-entry tight range)
///   2) Reverse Has intersection (merge join).                   Cost ~=   2 seeks + 1510 advances
///   (Sequential ~2× cheaper in principle; planner picks merge —
///    documents the under-weighting of INL for small-PK × large-FK.)
#[test]
fn has_2_join_fk_pushed_to_inl() {
    const N_PK: usize = 5;
    const N_FK: usize = 500;

    let mut context = setup();
    define_two_owner_schema(&mut context);

    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N_PK, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_FK, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_PK,
                attribute_generator: sequential(),  // PK side: values 0..4
            },
            HasSpec {
                owner_type: OWNER_2, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_FK,
                attribute_generator: cyclic(N_PK),  // FK side: cyclic over 0..4, ~1000 per value
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());
    let merges = multi_iter_intersection_steps(&pipeline);
    let merge_count = merges.len();
    let (rows, profile) = execute_read(pipeline);
    let (ratio, advances, prof_rows, descr) = worst_advances_per_row(&profile);

    // Diagnostic — surface plan + key numbers regardless of pass/fail so the
    // user can see whether the planner picked the literature-optimal INL or
    // stayed on merge.
    eprintln!(
        "fk_pushed_to_inl: rows={rows} merges={merge_count} worst={ratio:.2} adv/row \
         ({advances}/{prof_rows}). step: {descr}"
    );

    assert_eq!(rows, N_FK, "fk_pushed_to_inl: each FK matches one PK → N_FK rows");
    // Bound at 5 advances/row — leaves headroom for either plan choice.
    assert!(
        ratio < 5.0,
        "fk_pushed_to_inl: worst step should remain bounded (< 5 advances/row); got {ratio:.2}. step: {descr}",
    );
}

// --- VARIANT 12: has_2_join_fk_pushed_to_merge -------------------------------------------------

/// Scale-up of `fk_fanout` pushed into merge-clearly-wins territory: 200 × 200
/// with cyclic over 20 distinct values (10 owners per value per side). Storage
/// covers 400 entries. Output: 10 × 10 × 20 = 2000 rows (cartesian within each
/// value).
///
/// Best plan options:
///   1) Reverse Has iteration sequentially (indexed loop join). Cost ~= 201 seeks + 4600 advances
///      (200 outer + 200 probes × 21 entries/probe)
///   2) Reverse Has intersection (merge join).                   Cost ~=   2 seeks + 2800 advances
///      (both iters walk 400 + cartesian emits 2000)
///   (Merge cheaper in principle; planner picks sequential at this scale —
///    the merge breakpoint is around per-value cartesian ≥ ~400 outputs.)
#[test]
fn has_2_join_fk_pushed_to_merge() {
    const N_EACH: usize = 200;
    const N_DISTINCT: usize = 20;
    const PER_VALUE: usize = N_EACH / N_DISTINCT;  // 10

    let mut context = setup();
    define_two_owner_schema(&mut context);

    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N_EACH, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_EACH, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_EACH,
                attribute_generator: cyclic(N_DISTINCT),  // 10 per value
            },
            HasSpec {
                owner_type: OWNER_2, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_EACH,
                attribute_generator: cyclic(N_DISTINCT),  // 10 per value
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());
    let merges = multi_iter_intersection_steps(&pipeline);
    let merge_count = merges.len();
    let (rows, profile) = execute_read(pipeline);
    let (ratio, advances, prof_rows, descr) = worst_advances_per_row(&profile);

    eprintln!(
        "fk_pushed_to_merge: rows={rows} merges={merge_count} worst={ratio:.2} adv/row \
         ({advances}/{prof_rows}). step: {descr}"
    );

    // Output: 10 × 10 cartesian × 20 values = 2000.
    let expected_rows = PER_VALUE * PER_VALUE * N_DISTINCT;
    assert_eq!(rows, expected_rows, "fk_pushed_to_merge: cartesian-within-value → 2000 rows");
    // Bound at 30 advances/row (matches many_to_many) — merge with cartesian
    // sub-iterator should easily stay under this.
    assert!(
        ratio < 30.0,
        "fk_pushed_to_merge: worst step should remain bounded (< 30 advances/row); got {ratio:.2}. step: {descr}",
    );
}

// --- Sweep helpers / probes -------------------------------------------------------------------

/// Build, populate, plan, execute a single fk_fanout scenario; print one line of summary.
/// Used by the breakpoint-sweep tests to record what the planner picks at each scale.
fn probe_fk_at_scale(label: &str, n_pk: usize, n_fk: usize) -> (usize, usize, f64) {
    let mut context = setup();
    define_two_owner_schema(&mut context);
    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: n_pk, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: n_fk, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1, attr_type: JOIN_ATTR,
                count_each: 1, count_total: n_pk,
                attribute_generator: sequential(),
            },
            HasSpec {
                owner_type: OWNER_2, attr_type: JOIN_ATTR,
                count_each: 1, count_total: n_fk,
                attribute_generator: cyclic(n_pk),  // fan FK over PK values
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());
    let merge_count = multi_iter_intersection_steps(&pipeline).len();
    let (rows, profile) = execute_read(pipeline);
    let (ratio, _, _, _) = worst_advances_per_row(&profile);
    eprintln!(
        "  [{label}] pk={n_pk:>4} fk={n_fk:>5} -> merges={merge_count} rows={rows:>5} worst={ratio:.2}"
    );
    (merge_count, rows, ratio)
}

/// Same shape as `probe_fk_at_scale` but for the balanced N:M cartesian shape.
fn probe_nm_at_scale(label: &str, n_each: usize, n_distinct: usize) -> (usize, usize, f64) {
    let mut context = setup();
    define_two_owner_schema(&mut context);
    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: n_each, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: n_each, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1, attr_type: JOIN_ATTR,
                count_each: 1, count_total: n_each,
                attribute_generator: cyclic(n_distinct),
            },
            HasSpec {
                owner_type: OWNER_2, attr_type: JOIN_ATTR,
                count_each: 1, count_total: n_each,
                attribute_generator: cyclic(n_distinct),
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());
    let merge_count = multi_iter_intersection_steps(&pipeline).len();
    let (rows, profile) = execute_read(pipeline);
    let (ratio, _, _, _) = worst_advances_per_row(&profile);
    eprintln!(
        "  [{label}] n={n_each:>3} distinct={n_distinct:>2} -> merges={merge_count} rows={rows:>5} worst={ratio:.2}"
    );
    (merge_count, rows, ratio)
}

// --- Sweep tests -----------------------------------------------------------------------------

/// Sweep PK/FK scales in the asymmetric direction. As n_pk shrinks and the
/// asymmetry grows, classical literature increasingly favours INL drive-from-
/// the-small-PK over merge. Record at what scale (if any) the planner
/// actually switches from picking merge to picking sequential/INL.
///
/// This is an informational test — it asserts only that every scale runs to
/// completion with sensible output. The breakpoint (if found) is reported
/// in the test log for review.
#[test]
fn fk_breakpoint_sweep_toward_inl() {
    eprintln!("fk_breakpoint_sweep_toward_inl: merges>=1 means the planner picked a 2-iter merge");
    let scales = [(10, 1000), (5, 500), (3, 1000), (2, 1000), (1, 1000), (1, 2000)];
    let mut results = Vec::new();
    for (n_pk, n_fk) in scales {
        let (merges, rows, ratio) = probe_fk_at_scale("sweep_inl", n_pk, n_fk);
        results.push((n_pk, n_fk, merges, rows, ratio));
    }
    // Sanity: every scale produces correct row count and bounded per-step work.
    for (n_pk, n_fk, _merges, rows, ratio) in &results {
        assert_eq!(*rows, *n_fk, "fk_sweep pk={n_pk} fk={n_fk}: each FK matches → n_fk rows");
        assert!(*ratio < 10.0, "fk_sweep pk={n_pk} fk={n_fk}: ratio {ratio:.2} > 10 — runaway plan?");
    }
}

/// Sweep N:M scales in the cartesian-rich direction. As n_distinct shrinks
/// (more owners per value, larger cartesian per match), merge should become
/// the cheaper plan (cartesian sub-iterator amortizes one inner-side iter
/// open per matched value, vs INL re-opening per outer row). Record at what
/// scale the planner switches to merge.
#[test]
fn fk_breakpoint_sweep_toward_merge() {
    eprintln!("fk_breakpoint_sweep_toward_merge: merges>=1 means the planner picked a 2-iter merge");
    let scales = [
        (200, 50),  // 4 per value, 800 output
        (200, 20),  // 10 per value, 2000 output (already in fk_pushed_to_merge — should pick seq)
        (200, 10),  // 20 per value, 4000 output (already in many_to_many — picks merge)
        (200, 5),   // 40 per value, 8000 output
        (200, 2),   // 100 per value, 20000 output
    ];
    let mut results = Vec::new();
    for (n_each, n_distinct) in scales {
        let (merges, rows, ratio) = probe_nm_at_scale("sweep_merge", n_each, n_distinct);
        results.push((n_each, n_distinct, merges, rows, ratio));
    }
    for (n_each, n_distinct, _, rows, ratio) in &results {
        let per_value = n_each / n_distinct;
        let expected = per_value * per_value * n_distinct;
        assert_eq!(*rows, expected, "nm_sweep n={n_each} distinct={n_distinct}: cartesian → expected rows");
        assert!(*ratio < 30.0, "nm_sweep n={n_each} distinct={n_distinct}: ratio {ratio:.2} > 30");
    }
}

// --- VARIANT 13: has_2_join_selective_outer_filter ---------------------------------------------

/// Selective outer filter — non-default query shape. owner_1: 10 entities with
/// unique key_1 values 0..9 and unique join_attr values 0..9; owner_2: 1000
/// entities, each owning one join_attr value cyclic 0..9 (100 per value).
/// Query: `$pk isa owner_1, has key_1 5, has join_attr $j; $fk isa owner_2,
/// has join_attr $j;` — the `has key_1 5` pins `$pk` to one entity. Output:
/// 100 rows.
///
/// Best plan options:
///   1) Indexed loop via key lookup + bound-from chain. Cost ~= 3 seeks + ~113 advances
///      (key scan + `$pk has $j` with bound pk + `$fk has $j` with bound j)
///   2) Reverse Has intersection (merge join) on $j.    Cost ~= 2 seeks + ~2002 advances
///      (full reverse-has on both sides + 100 emits)
///
/// Observed: planner picks a 2-iter Sorted Iterator Intersection on $j with
/// `bound_vars=[$pk]` — one of the merged iterators is bound by the pinned
/// PK, effectively turning the "merge" into an INL probe internally. Observed
/// cost ~1.1 advances/row. The merge-vs-INL distinction blurs when iterators
/// have bound inputs: same operator, different data flow.
#[test]
fn has_2_join_selective_outer_filter() {
    const N_PK: usize = 10;
    const N_FK: usize = 1000;
    const SELECTED_KEY: i64 = 5;

    let mut context = setup();
    define_two_owner_schema(&mut context);

    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N_PK, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_FK, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_PK,
                attribute_generator: sequential(),  // values 0..9, paired with key_1 0..9
            },
            HasSpec {
                owner_type: OWNER_2, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_FK,
                attribute_generator: cyclic(N_PK),  // values cyclic 0..9, 100 per value
            },
        ],
    };
    load_data(&mut context, data_spec);

    let query = format!(
        "match \
         $pk isa {OWNER_1}, has {KEY_1} {SELECTED_KEY}, has {JOIN_ATTR} $j; \
         $fk isa {OWNER_2}, has {JOIN_ATTR} $j;"
    );

    let pipeline = compile_read(&context, &query);

    // Plan-shape assertion: the optimal plan never puts `Reverse[PK has $j]` and
    // `Reverse[FK has $j]` in a single merge intersection on $j. With $pk pinned
    // by the key lookup, $j becomes a single value early, and the join collapses
    // to a tight bound-from probe on FK.
    let merges = multi_iter_intersection_steps(&pipeline);
    let merge_count = merges.len();

    let (rows, profile) = execute_read(pipeline);
    let (ratio, advances, prof_rows, descr) = worst_advances_per_row(&profile);
    eprintln!(
        "selective_outer_filter: rows={rows} merges={merge_count} worst={ratio:.2} adv/row \
         ({advances}/{prof_rows}). step: {descr}"
    );

    // Expected: PK with key=5 has join_attr=5; FK with value=5 has 100 owners; output = 100 rows.
    assert_eq!(rows, N_FK / N_PK, "selective_outer_filter: 1 pk × 100 fk at value 5 = 100 rows");

    // Cost assertion: the planner must route the join through the selective key
    // lookup, regardless of whether it expresses the result as INL or as a
    // bound-input merge intersection. A "bad" plan (e.g. merging the two
    // reverse-has iters with no bound input) would be ~20× more expensive and
    // would blow past this bound.
    assert!(
        ratio < 5.0,
        "selective_outer_filter: worst step expected to be tight (< 5 advances/row); \
         got {ratio:.2} ({advances}/{prof_rows}). step: {descr}"
    );
}
