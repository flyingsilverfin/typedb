/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Planner-focused integration tests. These build a small schema, populate
//! data via a `DataSpec`-driven loader so cardinalities are easy to control,
//! run a read query, and inspect the resulting `QueryProfile` to assert that
//! the planner picked the plan shape (merge vs sequential) that we *expect*
//! it should pick from runtime cost.
//!
//! Each test is a "must-pick-X" scenario at production-ish scale: data is
//! shaped so one plan should clearly be cheaper, the planner is expected to
//! pick that shape, and the assertion fires if it doesn't. Correctness (row
//! count) is also checked so a wrong plan with the right row count still
//! gets flagged on shape, and a wrong row count gets flagged either way.
//!
//! Each test's docstring lists two things:
//! - **Expected** (theoretical/cost-model): the plan we'd expect to win from
//!   the literature / from the planner's `Cost::join` formula
//! - **Actually observed** (bench, opt mode, FORCE_*_INTERSECTION env-var
//!   forcing on each plan to measure both on the same data): the runtime
//!   wall-clock for each plan, and what the planner currently picks
//!
//! Run the bench yourself with:
//!   bazel run --compilation_mode=opt //query/benches:bench_planner_join_compare
//!
//! Empirical summary at time of writing: **sequential beats merge by ~2-3.5×
//! in every shape we've tested** (2-side has-joins from 1×5000 up to 5K×5K,
//! plus N-way same-variable joins from 2 to 12 sides). The IntersectionStep
//! does ~3-6× more storage seeks+advances per output row than the equivalent
//! bound-from probe, plus ~8.5µs/row of per-emit overhead. So:
//!
//! Merge-must-win scenarios (cost model thinks merge wins; **runtime says no**):
//! - `merge_wins_symmetric_balanced`         — 1:1 baseline, full coverage    [FAILS: planner picks seq]
//! - `merge_wins_moderate_cartesian`         — 10:1 per-value fan-out         [FAILS: planner picks seq]
//! - `merge_wins_heavy_cartesian`            — 50:1 per-value fan-out         [PASSES: planner picks merge, but merge is 3× SLOWER]
//! - `merge_wins_at_scale_with_fanout`       — moderate fan-out, higher card  [FAILS: planner picks seq]
//!
//! Sequential-must-win scenarios (planner correctly picks sequential):
//! - `sequential_wins_tiny_outer_huge_inner` — 1 outer × N inner              [PASSES]
//! - `sequential_wins_subset_coverage`       — small outer ⊂ huge inner       [PASSES]
//! - `sequential_wins_noisy_inner`           — noise inflates scan range      [PASSES]
//! - `sequential_wins_asymmetric_coverage`   — small outer × huge inner       [PASSES]
//!
//! The 3 failing merge_wins_* tests are intentional: they encode the
//! pre-bench theoretical expectation so a future runtime improvement to the
//! IntersectionStep (or a cost-model recalibration that gives up on merge in
//! these shapes) makes the discrepancy visible rather than silent.

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
use executor::pipeline::stage::ReadPipelineStage;
use function::function_manager::FunctionManager;
use query::options::QueryOptions;
use query::{query_cache::QueryCache, query_manager::QueryManager};
use resource::profile::{CommitProfile, PatternProfile, QueryProfile, StepProfile, SubstepProfile};
use storage::{MVCCStorage, durability_client::WALClient, snapshot::CommittableSnapshot};
use storage::snapshot::ReadSnapshot;
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

/// Sparse-sequential: yields 0, stride, 2*stride, ..., (N-1)*stride. Use on
/// one side of a two-side join to create asymmetric value distributions so the
/// merge intersection has to issue catch-up seeks (rather than walking both
/// iterators in lockstep). Stride > 1 means the OTHER side's iterator has to
/// advance past `stride-1` non-matching entries between each match.
fn sparse(stride: usize) -> AttributeGenerator {
    assert!(stride >= 1, "sparse stride must be >= 1");
    Box::new(move |i| (i * stride) as i64)
}

/// Same shape as `cyclic` but with stride between distinct values. Used in
/// cyclic-paired tests where one side has sparse-sequential values; cycling
/// the other side over the matching value range preserves output cardinality
/// (symmetric stride — no catch-up seeks fire, but the merge regime stays
/// the same as the dense case).
fn cyclic_sparse(modulus: usize, stride: usize) -> AttributeGenerator {
    Box::new(move |i| ((i % modulus) * stride) as i64)
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
    let mut instance_counts: HashMap<&'static str, usize> = HashMap::new();
    let mut queries: Vec<String> = Vec::new();

    for InstanceSpec { type_, count, key } in &spec.instances {
        if *count == 0 {
            continue;
        }
        let mut q = String::from("insert\n");
        for i in 0..*count {
            // typeql disallows underscore-prefixed variables, so name with a letter prefix.
            match key {
                Some(key_label) => {
                    q.push_str(&format!("  $x_{type_}_{i} isa {type_}, has {key_label} {i};\n"));
                }
                None => {
                    q.push_str(&format!("  $x_{type_}_{i} isa {type_};\n"));
                }
            }
        }
        queries.push(q);
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
        // HasSpec needs to address individual owners by key value, so the owner type
        // must have had a `key` set on its InstanceSpec.
        let key_label = spec
            .instances
            .iter()
            .find(|i| i.type_ == *owner_type)
            .and_then(|i| i.key)
            .unwrap_or_else(|| {
                panic!(
                    "HasSpec owner_type '{owner_type}' has no InstanceSpec with a key; \
                     can't address individual owners without one"
                )
            });
        // Assign edges to owners round-robin: edge `e` goes to owner `e % owner_count`.
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

// --- Two-owner schema & noise helpers -------------------------------------------------------

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

const NOISE_TYPES: &[&str] = &["noise_1", "noise_2", "noise_3", "noise_4", "noise_5"];
const NOISE_KEY: &str = "noise_id";

/// Schema shape for "noise" variants: two query entity types plus N noise entity
/// types, all owning the same `join_attr`. Noise types share a `noise_id` key so
/// load_data can address them via match-insert. We never query the noise types
/// directly — they exist to bloat the storage scan range of `Reverse[X has $join]`
/// for any owner type X, inflating merge's double-scan cost relative to sequential's
/// outer-then-bind-from cost.
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
          attribute {JOIN_ATTR}, value integer; \
          attribute {NOISE_KEY}, value integer; "
    );
    for t in &NOISE_TYPES[..n_noise_types] {
        schema.push_str(&format!("entity {t} owns {NOISE_KEY} @key, owns {JOIN_ATTR}; "));
    }
    define_schema(context, &schema);
}

/// Append InstanceSpec + HasSpec entries for noise owners to `spec`. Each noise
/// type gets `per_type` entities, each owning one `join_attr` value; values are
/// unique across all noise entities, starting at `value_start` and going up.
fn add_noise_owners(spec: &mut DataSpec, n_noise_types: usize, per_type: usize, value_start: i64) {
    let mut offset = value_start;
    for t in &NOISE_TYPES[..n_noise_types] {
        spec.instances.push(InstanceSpec { type_: t, count: per_type, key: Some(NOISE_KEY) });
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

// === Merge-must-win tests ====================================================================

/// Symmetric balanced full-coverage baseline. Both sides 500 owners, 1:1 with
/// values 0..499 → 500 output rows. No waste, no coverage gap, no noise.
///
/// Expected (cost model + classical literature): textbook merge-win shape —
/// both sides pre-sorted on join key, dense overlap, no waste. Sequential
/// pays ~500×(5+1)=3000 cost units; merge pays ~1000 (one co-walk over the
/// 500-entry storage range). Merge expected to win by ~3×.
///
/// Actually observed (profile dump at test scale, opt mode):
///   forced sequential:  4.49 ms total — join step: 1.0 seek + 1.0 adv per row
///   forced merge:      15.30 ms total — join step: 3.0 seeks + 6.0 adv per row
///                                       →  sequential wins by 3.4×
/// Planner picks sequential. **Test currently FAILS its plan-shape assertion**
/// (the planner's pick is empirically correct; the assertion encodes the
/// theoretical expectation we want to revisit once IntersectionStep is faster).
/// The 3-seek and 6-advance overhead per row in the merge step is the
/// structural slowdown — bound-from probe does the same join in 1+1.
#[test]
fn merge_wins_symmetric_balanced() {
    const N: usize = 500;

    let mut context = setup();
    define_two_owner_schema(&mut context);

    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N,
                attribute_generator: sequential(),
            },
            HasSpec {
                owner_type: OWNER_2, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N,
                attribute_generator: sequential(),
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());
    let merges = multi_iter_intersection_steps(&pipeline);
    assert!(
        !merges.is_empty(),
        "symmetric_balanced: planner should pick a merge intersection \
         (full coverage on both sides, no waste, no noise — merge clearly wins); \
         found none",
    );

    let (rows, profile) = execute_read(pipeline);
    assert_eq!(rows, N, "symmetric_balanced: 1:1 full coverage → N rows");
    let (ratio, advances, prof_rows, descr) = worst_advances_per_row(&profile);
    assert!(
        ratio < 10.0,
        "symmetric_balanced: worst step should be tight (< 10 advances/row); \
         got {ratio:.2} ({advances}/{prof_rows}). step: {descr}",
    );
}

/// Moderate per-value fan-out. Both sides 500 owners cyclic over 50 distinct
/// values (10 owners per value per side). Output = 50 × 10 × 10 = 5000 rows
/// (cartesian within each value).
///
/// Expected (cost model): sequential pays ~500×(5+10)=7500 (each probe walks
/// the 10-entry per-value cluster); merge pays ~1000 + per-value cartesian
/// sub-iter. Merge expected to win by ~5×.
///
/// Actually observed (profile dump at test scale, opt mode):
///   forced sequential: 16.83 ms total — join step: 0.1 seek + 1.0 adv per row (5000 rows)
///   forced merge:      55.06 ms total — join step: 0.5 seek + 6.5 adv per row
///                                       →  sequential wins by 3.3×
/// Planner picks sequential. **Test currently FAILS its plan-shape assertion**
/// (the per-value cartesian sub-iter overhead in the executor exceeds what
/// the cost model predicts; sequential's bind-from probe does ~1 advance/row
/// vs merge's ~6.5 advances/row).
#[test]
fn merge_wins_moderate_cartesian() {
    const N_OWNERS: usize = 500;
    const DISTINCT_VALUES: usize = 50;
    const OWNERS_PER_VALUE: usize = N_OWNERS / DISTINCT_VALUES; // 10
    const EXPECTED_ROWS: usize = DISTINCT_VALUES * OWNERS_PER_VALUE * OWNERS_PER_VALUE; // 5000

    let mut context = setup();
    define_two_owner_schema(&mut context);

    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N_OWNERS, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_OWNERS, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_OWNERS,
                attribute_generator: cyclic(DISTINCT_VALUES),
            },
            HasSpec {
                owner_type: OWNER_2, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_OWNERS,
                attribute_generator: cyclic(DISTINCT_VALUES),
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());
    let merges = multi_iter_intersection_steps(&pipeline);
    assert!(
        !merges.is_empty(),
        "moderate_cartesian: planner should pick a merge intersection \
         (10:1 per-value fan-out symmetric — cartesian sub-iter amortizes); \
         found none",
    );

    let (rows, profile) = execute_read(pipeline);
    assert_eq!(rows, EXPECTED_ROWS, "moderate_cartesian: 50 values × 10 × 10 = 5000 rows");
    let (ratio, advances, prof_rows, descr) = worst_advances_per_row(&profile);
    assert!(
        ratio < 20.0,
        "moderate_cartesian: worst step should be O(1) per row (< 20 advances/row); \
         got {ratio:.2} ({advances}/{prof_rows}). step: {descr}",
    );
}

/// Heavy per-value fan-out. Both sides 500 owners cyclic over 10 distinct
/// values (50 owners per value per side). Output = 10 × 50 × 50 = 25000 rows.
///
/// Expected (cost model): sequential pays ~500×(5+50)=27500 (each probe walks
/// the 50-entry per-value cluster); merge pays ~1000 + per-value cartesian.
/// Merge expected to win by ~25× — this is the regime where the cartesian
/// sub-iter most clearly should pay off.
///
/// Actually observed (profile dump at test scale, opt mode):
///   forced sequential:  72.05 ms total — join step: 0.02 seek + 1.0 adv per row (25000 rows)
///   forced merge:      272.88 ms total — join step: 0.1 seek + 7.7 adv per row
///                                       →  sequential beats merge by 3.8×
/// Planner mis-picks merge: this is the one shape where the cost model picks
/// merge but the runtime would have preferred sequential. **Test currently
/// PASSES its plan-shape assertion** (planner does pick merge) but the choice
/// is empirically wrong — the dump shows sequential would have been ~3.8×
/// faster. The cartesian sub-iter's per-emit work (~7.7 advances/row vs
/// sequential's 1.0) is much higher than the cost model accounts for.
#[test]
fn merge_wins_heavy_cartesian() {
    const N_OWNERS: usize = 500;
    const DISTINCT_VALUES: usize = 10;
    const OWNERS_PER_VALUE: usize = N_OWNERS / DISTINCT_VALUES; // 50
    const EXPECTED_ROWS: usize = DISTINCT_VALUES * OWNERS_PER_VALUE * OWNERS_PER_VALUE; // 25000

    let mut context = setup();
    define_two_owner_schema(&mut context);

    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N_OWNERS, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_OWNERS, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_OWNERS,
                attribute_generator: cyclic(DISTINCT_VALUES),
            },
            HasSpec {
                owner_type: OWNER_2, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_OWNERS,
                attribute_generator: cyclic(DISTINCT_VALUES),
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());
    let merges = multi_iter_intersection_steps(&pipeline);
    assert!(
        !merges.is_empty(),
        "heavy_cartesian: planner should pick a merge intersection \
         (50:1 per-value fan-out — sequential per-row probe cost dominates); \
         found none",
    );

    let (rows, profile) = execute_read(pipeline);
    assert_eq!(rows, EXPECTED_ROWS, "heavy_cartesian: 10 values × 50 × 50 = 25000 rows");
    let (ratio, advances, prof_rows, descr) = worst_advances_per_row(&profile);
    assert!(
        ratio < 20.0,
        "heavy_cartesian: worst step should be O(1) per row (< 20 advances/row); \
         got {ratio:.2} ({advances}/{prof_rows}). step: {descr}",
    );
}

/// Moderate fan-out at higher cardinality. Both sides 1000 owners cyclic over
/// 100 distinct values (10 owners per value per side). Output = 100 × 10 × 10
/// = 10000 rows. Same regime as `moderate_cartesian` but at 2× scale.
///
/// Expected (cost model): same shape as moderate_cartesian, just larger —
/// merge expected to win by ~5× (sequential's per-outer-row cost scales
/// linearly with |outer|, merge's scan does too but with a smaller constant).
///
/// Actually observed (profile dump at test scale, opt mode):
///   forced sequential: 32.59 ms total — join step: 0.1 seek + 1.0 adv per row (10000 rows)
///   forced merge:     112.63 ms total — join step: 0.5 seek + 6.6 adv per row
///                                       →  sequential wins by 3.5×
/// Planner picks sequential. **Test currently FAILS its plan-shape assertion**.
/// Per-row counters match the moderate_cartesian shape almost exactly —
/// scaling N does not change merge's per-row overhead, so it never closes.
#[test]
fn merge_wins_at_scale_with_fanout() {
    const N_OWNERS: usize = 1000;
    const DISTINCT_VALUES: usize = 100;
    const OWNERS_PER_VALUE: usize = N_OWNERS / DISTINCT_VALUES; // 10
    const EXPECTED_ROWS: usize = DISTINCT_VALUES * OWNERS_PER_VALUE * OWNERS_PER_VALUE; // 10000

    let mut context = setup();
    define_two_owner_schema(&mut context);

    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N_OWNERS, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_OWNERS, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_OWNERS,
                attribute_generator: cyclic(DISTINCT_VALUES),
            },
            HasSpec {
                owner_type: OWNER_2, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_OWNERS,
                attribute_generator: cyclic(DISTINCT_VALUES),
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());
    let merges = multi_iter_intersection_steps(&pipeline);
    assert!(
        !merges.is_empty(),
        "at_scale_with_fanout: planner should pick a merge intersection \
         (cardinality scaled — sequential's linear per-row cost dominates); \
         found none",
    );

    let (rows, profile) = execute_read(pipeline);
    assert_eq!(rows, EXPECTED_ROWS, "at_scale_with_fanout: 100 values × 10 × 10 = 10000 rows");
    let (ratio, advances, prof_rows, descr) = worst_advances_per_row(&profile);
    assert!(
        ratio < 20.0,
        "at_scale_with_fanout: worst step should be O(1) per row (< 20 advances/row); \
         got {ratio:.2} ({advances}/{prof_rows}). step: {descr}",
    );
}

// === Sequential-must-win tests ===============================================================

/// Extreme cardinality asymmetry. owner_1: 1 owner with value 0; owner_2:
/// 2000 owners with unique values 0..1999 → 1 output row.
///
/// Expected (cost model): sequential pays ~1 outer + 1 tight bound-from
/// probe ≈ 10 cost units; merge pays ~1 + 2000 (both iters walk full inner).
/// Sequential expected to win by ~200×. A merge plan here would be
/// catastrophic — the test is the primary guard against that.
///
/// Actually observed (profile dump at test scale, opt mode):
///   forced sequential: 123 µs total — join step: 1 seek + 1 adv (1 row)
///   forced merge:      628 µs total — join step: 2 seeks + 2003 advs (1 row)
///                                    →  sequential wins by 5.1×
/// Planner picks sequential. **Test PASSES.** The merge step is forced to
/// scan the entire 2000-entry inner range looking for the 1 matching value
/// (2003 advances) — exactly the catastrophic O(N_inner) work this test
/// guards against. Sequential's bind-from probe lands the value in 1 seek.
#[test]
fn sequential_wins_tiny_outer_huge_inner() {
    const N_INNER: usize = 2000;

    let mut context = setup();
    define_two_owner_schema(&mut context);

    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: 1, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_INNER, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1, attr_type: JOIN_ATTR,
                count_each: 1, count_total: 1,
                attribute_generator: sequential(),
            },
            HasSpec {
                owner_type: OWNER_2, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_INNER,
                attribute_generator: sequential(),
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());
    let merges = multi_iter_intersection_steps(&pipeline);
    assert!(
        merges.is_empty(),
        "tiny_outer_huge_inner: planner should pick sequential \
         (1 × {N_INNER} cardinality asymmetry — merge would do O(N_INNER) work for 1 row); \
         found {} multi-iter merge step(s)",
        merges.len(),
    );

    let (rows, profile) = execute_read(pipeline);
    assert_eq!(rows, 1, "tiny_outer_huge_inner: 1 outer × 1 matching inner = 1 row");
    // Worst-step ratio: with 1 row of output, ratio = advances. Bound generously
    // to catch only catastrophic plans (a merge would do ~2000 advances on its
    // inner-scan step).
    let (ratio, advances, prof_rows, descr) = worst_advances_per_row(&profile);
    assert!(
        ratio < (N_INNER as f64) * 0.5,
        "tiny_outer_huge_inner: worst step ratio {ratio:.2} ({advances}/{prof_rows}) \
         exceeds {} — a catastrophically-bad plan is likely. step: {descr}",
        (N_INNER as f64) * 0.5,
    );
}

/// Small outer ⊂ huge inner (the bug case, scaled up). owner_1: 100 owners
/// with values 0..99 (full coverage of own range); owner_2: 2000 owners with
/// unique values 0..1999 (5% coverage of inner's domain by outer). Output:
/// 100 rows. Regression test for the blend penalty on outer-side waste.
///
/// Expected (cost model): sequential pays ~100 outer + 100 tight probes ≈
/// 700; merge pays ~100 + 2000 (both iters walk inner range to find 100
/// overlapping values). Sequential expected to win by ~3×.
///
/// Actually observed (profile dump at test scale, opt mode):
///   forced sequential: 937 µs total — join step: 1.0 seek + 1.0 adv per row (100 rows)
///   forced merge:      2.89 ms total — join step: 3.0 seeks + 7.9 adv per row
///                                       →  sequential wins by 3.1×
/// Planner picks sequential. **Test PASSES.** The blend penalty in `Cost::join`
/// correctly steers the planner here. Per-row counters match symmetric_balanced
/// closely (3 seeks + ~8 advances in the merge step regardless of waste).
#[test]
fn sequential_wins_subset_coverage() {
    const N_OUTER: usize = 100;
    const N_INNER: usize = 2000;

    let mut context = setup();
    define_two_owner_schema(&mut context);

    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N_OUTER, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_INNER, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_OUTER,
                attribute_generator: sequential(), // values 0..99
            },
            HasSpec {
                owner_type: OWNER_2, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_INNER,
                attribute_generator: sequential(), // values 0..1999, outer ⊂ inner
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());
    let merges = multi_iter_intersection_steps(&pipeline);
    assert!(
        merges.is_empty(),
        "subset_coverage: planner should pick sequential \
         (outer's 100 values cover only 5% of inner's 2000-value domain — \
         merge would waste 95% of its inner scan); \
         found {} multi-iter merge step(s)",
        merges.len(),
    );

    let (rows, profile) = execute_read(pipeline);
    assert_eq!(rows, N_OUTER, "subset_coverage: outer ⊂ inner → N_OUTER rows");
    let (ratio, advances, prof_rows, descr) = worst_advances_per_row(&profile);
    assert!(
        ratio < 10.0,
        "subset_coverage: worst step should be tight (< 10 advances/row); \
         got {ratio:.2} ({advances}/{prof_rows}). step: {descr}",
    );
}

/// Noise inflates the storage scan range for any `Reverse[X has $join]` iter,
/// so merge pays for the bloated range twice (once per side) while sequential
/// pays once on the outer scan plus tight bound-from probes on the inner.
/// owner_1: 100 owners values 0..99; owner_2: 100 owners values 0..99 (full
/// overlap with outer); plus 2000 noise entries with values 100..2099
/// inflating the value range. Output: 100 rows.
///
/// Expected (cost model): sequential ≈ 2100 outer + 100×tight = ~2700;
/// merge ≈ 2 × 2100 = ~4200. Sequential expected to win by ~1.5×.
///
/// Actually observed (profile dump at test scale, opt mode):
///   forced sequential: 882 µs total — join step: 1.0 seek + 1.0 adv per row (100 rows)
///   forced merge:      3.31 ms total — join step: 3.0 seeks + 26.0 adv per row
///                                       →  sequential wins by 3.8×
/// Planner picks sequential. **Test PASSES.** Noise inflates the per-row
/// advance count from ~8 (subset_coverage) to ~26 here — the merge has to
/// walk past every noise entry in the bloated value range, while sequential's
/// bind-from probe seeks directly to each matched value (still 1+1 per row).
#[test]
fn sequential_wins_noisy_inner() {
    const N_QUERY: usize = 100;
    const N_NOISE_TYPES: usize = 2;
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
                attribute_generator: sequential(), // values 0..99
            },
            HasSpec {
                owner_type: OWNER_2, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_QUERY,
                attribute_generator: sequential(), // values 0..99 (full overlap with owner_1)
            },
        ],
    };
    // Noise values 100..2099 — disjoint from query values, but bloat the scan range
    // of any reverse[has $join] iterator.
    add_noise_owners(&mut spec, N_NOISE_TYPES, N_NOISE_PER_TYPE, N_QUERY as i64);
    load_data(&mut context, spec);

    let pipeline = compile_read(&context, &two_owner_join_query());
    let merges = multi_iter_intersection_steps(&pipeline);
    assert!(
        merges.is_empty(),
        "noisy_inner: planner should pick sequential \
         (storage scan range bloated by noise — merge would double-pay the bloated scan); \
         found {} multi-iter merge step(s)",
        merges.len(),
    );

    let (rows, profile) = execute_read(pipeline);
    assert_eq!(rows, N_QUERY, "noisy_inner: query sides fully overlap → N_QUERY rows");
    let (ratio, advances, prof_rows, descr) = worst_advances_per_row(&profile);
    assert!(
        ratio < 30.0,
        "noisy_inner: worst step bounded by outer scan / matches ratio (< 30 advances/row); \
         got {ratio:.2} ({advances}/{prof_rows}). step: {descr}",
    );
}

/// Small outer × huge inner with small overlap. owner_1: 10 owners values
/// 0..9; owner_2: 2000 owners values 5..2004 (overlap = 5 values: 5..9).
/// Output: 5 rows. Distinct from `subset_coverage` because the outer is
/// small (extreme outer-side selectivity) and the intersection is even
/// tinier than the outer itself.
///
/// Expected (cost model): sequential pays ~10 outer + 10×tight ≈ 60;
/// merge pays ~10 + 2000 ≈ 2010. Sequential expected to win by ~30×.
///
/// Actually observed (profile dump at test scale, opt mode):
///   forced sequential: 149 µs total — join step: 2.0 seeks + 1.0 adv per row (5 rows)
///   forced merge:      327 µs total — join step: 4.4 seeks + 18.6 adv per row
///                                    →  sequential wins by 2.2×
/// Planner picks sequential. **Test PASSES.** Merge's per-row work scales
/// with the wasted inner-range walks (the outer's 10 values are far apart
/// in the 2000-value inner domain), giving ~19 advances/row vs sequential's
/// 1. The wall-clock gap stays small only because absolute times are tiny.
#[test]
fn sequential_wins_asymmetric_coverage() {
    const N_OUTER: usize = 10;
    const N_INNER: usize = 2000;
    const INNER_OFFSET: i64 = 5; // shifts inner so only outer's 5..9 overlap
    const EXPECTED_ROWS: usize = (N_OUTER as i64 - INNER_OFFSET) as usize; // 5

    let mut context = setup();
    define_two_owner_schema(&mut context);

    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: N_OUTER, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: N_INNER, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_OUTER,
                attribute_generator: sequential(), // values 0..9
            },
            HasSpec {
                owner_type: OWNER_2, attr_type: JOIN_ATTR,
                count_each: 1, count_total: N_INNER,
                attribute_generator: offset_unique(INNER_OFFSET), // values 5..2004
            },
        ],
    };
    load_data(&mut context, data_spec);

    let pipeline = compile_read(&context, &two_owner_join_query());
    let merges = multi_iter_intersection_steps(&pipeline);
    assert!(
        merges.is_empty(),
        "asymmetric_coverage: planner should pick sequential \
         (10 outer × {N_INNER} inner, overlap = {EXPECTED_ROWS} — \
         merge wastes all but ~0.25% of its scan); \
         found {} multi-iter merge step(s)",
        merges.len(),
    );

    let (rows, profile) = execute_read(pipeline);
    assert_eq!(rows, EXPECTED_ROWS, "asymmetric_coverage: 5 overlapping values × 1 × 1 = 5 rows");
    let (ratio, advances, prof_rows, descr) = worst_advances_per_row(&profile);
    // Bound: with 5 rows of output, a healthy sequential plan does O(outer + per-match-probe)
    // ≈ 15 advances. Cap at N_INNER * 0.5 to catch only catastrophic merge-style scans.
    assert!(
        ratio < (N_INNER as f64) * 0.5,
        "asymmetric_coverage: worst step ratio {ratio:.2} ({advances}/{prof_rows}) \
         exceeds {} — a catastrophically-bad plan is likely. step: {descr}",
        (N_INNER as f64) * 0.5,
    );
}
