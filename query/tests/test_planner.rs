/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Planner-focused integration tests. Each test builds a small schema, populates
//! data via a `DataSpec`-driven loader, runs a read query, and asserts that the
//! planner picked the plan shape (merge vs sequential) we'd expect for the
//! scenario. Row count and per-step storage work are also checked.
//!
//! The 9 tests are "must-pick-X" scenarios: data is shaped so one plan shape is
//! the theoretical right answer, and we assert the planner picks it. The
//! assertions reflect theoretical expectation, not the planner's current
//! choices — some tests may fail because the planner's cost model doesn't yet
//! pick the theoretically-best plan for that shape. Those failures are
//! intentional markers for cost-model gaps to fix.
//!
//! Merge-wins scenarios (cost model should prefer merge):
//! - `merge_wins_multi_attr_filter`          — K patterns on one entity
//! - `merge_wins_symmetric_balanced`         — 1:1 baseline, full coverage
//! - `merge_wins_moderate_cartesian`         — 10:1 per-value fan-out
//! - `merge_wins_heavy_cartesian`            — 50:1 per-value fan-out
//! - `merge_wins_at_scale_with_fanout`       — moderate fan-out, higher card
//!
//! Sequential-wins scenarios (cost model should prefer sequential):
//! - `sequential_wins_tiny_outer_huge_inner` — 1 outer × N inner
//! - `sequential_wins_subset_coverage`       — small outer ⊂ huge inner
//! - `sequential_wins_noisy_inner`           — noise inflates scan range
//! - `sequential_wins_asymmetric_coverage`   — small outer × huge inner

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
/// values 0..499 → 500 output rows. No waste, no coverage gap, no noise —
/// textbook merge-win shape: both sides pre-sorted on the join attribute,
/// dense overlap. A 2-iter merge intersection on the attribute should beat
/// sequential's per-outer-row probe.
///
/// Cost-model estimate (SEEK=5, ADVANCE=1):
/// - sequential chain ≈ 500 × (SEEK + 1 ADV) ≈ 3000
/// - 2-iter merge    ≈ 2 × (SEEK + 500 ADV)  ≈ 1010
/// Merge expected to win by ~3×.
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
/// (cartesian within each value). Per-value cartesian should amortize merge's
/// scan over many emits, so merge should win.
///
/// Cost-model estimate:
/// - sequential chain ≈ 500 × (SEEK + 10 ADV) ≈ 7500
/// - 2-iter merge    ≈ 1000 + per-value cartesian sub-iter
/// Merge expected to win by ~5×.
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
/// Each output value has a per-side cartesian cluster, so merge's cartesian
/// sub-iter should dominate sequential's per-outer probe.
///
/// Cost-model estimate:
/// - sequential chain ≈ 500 × (SEEK + 50 ADV) ≈ 27500
/// - 2-iter merge    ≈ 1000 + per-value cartesian (~10 reopens, big amortization)
/// Merge expected to win decisively (~25×) — this is the regime where per-value
/// cartesian most clearly pays off.
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
         (50:1 per-value fan-out — the cartesian sub-iter should amortize \
         scan cost over many emits per matched value); found none",
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
/// = 10000 rows. Same regime as `moderate_cartesian` but at 2× scale — checks
/// that the planner's pick stays consistent as cardinality grows.
///
/// Cost-model estimate:
/// - sequential chain ≈ 1000 × (SEEK + 10 ADV) ≈ 15000
/// - 2-iter merge    ≈ 2000 + per-value cartesian sub-iter
/// Merge expected to win by ~5×, same shape as moderate_cartesian.
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

// --- Multi-attribute filter helpers (single-entity, K-pattern intersection) -----------------

const WIDGET: &str = "widget";
const WIDGET_KEY: &str = "widget_key";
const MULTI_ATTR_TYPES: &[&str] = &[
    "m_attr_0", "m_attr_1", "m_attr_2", "m_attr_3", "m_attr_4",
    "m_attr_5", "m_attr_6", "m_attr_7", "m_attr_8", "m_attr_9",
];

fn define_multi_attr_schema(context: &mut Context, k_attrs: usize) {
    assert!(k_attrs >= 2 && k_attrs <= MULTI_ATTR_TYPES.len());
    let mut schema = format!("define entity {WIDGET} owns {WIDGET_KEY} @key");
    for i in 0..k_attrs {
        schema.push_str(&format!(", owns {}", MULTI_ATTR_TYPES[i]));
    }
    schema.push_str(&format!("; attribute {WIDGET_KEY}, value integer;"));
    for i in 0..k_attrs {
        schema.push_str(&format!(" attribute {}, value integer;", MULTI_ATTR_TYPES[i]));
    }
    define_schema(context, &schema);
}

fn multi_attr_query(k_attrs: usize, fixed_value: i64) -> String {
    let mut q = format!("match $x isa {WIDGET}");
    for i in 0..k_attrs {
        q.push_str(&format!(", has {} {fixed_value}", MULTI_ATTR_TYPES[i]));
    }
    q.push(';');
    q
}

fn build_multi_attr_spec(n_owners: usize, k_attrs: usize, m_values: usize) -> DataSpec {
    assert!(k_attrs <= MULTI_ATTR_TYPES.len());
    let mut spec = DataSpec {
        instances: vec![InstanceSpec { type_: WIDGET, count: n_owners, key: Some(WIDGET_KEY) }],
        has: vec![],
    };
    for i in 0..k_attrs {
        // Base-M digit-position: widget j gets value (j / m^i) % m for attr_i.
        // This gives a uniform combinatorial assignment over (val_0, ..., val_{K-1}).
        let divisor: usize = m_values.pow(i as u32);
        let modulus = m_values;
        spec.has.push(HasSpec {
            owner_type: WIDGET, attr_type: MULTI_ATTR_TYPES[i],
            count_each: 1, count_total: n_owners,
            attribute_generator: Box::new(move |edge_idx| {
                // With count_total = n_owners and count_each = 1, edge_idx == owner_idx.
                ((edge_idx / divisor) % modulus) as i64
            }),
        });
    }
    spec
}

// --- merge_wins_multi_attr_filter ------------------------------------------------------------

/// Multi-attribute filter on a single entity. One `widget` entity owning K=3
/// integer attributes; query constrains all 3 to a fixed value:
///   `match $x isa widget, has attr_0 0, has attr_1 0, has attr_2 0;`
///
/// Data: N=1000 widgets, K=3 attrs × M=3 values each. Values are assigned by
/// base-M digit positions, so each combination appears N/M^K = ~37 times.
///
/// The K `Reverse[has(attr_i = 0)]` iterators are all pre-sorted by owner-id
/// (storage layout `[has-reverse][attr_type][attr_value][owner_type][owner_iid]`,
/// with attr_type and attr_value pinned by the literal). Merge can co-walk all
/// K owner-sorted iters in a single pass on $x; sequential would have to drive
/// from one filter and bind-from probe each of the other K-1 patterns per row.
/// This is the canonical merge-wins shape (entity-id axis, equally selective
/// per-attribute filters).
///
/// Cost-model estimate:
/// - sequential cascade ≈ 333 outer + 2 × (333 × bound-probe) ≈ ~1300
/// - K-iter merge       ≈ K × 333 + per-row intersection compare ≈ ~1000
/// Merge expected to win (cascading per-row pipeline overhead in the
/// sequential plan is paid on intermediate row counts, not just on the final
/// ~37 outputs).
#[test]
fn merge_wins_multi_attr_filter() {
    const N: usize = 1_000;
    const K: usize = 3;
    const M: usize = 3;
    const EXPECTED_ROWS: usize = N / (M * M * M); // 1000 / 27 = 37

    let mut context = setup();
    define_multi_attr_schema(&mut context, K);
    load_data(&mut context, build_multi_attr_spec(N, K, M));

    let pipeline = compile_read(&context, &multi_attr_query(K, 0));
    let merges = multi_iter_intersection_steps(&pipeline);
    assert!(
        !merges.is_empty(),
        "multi_attr_filter: planner should pick a merge intersection \
         (K={K} same-entity attribute filters, all owner-id sorted — \
         the canonical merge-wins shape); found none",
    );

    let (rows, profile) = execute_read(pipeline);
    // Each base-M combination appears floor(N / M^K) or ceil(N / M^K) times due to
    // integer arithmetic in the digit-position generator. Allow ±a few rows of slack.
    let lo = EXPECTED_ROWS.saturating_sub(2);
    let hi = EXPECTED_ROWS + 2;
    assert!(
        (lo..=hi).contains(&rows),
        "multi_attr_filter: expected ~{EXPECTED_ROWS} rows (= N / M^K with N={N}, M={M}, K={K}); \
         got {rows}",
    );
    let (ratio, advances, prof_rows, descr) = worst_advances_per_row(&profile);
    // Merge co-walks K iters each of ~N/M size. Per output row, roughly ~K × M
    // advances of "scan one position on each side"; bound generously at 30/row to
    // catch only a catastrophically-bad plan.
    assert!(
        ratio < 30.0,
        "multi_attr_filter: worst step should be bounded (< 30 advances/row); \
         got {ratio:.2} ({advances}/{prof_rows}). step: {descr}",
    );
}

// === Sequential-must-win tests ===============================================================

/// Extreme cardinality asymmetry. owner_1: 1 owner with value 0; owner_2:
/// 2000 owners with unique values 0..1999 → 1 output row. A merge here would
/// have to scan the entire inner range for a single matching value;
/// sequential's bind-from probe lands directly. Primary guard against
/// catastrophic O(N_inner) merge picks.
///
/// Cost-model estimate:
/// - sequential chain ≈ 1 outer + 1 × (SEEK + 1 ADV) ≈ ~10
/// - 2-iter merge    ≈ 2 × (SEEK + ~2000 ADV) ≈ ~2010
/// Sequential expected to win by ~200×.
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

/// Small outer ⊂ huge inner. owner_1: 100 owners with values 0..99 (full
/// coverage of own range); owner_2: 2000 owners with unique values 0..1999
/// (outer covers 5% of inner's value domain). Output: 100 rows. Regression
/// test for the blend penalty on outer-side waste: the small outer should
/// drive bound probes into inner, not the other way around.
///
/// Cost-model estimate:
/// - sequential chain ≈ 100 outer + 100 × (SEEK + 1 ADV) ≈ ~700
/// - 2-iter merge    ≈ 100 + 2000 (both iters walk inner range to find the
///   100 overlapping values) ≈ ~2100
/// Sequential expected to win by ~3×.
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
/// inflating the value range. Output: 100 rows. Sequential should win.
///
/// Cost-model estimate (noise puts the effective per-side scan at ~2100):
/// - sequential chain ≈ 2100 outer + 100 × (SEEK + 1 ADV) ≈ ~2700
/// - 2-iter merge    ≈ 2 × 2100 (both iters walk the bloated range) ≈ ~4200
/// Sequential expected to win by ~1.5×.
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
/// tinier than the outer itself. Sequential should win.
///
/// Cost-model estimate:
/// - sequential chain ≈ 10 outer + 10 × (SEEK + 1 ADV) ≈ ~60
/// - 2-iter merge    ≈ 10 + 2000 (merge wastes most of its scan) ≈ ~2010
/// Sequential expected to win by ~30×.
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
