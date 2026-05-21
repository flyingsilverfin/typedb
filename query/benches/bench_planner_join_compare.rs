/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Microbench comparing merge-intersection vs sequential plans on the same data,
//! across scenarios where the cost model's pick is non-obvious. Each scenario:
//!   1. Loads data once (large scale).
//!   2. Compiles + executes the join query N times with the planner's natural
//!      choice (sequential at moderate fan-out, merge at heavy fan-out).
//!   3. Repeats N times with FORCE_MERGE_INTERSECTION=1, which zeros the blend
//!      penalty in the cost model so merge always wins planner selection.
//!   4. Reports per-iteration wall-clock for both plans so we can tell which
//!      one is actually faster at runtime.
//!
//! Plan-shape detection: each iteration's pipeline is structurally inspected to
//! confirm which plan the planner picked (merge vs sequential). The bench
//! prints a "merges=" count alongside the timings.
//!
//! Run with:
//!   bazel run --compilation_mode=opt \
//!     //tests/benchmarks/planner_join_compare:bench_planner_join_compare

#![deny(unused_must_use)]

use std::{collections::HashMap, sync::Arc, time::{Duration, Instant}};

use compiler::executable::match_::planner::conjunction_executable::ExecutionStep;
use concept::{
    thing::{statistics::Statistics, thing_manager::ThingManager},
    type_::type_manager::{TypeManager, type_cache::TypeCache},
};
use durability::DurabilitySequenceNumber;
use encoding::graph::{
    definition::definition_key_generator::DefinitionKeyGenerator, thing::vertex_generator::ThingVertexGenerator,
};
use executor::{
    ExecutionInterrupt,
    pipeline::{
        pipeline::Pipeline,
        stage::{ExecutionContext, ReadPipelineStage, StageIterator},
    },
};
use function::function_manager::FunctionManager;
use query::{options::QueryOptions, query_manager::QueryManager};
use resource::profile::CommitProfile;
use storage::{
    MVCCStorage,
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, ReadSnapshot},
};
use test_utils::TempDir;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

// --- Context & scaffolding (lightly adapted from query/tests/test_planner.rs) -----------------

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
        // No QueryCache: each compile_read re-runs the planner so env-var toggling takes effect.
        self.query_manager = QueryManager::new(None);
    }
}

fn setup() -> Context {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new(None);
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
        let (_iterator, exec_context) =
            pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
        snapshot = Arc::into_inner(exec_context.snapshot).unwrap();
    }
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    context.refresh();
}

fn compile_read(
    context: &Context,
    query: &str,
) -> Pipeline<ReadSnapshot<WALClient>, ReadPipelineStage<ReadSnapshot<WALClient>>> {
    compile_read_inner(context, query, QueryOptions::default())
}

fn compile_read_profiled(
    context: &Context,
    query: &str,
) -> Pipeline<ReadSnapshot<WALClient>, ReadPipelineStage<ReadSnapshot<WALClient>>> {
    compile_read_inner(context, query, QueryOptions { force_query_profile: true })
}

fn compile_read_inner(
    context: &Context,
    query: &str,
    options: QueryOptions,
) -> Pipeline<ReadSnapshot<WALClient>, ReadPipelineStage<ReadSnapshot<WALClient>>> {
    let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
    let parsed_query = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    context
        .query_manager
        .prepare_read_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &parsed_query,
            query,
            options,
        )
        .unwrap()
}

fn execute_read(
    pipeline: Pipeline<ReadSnapshot<WALClient>, ReadPipelineStage<ReadSnapshot<WALClient>>>,
) -> usize {
    let (iterator, ExecutionContext { .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    iterator.collect_owned().unwrap().len()
}

fn pipeline_has_multi_iter_merge(
    pipeline: &Pipeline<ReadSnapshot<WALClient>, ReadPipelineStage<ReadSnapshot<WALClient>>>,
) -> bool {
    pipeline
        .stages()
        .iter()
        .filter_map(|s| s.as_match())
        .flat_map(|m| m.executable().steps())
        .any(|s| matches!(s, ExecutionStep::Intersection(i) if i.instructions.len() >= 2))
}

// --- DataSpec (copied from test_planner.rs) ---------------------------------------------------

struct DataSpec {
    instances: Vec<InstanceSpec>,
    has: Vec<HasSpec>,
}

struct InstanceSpec {
    type_: &'static str,
    count: usize,
    key: Option<&'static str>,
}

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
    count_each: usize,
    count_total: usize,
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
        assert!(owner_count > 0);
        let max_edges = owner_count.saturating_mul(*count_each);
        assert!(*count_total <= max_edges);
        if *count_total == 0 {
            continue;
        }
        let key_label = spec.instances.iter().find(|i| i.type_ == *owner_type).and_then(|i| i.key).unwrap();
        let mut owner_edge_counts = vec![0usize; owner_count];
        for e in 0..*count_total {
            let owner_idx = e % owner_count;
            owner_edge_counts[owner_idx] += 1;
            assert!(owner_edge_counts[owner_idx] <= *count_each);
            let value = attribute_generator(e);
            queries.push(format!(
                "match $o isa {owner_type}, has {key_label} {owner_idx}; \
                 insert $o has {attr_type} {value};"
            ));
        }
    }

    commit_writes(context, &queries);
}

// --- Env-var helpers (single-threaded bench, so the unsafe is sound) --------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Natural,
    ForceMerge,
    ForceNoMerge,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Mode::Natural => "natural",
            Mode::ForceMerge => "force_merge",
            Mode::ForceNoMerge => "force_sequential",
        }
    }
}

fn apply_mode(mode: Mode) {
    unsafe {
        std::env::remove_var("FORCE_MERGE_INTERSECTION");
        std::env::remove_var("FORCE_NO_MERGE_INTERSECTION");
        std::env::remove_var("FORCE_HAS_REVERSE");
        match mode {
            Mode::Natural => {}
            Mode::ForceMerge => {
                // Combine: ForceMerge zeros the join cost; HasReverse forces unbound Has
                // patterns to use the Reverse direction (without this, canonical/reverse
                // tie-break in symmetric data picks Canonical, which joins on owner instead
                // of on the attribute variable — no pure 2-iter merge possible).
                std::env::set_var("FORCE_MERGE_INTERSECTION", "1");
                std::env::set_var("FORCE_HAS_REVERSE", "1");
            }
            Mode::ForceNoMerge => std::env::set_var("FORCE_NO_MERGE_INTERSECTION", "1"),
        }
    }
}

// --- Two-owner shared schema ------------------------------------------------------------------

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

// --- Noise schema (for sequential_wins_noisy_inner scenario) ----------------------------------

const NOISE_TYPES: &[&str] = &["noise_1", "noise_2", "noise_3", "noise_4", "noise_5"];
const NOISE_KEY: &str = "noise_id";

fn define_two_owner_with_noise_schema(context: &mut Context, n_noise_types: usize) {
    assert!(n_noise_types <= NOISE_TYPES.len());
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

// --- N-way join schema/data/query (sweep over join arity) -----------------------------------

// Pre-declared owner+key labels for up to 12 sides of the same-variable join. Static strings
// are required by the existing DataSpec/InstanceSpec API.
const NWAY_OWNERS: &[&str] = &[
    "nway_owner_0",  "nway_owner_1",  "nway_owner_2",  "nway_owner_3",
    "nway_owner_4",  "nway_owner_5",  "nway_owner_6",  "nway_owner_7",
    "nway_owner_8",  "nway_owner_9",  "nway_owner_10", "nway_owner_11",
];
const NWAY_KEYS: &[&str] = &[
    "nway_key_0",  "nway_key_1",  "nway_key_2",  "nway_key_3",
    "nway_key_4",  "nway_key_5",  "nway_key_6",  "nway_key_7",
    "nway_key_8",  "nway_key_9",  "nway_key_10", "nway_key_11",
];

fn define_nway_join_schema(context: &mut Context, n_sides: usize) {
    assert!(n_sides >= 2 && n_sides <= NWAY_OWNERS.len());
    let mut schema = String::from("define ");
    for i in 0..n_sides {
        let owner = NWAY_OWNERS[i];
        let key = NWAY_KEYS[i];
        schema.push_str(&format!("entity {owner} owns {key} @key, owns {JOIN_ATTR}; "));
        schema.push_str(&format!("attribute {key}, value integer; "));
    }
    schema.push_str(&format!("attribute {JOIN_ATTR}, value integer;"));
    define_schema(context, &schema);
}

fn nway_join_query(n_sides: usize) -> String {
    let mut q = String::from("match ");
    for i in 0..n_sides {
        let owner = NWAY_OWNERS[i];
        q.push_str(&format!("$e{i} isa {owner}, has {JOIN_ATTR} $join; "));
    }
    q
}

/// Build a DataSpec for an N-way join where every side has the same M owners with
/// sequential values 0..M-1 (1:1 full coverage). With this shape, the N-way intersection
/// produces M output rows (one tuple per value, one entity per side).
fn build_nway_spec(n_sides: usize, n_owners_per_side: usize) -> DataSpec {
    assert!(n_sides >= 2 && n_sides <= NWAY_OWNERS.len());
    let mut spec = DataSpec { instances: vec![], has: vec![] };
    for i in 0..n_sides {
        spec.instances.push(InstanceSpec {
            type_: NWAY_OWNERS[i], count: n_owners_per_side, key: Some(NWAY_KEYS[i]),
        });
        spec.has.push(HasSpec {
            owner_type: NWAY_OWNERS[i], attr_type: JOIN_ATTR,
            count_each: 1, count_total: n_owners_per_side,
            attribute_generator: sequential(),
        });
    }
    spec
}

// --- Multi-attribute filter on a single entity (sweep over K attribute patterns) ------------
//
// Shape: `match $x isa widget, has attr_0 0, has attr_1 0, ..., has attr_{K-1} 0;`
// Each `has attr_i 0` constrains $x via a `Reverse[has(attr_i = 0)]` iter that's pre-sorted
// by owner-id. Sequential drives from one such set, probes others with bind-from on $x.
// Merge intersects all K sorted-by-owner-id iters in lockstep.
//
// Data layout: N widgets, each with K attribute edges. For attribute i and widget j, the
// value is (j / M^i) % M — base-M digit-position assignment, giving a uniform combinatorial
// distribution over (val_0, val_1, ..., val_{K-1}). Expected output for the all-zeros query
// is N / M^K rows (1 widget per combo, repeated when N > M^K).

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

fn load_multi_attr_data(context: &mut Context, n_owners: usize, k_attrs: usize, m_values: usize) {
    assert!(k_attrs <= MULTI_ATTR_TYPES.len());
    let mut spec = DataSpec {
        instances: vec![InstanceSpec { type_: WIDGET, count: n_owners, key: Some(WIDGET_KEY) }],
        has: vec![],
    };
    for i in 0..k_attrs {
        // Base-M digit-position: widget j gets value (j / m^i) % m for attr_i.
        // This gives a uniform combinatorial assignment.
        let divisor: usize = m_values.pow(i as u32);
        let modulus = m_values;
        spec.has.push(HasSpec {
            owner_type: WIDGET, attr_type: MULTI_ATTR_TYPES[i],
            count_each: 1, count_total: n_owners,
            attribute_generator: Box::new(move |edge_idx| {
                // load_data assigns edge `e` to owner `e % owner_count` round-robin;
                // with count_total = n_owners and count_each = 1, edge_idx == owner_idx.
                ((edge_idx / divisor) % modulus) as i64
            }),
        });
    }
    load_data(context, spec);
}

// --- Bench harness ----------------------------------------------------------------------------

fn build_symmetric_spec(n_owners: usize, distinct_values: usize) -> DataSpec {
    DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: n_owners, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: n_owners, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1, attr_type: JOIN_ATTR,
                count_each: 1, count_total: n_owners,
                attribute_generator: if distinct_values >= n_owners {
                    sequential()
                } else {
                    cyclic(distinct_values)
                },
            },
            HasSpec {
                owner_type: OWNER_2, attr_type: JOIN_ATTR,
                count_each: 1, count_total: n_owners,
                attribute_generator: if distinct_values >= n_owners {
                    sequential()
                } else {
                    cyclic(distinct_values)
                },
            },
        ],
    }
}

struct ModeResult {
    mode: Mode,
    picked_merge: bool,
    output_rows: usize,
    total: Duration,
}

struct BenchResult {
    label: &'static str,
    shape_label: String,
    iterations: usize,
    results: [ModeResult; 3],
}

impl BenchResult {
    fn natural(&self) -> &ModeResult { &self.results[0] }
    fn forced_merge(&self) -> &ModeResult { &self.results[1] }
    fn forced_seq(&self) -> &ModeResult { &self.results[2] }

    fn print(&self) {
        let iters = self.iterations as u32;
        println!(
            "  {label:<40} {shape}  out={out:>8} iters={iters:>4}",
            label = self.label, shape = self.shape_label,
            out = self.natural().output_rows, iters = iters,
        );
        for r in &self.results {
            let per = r.total / iters;
            let pick = if r.picked_merge { "merge     " } else { "sequential" };
            println!(
                "    {mode:<18} -> {pick:<10}  {per:>9.2?}/iter  total {total:>9.2?}",
                mode = r.mode.label(), pick = pick, per = per, total = r.total,
            );
        }
        // Pick the two modes that actually produced different plans and compare.
        let merge_run = if self.forced_merge().picked_merge { Some(self.forced_merge()) }
                        else if self.natural().picked_merge { Some(self.natural()) }
                        else { None };
        let seq_run = if !self.forced_seq().picked_merge { Some(self.forced_seq()) }
                      else if !self.natural().picked_merge { Some(self.natural()) }
                      else { None };
        match (merge_run, seq_run) {
            (Some(m), Some(s)) => {
                let m_per = m.total.as_secs_f64() / iters as f64;
                let s_per = s.total.as_secs_f64() / iters as f64;
                if s_per < m_per {
                    println!("    => sequential beats merge by {:.2}x", m_per / s_per);
                } else {
                    println!("    => merge beats sequential by {:.2}x", s_per / m_per);
                }
            }
            _ => println!("    (could not isolate both plan shapes; comparison skipped)"),
        }
    }
}

// --- Profile dump (BENCH_PROFILE_DUMP=1): runs each test scenario with profiling
// enabled and dumps both forced-merge and forced-sequential QueryProfiles. -------------------

fn dump_profile_for_scenario(
    label: &str,
    shape_label: &str,
    query: &str,
    setup_data: impl FnOnce(&mut Context),
) {
    println!("================================================================");
    println!("=== SCENARIO: {label}");
    println!("=== {shape_label}");
    println!("================================================================");
    let setup_start = Instant::now();
    let mut context = setup();
    setup_data(&mut context);
    println!("[setup] data loaded in {:.2?}", setup_start.elapsed());
    println!();

    // Warmup (page cache, etc.) with whatever plan the planner naturally picks.
    apply_mode(Mode::Natural);
    let _ = execute_read(compile_read(&context, query));

    for mode in [Mode::ForceNoMerge, Mode::ForceMerge] {
        apply_mode(mode);
        let pipeline = compile_read_profiled(&context, query);
        let plan_label = if pipeline_has_multi_iter_merge(&pipeline) { "merge" } else { "sequential" };

        let exec_start = Instant::now();
        let (iterator, exec_ctx) =
            pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
        let row_count = iterator.collect_owned().unwrap().len();
        let wall = exec_start.elapsed();

        println!("---- {label} :: {:<16}  plan: {:<10}  wall: {:>9.2?}  rows: {}",
            mode.label(), plan_label, wall, row_count);
        println!("{}", exec_ctx.profile);
        println!();
    }
    apply_mode(Mode::Natural);
    println!();
}

fn dump_profile_all_scenarios() {
    // Run each scenario at the same scales as the test_planner.rs tests so the
    // counter numbers are directly cite-able in the test docstrings.
    println!("Profile dump for all 8 test scenarios at test-suite scales.");
    println!();

    // === Merge-must-win shapes ===

    dump_profile_for_scenario(
        "merge_wins_symmetric_balanced", "500x500 owners 1:1 full coverage",
        &two_owner_join_query(),
        |ctx| { define_two_owner_schema(ctx); load_data(ctx, build_symmetric_spec(500, 500)); },
    );
    dump_profile_for_scenario(
        "merge_wins_moderate_cartesian", "500x500 owners, 50 distinct (10:1 fan-out)",
        &two_owner_join_query(),
        |ctx| { define_two_owner_schema(ctx); load_data(ctx, build_symmetric_spec(500, 50)); },
    );
    dump_profile_for_scenario(
        "merge_wins_heavy_cartesian", "500x500 owners, 10 distinct (50:1 fan-out)",
        &two_owner_join_query(),
        |ctx| { define_two_owner_schema(ctx); load_data(ctx, build_symmetric_spec(500, 10)); },
    );
    dump_profile_for_scenario(
        "merge_wins_at_scale_with_fanout", "1000x1000 owners, 100 distinct (10:1 fan-out)",
        &two_owner_join_query(),
        |ctx| { define_two_owner_schema(ctx); load_data(ctx, build_symmetric_spec(1_000, 100)); },
    );

    // === Sequential-must-win shapes ===

    dump_profile_for_scenario(
        "sequential_wins_tiny_outer_huge_inner", "1 outer x 2000 inner, full coverage",
        &two_owner_join_query(),
        |ctx| {
            define_two_owner_schema(ctx);
            load_data(ctx, DataSpec {
                instances: vec![
                    InstanceSpec { type_: OWNER_1, count: 1, key: Some(KEY_1) },
                    InstanceSpec { type_: OWNER_2, count: 2_000, key: Some(KEY_2) },
                ],
                has: vec![
                    HasSpec { owner_type: OWNER_1, attr_type: JOIN_ATTR,
                              count_each: 1, count_total: 1,
                              attribute_generator: sequential() },
                    HasSpec { owner_type: OWNER_2, attr_type: JOIN_ATTR,
                              count_each: 1, count_total: 2_000,
                              attribute_generator: sequential() },
                ],
            });
        },
    );
    dump_profile_for_scenario(
        "sequential_wins_subset_coverage", "100 outer (0..99) ⊂ 2000 inner (0..1999)",
        &two_owner_join_query(),
        |ctx| {
            define_two_owner_schema(ctx);
            load_data(ctx, DataSpec {
                instances: vec![
                    InstanceSpec { type_: OWNER_1, count: 100, key: Some(KEY_1) },
                    InstanceSpec { type_: OWNER_2, count: 2_000, key: Some(KEY_2) },
                ],
                has: vec![
                    HasSpec { owner_type: OWNER_1, attr_type: JOIN_ATTR,
                              count_each: 1, count_total: 100,
                              attribute_generator: sequential() },
                    HasSpec { owner_type: OWNER_2, attr_type: JOIN_ATTR,
                              count_each: 1, count_total: 2_000,
                              attribute_generator: sequential() },
                ],
            });
        },
    );
    dump_profile_for_scenario(
        "sequential_wins_noisy_inner", "100+100 query (0..99) + 2000 noise (100..2099)",
        &two_owner_join_query(),
        |ctx| {
            define_two_owner_with_noise_schema(ctx, 2);
            let mut spec = DataSpec {
                instances: vec![
                    InstanceSpec { type_: OWNER_1, count: 100, key: Some(KEY_1) },
                    InstanceSpec { type_: OWNER_2, count: 100, key: Some(KEY_2) },
                ],
                has: vec![
                    HasSpec { owner_type: OWNER_1, attr_type: JOIN_ATTR,
                              count_each: 1, count_total: 100,
                              attribute_generator: sequential() },
                    HasSpec { owner_type: OWNER_2, attr_type: JOIN_ATTR,
                              count_each: 1, count_total: 100,
                              attribute_generator: sequential() },
                ],
            };
            add_noise_owners(&mut spec, 2, 1_000, 100);  // 2 * 1000 = 2000 noise
            load_data(ctx, spec);
        },
    );
    dump_profile_for_scenario(
        "sequential_wins_asymmetric_coverage", "10 outer (0..9) x 2000 inner (5..2004), overlap=5",
        &two_owner_join_query(),
        |ctx| {
            define_two_owner_schema(ctx);
            load_data(ctx, DataSpec {
                instances: vec![
                    InstanceSpec { type_: OWNER_1, count: 10, key: Some(KEY_1) },
                    InstanceSpec { type_: OWNER_2, count: 2_000, key: Some(KEY_2) },
                ],
                has: vec![
                    HasSpec { owner_type: OWNER_1, attr_type: JOIN_ATTR,
                              count_each: 1, count_total: 10,
                              attribute_generator: sequential() },
                    HasSpec { owner_type: OWNER_2, attr_type: JOIN_ATTR,
                              count_each: 1, count_total: 2_000,
                              attribute_generator: offset_unique(5) },
                ],
            });
        },
    );

    // === Multi-attribute filter scenarios (merge-wins shape) =================================
    // K=3, M=3, N=1000 — clean signal, planner naturally picks merge, merge wins ~1.8x.
    dump_profile_for_scenario(
        "merge_wins_multi_attr_K3_M3_N1000", "1000 widgets, 3 attrs × 3 values; query: all-zeros",
        &multi_attr_query(3, 0),
        |ctx| { define_multi_attr_schema(ctx, 3); load_multi_attr_data(ctx, 1_000, 3, 3); },
    );
    // K=10, M=2, N=1000 — extreme arity case to see how counters scale.
    dump_profile_for_scenario(
        "merge_wins_multi_attr_K10_M2_N1000", "1000 widgets, 10 attrs × 2 values; query: all-zeros",
        &multi_attr_query(10, 0),
        |ctx| { define_multi_attr_schema(ctx, 10); load_multi_attr_data(ctx, 1_000, 10, 2); },
    );
}

fn time_iterations(
    context: &Context, query: &str, mode: Mode, iterations: usize,
) -> ModeResult {
    apply_mode(mode);
    // Warmup: page caches, branch predictors.
    let warmup_pipeline = compile_read(context, query);
    let picked_merge = pipeline_has_multi_iter_merge(&warmup_pipeline);
    let warmup_rows = execute_read(warmup_pipeline);

    let start = Instant::now();
    let mut output_rows = warmup_rows;
    for _ in 0..iterations {
        let pipeline = compile_read(context, query);
        output_rows = execute_read(pipeline);
    }
    let total = start.elapsed();
    apply_mode(Mode::Natural);
    ModeResult { mode, picked_merge, output_rows, total }
}

/// Generic scenario runner: caller provides label, shape description (for printing),
/// iteration count, the query to run, and a closure that prepares the Context.
fn run_scenario_with_query(
    label: &'static str,
    shape_label: impl Into<String>,
    iterations: usize,
    query: String,
    setup_data: impl FnOnce(&mut Context),
) -> BenchResult {
    let shape_label = shape_label.into();
    println!("[setup] {label}: {shape_label}...");
    let setup_start = Instant::now();
    let mut context = setup();
    setup_data(&mut context);
    println!("[setup] {label}: data loaded in {:.2?}", setup_start.elapsed());

    let natural = time_iterations(&context, &query, Mode::Natural, iterations);
    let forced_merge = time_iterations(&context, &query, Mode::ForceMerge, iterations);
    let forced_seq = time_iterations(&context, &query, Mode::ForceNoMerge, iterations);

    let result = BenchResult {
        label, shape_label, iterations,
        results: [natural, forced_merge, forced_seq],
    };
    result.print();
    result
}

/// Convenience wrapper for the standard two-owner has-join query.
fn run_scenario(
    label: &'static str,
    shape_label: impl Into<String>,
    iterations: usize,
    setup_data: impl FnOnce(&mut Context),
) -> BenchResult {
    run_scenario_with_query(label, shape_label, iterations, two_owner_join_query(), setup_data)
}

fn main() {
    println!("=== Planner join compare ===");
    println!("Each scenario runs 3 modes:");
    println!("  natural          - planner's natural pick");
    println!("  force_merge      - FORCE_MERGE_INTERSECTION=1 (Cost::join returns 0)");
    println!("  force_sequential - FORCE_NO_MERGE_INTERSECTION=1 (Cost::join returns INFINITY)");
    println!("Times include compile + execute per iteration (compile is small at this scale).");
    println!();

    // Profile dump mode: run each of the 8 test_planner.rs scenarios at test-suite scales
    // with profiling enabled, print full QueryProfile for both forced-sequential and
    // forced-merge plans, then exit. Use to extract per-step seek/advance counters for
    // each scenario.
    if std::env::var("BENCH_PROFILE_DUMP").is_ok() {
        dump_profile_all_scenarios();
        return;
    }

    let mut results = Vec::new();

    // Skip the legacy 2-side scenarios for fast turnaround on the N-way sweep.
    // Set BENCH_SKIP_LEGACY=0 (or unset) to run everything.
    let skip_legacy = std::env::var("BENCH_SKIP_LEGACY").map(|v| v == "1").unwrap_or(false);
    if skip_legacy { println!("(skipping legacy 2-side scenarios; set BENCH_SKIP_LEGACY=0 to enable)\n"); }

    // === Merge-must-win shapes (per test_planner.rs) =========================================

    if !skip_legacy {
    results.push(run_scenario(
        "symmetric_balanced_5000", "5000x5000 owners 1:1 full coverage", 50,
        |ctx| { define_two_owner_schema(ctx); load_data(ctx, build_symmetric_spec(5_000, 5_000)); },
    ));
    println!();

    results.push(run_scenario(
        "moderate_cartesian_2000", "2000x2000 owners, 200 distinct (10:1 fan-out)", 30,
        |ctx| { define_two_owner_schema(ctx); load_data(ctx, build_symmetric_spec(2_000, 200)); },
    ));
    println!();

    results.push(run_scenario(
        "heavy_cartesian_2000", "2000x2000 owners, 40 distinct (50:1 fan-out)", 10,
        |ctx| { define_two_owner_schema(ctx); load_data(ctx, build_symmetric_spec(2_000, 40)); },
    ));
    println!();

    results.push(run_scenario(
        "at_scale_with_fanout_5000", "5000x5000 owners, 500 distinct (10:1 fan-out)", 10,
        |ctx| { define_two_owner_schema(ctx); load_data(ctx, build_symmetric_spec(5_000, 500)); },
    ));
    println!();
    } // end if !skip_legacy

    // === Large-scale variants of merge_wins_* shapes (BENCH_LARGE_SCALE=1) ====================
    // Same shapes as the merge_wins_* scenarios above but at 2-5× scale, to see whether
    // merge's structural O(N+M) catches up to sequential's O(N) at production scale.
    if std::env::var("BENCH_LARGE_SCALE").map(|v| v == "1").unwrap_or(false) {
        results.push(run_scenario(
            "symmetric_balanced_10000", "10000x10000 owners 1:1 full coverage", 20,
            |ctx| { define_two_owner_schema(ctx); load_data(ctx, build_symmetric_spec(10_000, 10_000)); },
        ));
        println!();

        results.push(run_scenario(
            "moderate_cartesian_5000_500", "5000x5000 owners, 500 distinct (10:1 fan-out)", 15,
            |ctx| { define_two_owner_schema(ctx); load_data(ctx, build_symmetric_spec(5_000, 500)); },
        ));
        println!();

        results.push(run_scenario(
            "heavy_cartesian_5000_100", "5000x5000 owners, 100 distinct (50:1 fan-out)", 5,
            |ctx| { define_two_owner_schema(ctx); load_data(ctx, build_symmetric_spec(5_000, 100)); },
        ));
        println!();

        results.push(run_scenario(
            "at_scale_with_fanout_10000", "10000x10000 owners, 1000 distinct (10:1 fan-out)", 5,
            |ctx| { define_two_owner_schema(ctx); load_data(ctx, build_symmetric_spec(10_000, 1_000)); },
        ));
        println!();
    }

    if !skip_legacy {
    // === Sequential-must-win shapes (per test_planner.rs) ====================================

    const N_INNER: usize = 5_000;

    results.push(run_scenario(
        "tiny_outer_huge_inner", "1 outer x 5000 inner, full coverage", 100,
        |ctx| {
            define_two_owner_schema(ctx);
            load_data(ctx, DataSpec {
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
            });
        },
    ));
    println!();

    results.push(run_scenario(
        "subset_coverage", "100 outer (vals 0..99) ⊂ 5000 inner (vals 0..4999)", 30,
        |ctx| {
            define_two_owner_schema(ctx);
            load_data(ctx, DataSpec {
                instances: vec![
                    InstanceSpec { type_: OWNER_1, count: 100, key: Some(KEY_1) },
                    InstanceSpec { type_: OWNER_2, count: N_INNER, key: Some(KEY_2) },
                ],
                has: vec![
                    HasSpec {
                        owner_type: OWNER_1, attr_type: JOIN_ATTR,
                        count_each: 1, count_total: 100,
                        attribute_generator: sequential(),
                    },
                    HasSpec {
                        owner_type: OWNER_2, attr_type: JOIN_ATTR,
                        count_each: 1, count_total: N_INNER,
                        attribute_generator: sequential(),
                    },
                ],
            });
        },
    ));
    println!();

    results.push(run_scenario(
        "noisy_inner", "100+100 query owners (vals 0..99) + 5000 noise (vals 100..5099)", 30,
        |ctx| {
            define_two_owner_with_noise_schema(ctx, 2);
            let mut spec = DataSpec {
                instances: vec![
                    InstanceSpec { type_: OWNER_1, count: 100, key: Some(KEY_1) },
                    InstanceSpec { type_: OWNER_2, count: 100, key: Some(KEY_2) },
                ],
                has: vec![
                    HasSpec {
                        owner_type: OWNER_1, attr_type: JOIN_ATTR,
                        count_each: 1, count_total: 100,
                        attribute_generator: sequential(),
                    },
                    HasSpec {
                        owner_type: OWNER_2, attr_type: JOIN_ATTR,
                        count_each: 1, count_total: 100,
                        attribute_generator: sequential(),
                    },
                ],
            };
            add_noise_owners(&mut spec, 2, 2_500, 100);  // 2 types * 2500 = 5000 noise edges
            load_data(ctx, spec);
        },
    ));
    println!();

    results.push(run_scenario(
        "asymmetric_coverage", "10 outer (vals 0..9) x 5000 inner (vals 5..5004), overlap=5", 100,
        |ctx| {
            define_two_owner_schema(ctx);
            load_data(ctx, DataSpec {
                instances: vec![
                    InstanceSpec { type_: OWNER_1, count: 10, key: Some(KEY_1) },
                    InstanceSpec { type_: OWNER_2, count: N_INNER, key: Some(KEY_2) },
                ],
                has: vec![
                    HasSpec {
                        owner_type: OWNER_1, attr_type: JOIN_ATTR,
                        count_each: 1, count_total: 10,
                        attribute_generator: sequential(),
                    },
                    HasSpec {
                        owner_type: OWNER_2, attr_type: JOIN_ATTR,
                        count_each: 1, count_total: N_INNER,
                        attribute_generator: offset_unique(5),
                    },
                ],
            });
        },
    ));
    println!();
    } // end if !skip_legacy

    // === N-way join sweep ====================================================================
    // Increase the number of sides joined on the same variable while keeping per-side
    // cardinality constant (M=500 each, sequential 0..499). Output stays at 500 rows
    // regardless of N — this isolates "join arity" as the only changing axis.
    //
    // Theoretical asymmetry: sequential plan pays M × (N-1) probe-opens, merge pays N
    // total opens + N × M advances. As N grows, sequential's open cost compounds while
    // merge's stays linear in (N+1)M. So merge's relative position should improve with N.

    // === Multi-attribute filter on single entity (sweep K patterns) =========================
    // Single $x with K `has attr_i 0` patterns; intersection on owner-id is structurally
    // the case where sort-merge has its best chance vs INL/bind-from. Sweep K with N=1000
    // and M=3 (each pattern selects N/M ≈ 333 owners). Expected output = N / M^K rows.
    {
        const MULTI_ATTR_N: usize = 1_000;
        const MULTI_ATTR_M: usize = 3;
        for &k_attrs in &[2usize, 3, 5] {
            let label_owned = format!("multi_attr_K{k_attrs}_M{MULTI_ATTR_M}_N{MULTI_ATTR_N}");
            let label: &'static str = Box::leak(label_owned.into_boxed_str());
            let shape = format!(
                "{MULTI_ATTR_N} widgets, {k_attrs} attrs × {MULTI_ATTR_M} values; \
                 query: all-zeros (output ≈ {MULTI_ATTR_N}/{MULTI_ATTR_M}^{k_attrs} rows)"
            );
            results.push(run_scenario_with_query(
                label, shape, 30,
                multi_attr_query(k_attrs, 0),
                move |ctx| {
                    define_multi_attr_schema(ctx, k_attrs);
                    load_multi_attr_data(ctx, MULTI_ATTR_N, k_attrs, MULTI_ATTR_M);
                },
            ));
            println!();
        }
    }
    // High-K + low-selectivity (M=2 binary attrs): each pattern matches ~half of all owners,
    // intersection tightens by 2× per added pattern. Classical case for sort-merge.
    {
        const MULTI_ATTR_N: usize = 1_000;
        const MULTI_ATTR_M: usize = 2;
        for &k_attrs in &[5usize, 7, 10] {
            let label_owned = format!("multi_attr_K{k_attrs}_M{MULTI_ATTR_M}_N{MULTI_ATTR_N}");
            let label: &'static str = Box::leak(label_owned.into_boxed_str());
            let shape = format!(
                "{MULTI_ATTR_N} widgets, {k_attrs} attrs × {MULTI_ATTR_M} values; \
                 query: all-zeros (output ≈ {MULTI_ATTR_N}/{MULTI_ATTR_M}^{k_attrs} rows)"
            );
            results.push(run_scenario_with_query(
                label, shape, 30,
                multi_attr_query(k_attrs, 0),
                move |ctx| {
                    define_multi_attr_schema(ctx, k_attrs);
                    load_multi_attr_data(ctx, MULTI_ATTR_N, k_attrs, MULTI_ATTR_M);
                },
            ));
            println!();
        }
    }

    const NWAY_OWNERS_PER_SIDE: usize = 500;
    for &n_sides in &[2usize, 3, 4, 5, 6, 7, 8, 10, 12] {
        results.push(run_scenario_with_query(
            // Leak: bench-only, intentional; needed for &'static label.
            Box::leak(format!("nway_join_{n_sides}_sides").into_boxed_str()),
            format!("{n_sides}-side join on $join, {NWAY_OWNERS_PER_SIDE} owners/side, 1:1 full coverage"),
            30,
            nway_join_query(n_sides),
            move |ctx| {
                define_nway_join_schema(ctx, n_sides);
                load_data(ctx, build_nway_spec(n_sides, NWAY_OWNERS_PER_SIDE));
            },
        ));
        println!();
    }

    println!("=== Summary ===");
    for r in &results {
        let iters = r.iterations as u32;
        print!("  {:<40} ", r.label);
        for mr in &r.results {
            let per = mr.total / iters;
            let pick = if mr.picked_merge { "M" } else { "S" };
            print!("{:<10}[{}]={:>8.2?}  ", mr.mode.label(), pick, per);
        }
        println!();
    }
}
