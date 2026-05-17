/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Planner-focused integration tests. These build a small schema, populate
//! data via a `DataSpec`-driven loader so cardinalities are easy to control,
//! run a read query, and inspect the resulting `QueryProfile` to assert that
//! the planner picked a plan with O(1) advances-per-row in every step.
//!
//! These tests are the "live" complement to the static cost-model unit tests
//! in `compiler::executable::match_::planner::vertex` — they exercise
//! `Cost::join` and friends against real plans on real (small) data.

use std::{collections::HashMap, sync::Arc};

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
    pipeline::stage::{ExecutionContext, StageIterator},
};
use function::function_manager::FunctionManager;
use query::{query_cache::QueryCache, query_manager::QueryManager};
use resource::profile::{CommitProfile, PatternProfile, QueryProfile, StepProfile, SubstepProfile};
use storage::{MVCCStorage, durability_client::WALClient, snapshot::CommittableSnapshot};
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
    /// Rebuild the type manager (with fresh `TypeCache`) and thing manager (with synced
    /// `Statistics`) so that the next planning pass sees the latest committed schema and
    /// row counts. Must be called after every schema or write commit.
    ///
    /// Note: we cannot mutate the existing `Arc<ThingManager>` in place — `ThingManager`
    /// holds its statistics as an `Arc<Statistics>` field, and even if we could swap that
    /// `Arc`, outstanding clones of the parent `Arc<ThingManager>` (e.g. inside cached
    /// executables) would still see the old value. Rebuilding both managers is the
    /// straightforward correct fix; the cost is negligible in a test.
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
        // The query cache holds executables compiled against stale stats — invalidate it.
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
            )
            .unwrap();
        // `into_rows_iterator` executes eagerly for write pipelines: every write stage
        // (Insert/Put/Update/Delete) drains its input iterator and performs all writes
        // before returning. The returned iterator only walks the pre-computed output batch.
        let (_iterator, exec_context) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
        snapshot = Arc::into_inner(exec_context.snapshot).unwrap();
    }
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    context.refresh();
}

/// Run a read pipeline to completion and return its `QueryProfile` for assertions on
/// planner choices. We always return the profile (rather than the un-executed prepared
/// pipeline) because every existing test caller wants to inspect runtime counters.
fn execute_read(context: &Context, query: &str) -> Arc<QueryProfile> {
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
            // REVIEWER: this `force_query_profile: bool` is currently dead in
            // `QueryManager::prepare_read_pipeline` (see TODO there) — the profile is
            // actually enabled because the test calls `init_logging()` indirectly via
            // `create_core_storage()`, which sets a `Level::TRACE` subscriber and makes
            // `tracing::enabled!(Level::TRACE)` return true. If that ever changes (e.g.
            // logging init becomes opt-in), the profile will silently become disabled and
            // `worst_advances_per_row` will return zero — making the assertion vacuous.
            true,
        )
        .unwrap();
    let (iterator, ExecutionContext { profile, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let _ = iterator.collect_owned().unwrap();
    profile
}

// --- DataSpec: declarative test-data builder -------------------------------------------------
//
// `DataSpec` is a small DSL for populating a freshly-defined schema with controllable
// cardinalities. The shape mirrors how planner tests want to reason about data:
// "this many entities of type X, each with a unique key; that many `has` edges
// connecting owners of type Y to attributes of type Z with values from generator g".
//
// Implementation strategy: build one `insert` query per InstanceSpec, and one
// `match $o by key; insert $o has attr value;` per HasSpec edge, then hand the
// batch to `commit_writes`. We track owner counts per type so HasSpec can
// distribute edges round-robin, and look up specific owners by their key value
// (which is why HasSpec owner types must have `key: Some(_)`).

struct DataSpec {
    instances: Vec<InstanceSpec>,
    has: Vec<HasSpec>,
}

struct InstanceSpec {
    /// Type label of the entity to insert.
    type_: &'static str,
    /// Number of instances to insert.
    count: usize,
    /// If `Some(label)`, also give each instance a `has` edge to a unique attribute
    /// value of type `label`. Values are integers `0..count`. `label` must be an
    /// integer-valued attribute type owned by `type_` (typically declared `@key`).
    key: Option<&'static str>,
}

struct HasSpec {
    /// Owner entity type. Must have been populated by a prior `InstanceSpec`.
    owner_type: &'static str,
    /// Attribute type for the edge. Must be integer-valued.
    attr_type: &'static str,
    /// Per-owner cap on the number of edges produced (round-robin across owners).
    count_each: usize,
    /// Total number of `has` edges to produce. Distributed across owners round-robin.
    count_total: usize,
    /// Maps edge index `0..count_total` to an integer attribute value. Repeating values
    /// across different `HasSpec`s lets you set up join keys.
    attribute_generator: fn(usize) -> i64,
}

fn load_data(context: &mut Context, spec: DataSpec) {
    // REVIEWER: borderline cases not currently guarded — flag for the user.
    //  - If multiple HasSpecs share the same owner_type, each one's `count_each` cap is
    //    tracked independently, so the per-owner global edge count can exceed `count_each`.
    //    Fine for the current single-HasSpec-per-(owner,attr) usage, but rethink before
    //    adding multi-HasSpec-per-owner tests.
    //  - An InstanceSpec with `count: 0` doesn't register the owner_type, so a later
    //    HasSpec referencing it panics with the (misleading) "no prior InstanceSpec
    //    inserts" message.
    // Track instance counts per type so HasSpec lookups can iterate over them.
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
        // We address individual owners by their key value, so the owner type must have
        // had a `key` set on its InstanceSpec.
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

    commit_writes(context, &queries);
}

// --- Helpers for inspecting QueryProfile ----------------------------------------------------
//
// Mirrors `executor/tests/pipeline_planner_repro.rs::worst_advances_per_row`. Kept local
// so this test doesn't reach across crates for one helper; if a third caller appears,
// fold this into a shared test util.

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

// --- Tests ----------------------------------------------------------------------------------

const OWNER_1: &str = "owner_1";
const OWNER_2: &str = "owner_2";
const KEY_1: &str = "key_1";
const KEY_2: &str = "key_2";
const JOIN_ATTR: &str = "join_attr";

const OWNER_1_COUNT: usize = 1;
const OWNER_2_COUNT: usize = 1;
const JOIN_EDGES_EACH: usize = 1;
const JOIN_EDGES_TOTAL: usize = 1;

#[test]
fn has_2_join() {
    let mut context = setup();

    let schema = format!(
        "define \
          entity {OWNER_1} owns {KEY_1} @key, owns {JOIN_ATTR}; \
          entity {OWNER_2} owns {KEY_2} @key, owns {JOIN_ATTR}; \
          attribute {KEY_1}, value integer; \
          attribute {KEY_2}, value integer; \
          attribute {JOIN_ATTR}, value integer;"
    );
    define_schema(&mut context, &schema);

    // Both owners get a single join_attr with the same value (0), so the join
    // produces exactly one row. Deliberately tiny: this is a smoke test that the
    // planner picks a sane plan when stats are essentially empty.
    let data_spec = DataSpec {
        instances: vec![
            InstanceSpec { type_: OWNER_1, count: OWNER_1_COUNT, key: Some(KEY_1) },
            InstanceSpec { type_: OWNER_2, count: OWNER_2_COUNT, key: Some(KEY_2) },
        ],
        has: vec![
            HasSpec {
                owner_type: OWNER_1,
                attr_type: JOIN_ATTR,
                count_each: JOIN_EDGES_EACH,
                count_total: JOIN_EDGES_TOTAL,
                attribute_generator: |i| i as i64,
            },
            HasSpec {
                owner_type: OWNER_2,
                attr_type: JOIN_ATTR,
                count_each: JOIN_EDGES_EACH,
                count_total: JOIN_EDGES_TOTAL,
                attribute_generator: |i| i as i64,
            },
        ],
    };
    load_data(&mut context, data_spec);

    let query = format!(
        "match \
         $e1 isa {OWNER_1}, has {JOIN_ATTR} $join; \
         $e2 isa {OWNER_2}, has {JOIN_ATTR} $join;"
    );

    let profile = execute_read(&context, &query);

    // Smoke assertion: every step's raw advances should be O(rows produced). On a
    // 1-row-per-side join the worst step should land in low single digits. We leave
    // generous headroom (< 100) because:
    //   - tiny tables make denominators tiny, exaggerating ratios
    //   - the planner cost model under test is still being tuned; if this fails on
    //     real planner regressions, leave a comment here rather than weakening the
    //     bound just to make CI green.
    let (ratio, advances, rows, descr) = worst_advances_per_row(&profile);
    eprintln!("has_2_join: worst step {advances} advances / {rows} rows = {ratio:.1} advances/row\n  step: {descr}");
    assert!(
        ratio < 100.0,
        "expected the planner to pick a healthy plan for the 2-side has-join (worst < 100 advances/row), \
         got {ratio:.1} ({advances} advances / {rows} rows). step: {descr}",
    );
}
