/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Reproduction for the merge-intersection cost-underestimation bug. See
//! `merge_intersection_planner_handoff.md` for the analysis.
//!
//! The bug: on FK-style joins where one merge side is bound-driven (single value per outer row)
//! and the other is an unbounded scan, the planner picks a sort-merge intersection whose actual
//! cost is dominated by "prove no match exists" linear scans on outer rows that don't join.
//! VARIANT A (single match) reproduces the bad plan; VARIANT B (manually pipelined) is the same
//! query split into stages so each stage's planning has a bound input from the previous stage,
//! which forces the planner into the cheap sequential plan.
//!
//! The assertion uses the runtime `QueryProfile` to detect the perf cliff: if any single step
//! does >> rows-produced raw advances, the merge intersection picked the bad side.

use std::sync::Arc;

use concept::{
    thing::{statistics::Statistics, thing_manager::ThingManager},
    type_::type_manager::TypeManager,
};
use durability::DurabilitySequenceNumber;
use encoding::graph::{
    definition::definition_key_generator::DefinitionKeyGenerator, thing::vertex_generator::ThingVertexGenerator,
    type_::vertex_generator::TypeVertexGenerator,
};
use executor::{
    ExecutionInterrupt,
    pipeline::stage::{ExecutionContext, StageIterator},
};
use function::function_manager::FunctionManager;
use lending_iterator::LendingIterator;
use query::{query_cache::QueryCache, query_manager::QueryManager};
use resource::profile::{CommitProfile, PatternProfile, QueryProfile, StepProfile, SubstepProfile};
use storage::{MVCCStorage, durability_client::WALClient, snapshot::CommittableSnapshot};
use test_utils::TempDir;
use test_utils_concept::setup_concept_storage;
use test_utils_encoding::create_core_storage;

struct Context {
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: Arc<TypeManager>,
    thing_manager: Arc<ThingManager>,
    function_manager: FunctionManager,
    query_manager: QueryManager,
    _tmp_dir: TempDir,
}

/// `test_utils_concept::load_managers` synchronises a local `Statistics` but constructs the
/// `ThingManager` with a fresh empty one — the synced stats are dropped on the floor. The planner
/// then sees zero counts and clamps every constraint to `MIN_SCAN_SIZE`, beam-searching blind.
/// This helper actually wires the synced statistics into the manager so the planner sees real
/// cardinalities. Should be folded back into `test_utils_concept::load_managers` separately.
fn load_managers_with_stats(storage: Arc<MVCCStorage<WALClient>>) -> (Arc<TypeManager>, Arc<ThingManager>) {
    let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
    let mut statistics = Statistics::new(DurabilitySequenceNumber::MIN);
    statistics.may_synchronise(storage.as_ref()).unwrap();
    let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
    let thing_vertex_generator = Arc::new(ThingVertexGenerator::load(storage.clone()).unwrap());
    let type_manager = Arc::new(TypeManager::new(definition_key_generator, type_vertex_generator, None));
    let thing_manager = Arc::new(ThingManager::new(thing_vertex_generator, type_manager.clone(), Arc::new(statistics)));
    (type_manager, thing_manager)
}

fn setup_schema(schema: &str) -> Context {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers_with_stats(storage.clone());
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new(None);
    let mut snapshot = storage.clone().open_snapshot_schema();
    let define = typeql::parse_query(schema).unwrap().into_structure().into_schema();
    query_manager
        .execute_schema(&mut snapshot, &type_manager, &thing_manager, &function_manager, define, schema)
        .unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();

    let (type_manager, thing_manager) = load_managers_with_stats(storage.clone());
    let query_manager = QueryManager::new(Some(Arc::new(QueryCache::new())));
    Context { _tmp_dir, storage, type_manager, function_manager, query_manager, thing_manager }
}

/// Re-derive type and thing managers so that `thing_manager.statistics()` reflects the latest
/// committed data writes. Must be called after `populate(...)` and before any read query whose
/// plan depends on cardinalities.
fn refresh_after_writes(context: &mut Context) {
    let (type_manager, thing_manager) = load_managers_with_stats(context.storage.clone());
    context.type_manager = type_manager;
    context.thing_manager = thing_manager;
    // Reset query cache - the cached executable was compiled against stale stats.
    context.query_manager = QueryManager::new(Some(Arc::new(QueryCache::new())));
}

fn run_write(context: &Context, query_str: &str) {
    let snapshot = context.storage.clone().open_snapshot_write();
    let query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &query,
            query_str,
        )
        .unwrap();
    let (iterator, ExecutionContext { snapshot, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let _ = iterator.count();
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
}

/// Run a read query, returning the row count and the `QueryProfile` for assertion / display.
fn run_read(context: &Context, query_str: &str) -> (usize, Arc<QueryProfile>) {
    let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
    let query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let pipeline = context
        .query_manager
        .prepare_read_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &query,
            query_str,
            false,
        )
        .unwrap();
    let (iterator, ExecutionContext { profile, .. }) =
        pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let count = iterator.collect_owned().unwrap().len();
    (count, profile)
}

/// Inspect a `QueryProfile` and return the worst step's (advances, rows, description) ratio.
/// A healthy plan has roughly O(1) advances per produced row in every step. The merge-intersection
/// bug shows up as a single step with hundreds of advances per row (because the unbounded side
/// has to linear-scan to end-of-range to prove no match exists for unmatched outer rows).
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

fn schema() -> &'static str {
    // Mirrors a SQL-style schema: each "table" is an entity with attribute-FKs.
    // - inv_id is shared between invoice_view and invoice_line (FK)
    // - oln_id is shared between invoice_line and order_line (FK)
    // - ord_id is shared between invoice_line and order_attribute_value (FK)
    // - ov / ac are joined by value equality across order_attribute_value and attribute_list
    r#"
    define
        attribute inv_id value integer;
        attribute inv_date value integer;
        attribute oln_id value integer;
        attribute ord_id value integer;
        attribute ltp_id value integer;
        attribute atb_id value integer;
        attribute invl_extended_amount value double;
        attribute ordav_value value string;
        attribute atbl_code value string;
        attribute atbl_description value string;

        entity invoice_view
            owns inv_id @card(0..),
            owns inv_date @card(0..);

        entity invoice_line
            owns inv_id @card(0..),
            owns invl_extended_amount @card(0..),
            owns oln_id @card(0..),
            owns ord_id @card(0..);

        entity order_line
            owns oln_id @card(0..),
            owns ltp_id @card(0..);

        entity order_attribute_value
            owns ord_id @card(0..),
            owns ordav_value @card(0..),
            owns atb_id @card(0..);

        entity attribute_list
            owns atbl_code @card(0..),
            owns atbl_description @card(0..);
    "#
}

fn populate(context: &Context) {
    // Sizes chosen to:
    // - Keep test fast (~few seconds for inserts)
    // - Make planning decisions actually depend on bound-vs-unbound: each FK fans out ~10x
    // - Trigger the perf cliff: 75% of invoice_lines have an oln_id with no matching order_line
    const N_INVOICES: usize = 200;
    const N_LINES_PER_INVOICE: usize = 10;
    const N_ORDER_LINES: usize = 500;
    const N_ORDER_ATTR_VALUES: usize = 500;
    const N_ATTR_LIST: usize = 50;

    // 1) invoice_view: inv_id ∈ [0, N_INVOICES), inv_date in two clusters (some in target window).
    let mut q = String::from("insert\n");
    for i in 0..N_INVOICES {
        let date = if i % 4 == 0 { 20_260_115 } else { 20_250_101 }; // ~25% in window
        q.push_str(&format!("$v{i} isa invoice_view, has inv_id {i}, has inv_date {date};\n"));
    }
    run_write(context, &q);

    // 2) invoice_line: each invoice has N_LINES_PER_INVOICE lines, each line has unique oln_id.
    for chunk_start in (0..N_INVOICES).step_by(20) {
        let mut q = String::from("insert\n");
        for i in chunk_start..(chunk_start + 20).min(N_INVOICES) {
            for k in 0..N_LINES_PER_INVOICE {
                let oln = i * N_LINES_PER_INVOICE + k;
                let ord = (i * N_LINES_PER_INVOICE + k) % N_ORDER_LINES;
                q.push_str(&format!(
                    "$l{i}_{k} isa invoice_line, has inv_id {i}, has invl_extended_amount {amt}, has oln_id {oln}, has ord_id {ord};\n",
                    amt = (i + k) as f64,
                ));
            }
        }
        run_write(context, &q);
    }

    // 3) order_line: oln_id ∈ [0, N_ORDER_LINES), ltp_id varies.
    let mut q = String::from("insert\n");
    for i in 0..N_ORDER_LINES {
        let ltp = if i % 5 == 0 { 9 } else { (i % 7) as i64 + 1 }; // 1/5 are ltp_id=9 (filtered out)
        q.push_str(&format!("$ol{i} isa order_line, has oln_id {i}, has ltp_id {ltp};\n"));
    }
    run_write(context, &q);

    // 4) order_attribute_value: each order has multiple attributes. atb_id=74 is the special one (~1/4).
    let mut q = String::from("insert\n");
    for i in 0..N_ORDER_ATTR_VALUES {
        let atb = if i % 4 == 0 { 74 } else { (i % 10) as i64 + 1 };
        let ord = i % N_ORDER_LINES;
        q.push_str(&format!(
            "$av{i} isa order_attribute_value, has ord_id {ord}, has ordav_value 'V{val}', has atb_id {atb};\n",
            val = i % N_ATTR_LIST,
        ));
    }
    run_write(context, &q);

    // 5) attribute_list: atbl_code values match ordav_value strings.
    let mut q = String::from("insert\n");
    for i in 0..N_ATTR_LIST {
        q.push_str(&format!("$al{i} isa attribute_list, has atbl_code 'V{i}', has atbl_description 'desc-{i}';\n"));
    }
    run_write(context, &q);
}

fn run_with_dump(context: &Context, label: &str, query_str: &str) -> (usize, Arc<QueryProfile>) {
    eprintln!("\n========== {label} ==========\n{query_str}");
    let start = std::time::Instant::now();
    let (count, profile) = run_read(context, query_str);
    let elapsed = start.elapsed();
    eprintln!("\n--- QueryProfile ({label}) ---\n{profile}");
    let (ratio, advances, rows, descr) = worst_advances_per_row(&profile);
    eprintln!("[{label}] worst step: {advances} advances / {rows} rows = {ratio:.1} advances/row\n  step: {descr}");
    eprintln!("[{label}] rows: {count}  elapsed: {elapsed:?}");
    (count, profile)
}

#[test]
fn pipeline_attribute_fk_join_avoids_lopsided_merge_intersection() {
    let mut context = setup_schema(schema());
    populate(&context);
    refresh_after_writes(&mut context);

    // VARIANT A: single match clause — the planner has the most freedom, picks the bad merge.
    let single_match = r#"
        match
        $v isa invoice_view, has inv_id $invid, has inv_date $date;
        $date >= 20260101;
        $date <= 20260131;
        $l isa invoice_line,
            has inv_id $invid,
            has invl_extended_amount $amt,
            has oln_id $olnid,
            has ord_id $oid;
        $oln isa order_line, has oln_id $olnid, has ltp_id $ltpid;
        $ltpid != 9;
        $ordav isa order_attribute_value,
            has ord_id $oid,
            has ordav_value $ov,
            has atb_id 74;
        $atbl isa attribute_list, has atbl_description $sales_rep, has atbl_code $ac;
        $ov == $ac;
    "#;

    // VARIANT B: pipelined — same query, manually split so each stage starts FK-bound.
    let pipelined = r#"
        match
        $v isa invoice_view, has inv_id $invid, has inv_date $date;
        $date >= 20260101;
        $date <= 20260131;
        match
        $l isa invoice_line, has inv_id $invid;
        match
        $l has invl_extended_amount $amt, has oln_id $olnid, has ord_id $oid;
        match
        $oln isa order_line, has oln_id $olnid, has ltp_id $ltpid;
        $ltpid != 9;
        match
        $ordav isa order_attribute_value, has ord_id $oid;
        match
        $ordav has ordav_value $ov, has atb_id 74;
        match
        $atbl isa attribute_list, has atbl_code $ac;
        $ov == $ac;
        match
        $atbl has atbl_description $sales_rep;
    "#;

    let (n_a, profile_a) = run_with_dump(&context, "VARIANT A: single match", single_match);
    let (n_b, profile_b) = run_with_dump(&context, "VARIANT B: pipelined", pipelined);

    assert_eq!(n_a, n_b, "single-match and pipelined variants returned different counts!");

    let (ratio_a, advances_a, rows_a, descr_a) = worst_advances_per_row(&profile_a);
    let (ratio_b, advances_b, rows_b, descr_b) = worst_advances_per_row(&profile_b);

    // Healthy plan signature: every step's raw advances are O(rows_produced). Before the
    // failed-probe cost term landed in Cost::join, VARIANT A picked a lopsided merge
    // intersection whose worst step did ~2300 advances/row; VARIANT B (manually pipelined)
    // did ~4 advances/row. With the fix, both should be in the low single/double digits.
    eprintln!("\nVerdict: VARIANT A worst {ratio_a:.1} advances/row vs VARIANT B worst {ratio_b:.1} advances/row");
    assert!(
        ratio_a < 100.0,
        "expected the planner to avoid the lopsided merge-intersection plan (worst < 100 advances/row), \
         got {ratio_a:.1} ({advances_a} advances / {rows_a} rows). step: {descr_a}",
    );
    assert!(
        ratio_b < 100.0,
        "expected the pipelined variant to remain healthy (worst < 100 advances/row), \
         got {ratio_b:.1} ({advances_b} advances / {rows_b} rows). step: {descr_b}",
    );
}
