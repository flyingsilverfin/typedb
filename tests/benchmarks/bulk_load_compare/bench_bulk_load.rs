/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

// Single-threaded bulk-load comparison: feed N input rows into the database,
// once via the inputs-stage feature (one parse+compile per tx, N rows batched
// through a typed inputs declaration) and once via a disjunction of `let`
// branches (N branches in a single match stage, parsed/compiled fresh for every
// tx). Both variants persist the same {name, age} tuples through the same
// schema, so the wall-clock difference is the cost shape difference between
// the two query forms.

use std::{env, fmt::Write as _, sync::Arc, time::Instant};

use database::{
    database_manager::DatabaseManager,
    query::{execute_schema_query, execute_write_query_in_write},
    transaction::{TransactionRead, TransactionSchema, TransactionWrite},
    Database,
};
use executor::{pipeline::stage::StageIterator, ExecutionInterrupt};
use options::{QueryOptions, TransactionOptions};
use query::query_manager::PipelinePayload;
use storage::durability_client::WALClient;
use test_utils::{create_tmp_dir, TempDir};

const DB_NAME: &str = "bench-bulk-load";

const SCHEMA: &str = r#"define
    attribute name value string;
    attribute age value integer;
    entity person owns name, owns age @card(0..);
"#;

// Inputs-stage form: one parse+compile per transaction, N rows fed through the
// typed inputs declaration. This is the "krishnan/add-inputs-stage" path that
// the optimize-contention branch is built on.
const INPUTS_INSERT_QUERY: &str =
    r#"inputs $n: string, $a: integer; insert $p isa person, has name == $n, has age == $a;"#;

const DEFAULT_TOTAL_OPS: usize = 200_000;
const DEFAULT_BATCH_SIZE: usize = 1_000;

fn total_ops() -> usize {
    env::var("BENCH_TOTAL_OPS").ok().and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_TOTAL_OPS)
}

fn batch_size() -> usize {
    env::var("BENCH_BATCH_SIZE").ok().and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_BATCH_SIZE)
}

// --- Database setup ---

fn create_database() -> (TempDir, Arc<Database<WALClient>>) {
    let tmp_dir = create_tmp_dir();
    let dbm = DatabaseManager::new(&tmp_dir).unwrap();
    dbm.put_database(DB_NAME).unwrap();
    let database = dbm.database(DB_NAME).unwrap();

    let schema_query = typeql::parse_query(SCHEMA).unwrap().into_structure().into_schema();
    let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
    let (tx, result) = execute_schema_query(tx, schema_query, SCHEMA.to_string());
    result.unwrap();
    tx.commit().1.unwrap();

    (tmp_dir, database)
}

fn count_persons(database: &Arc<Database<WALClient>>) -> usize {
    let tx = TransactionRead::open(database.clone(), TransactionOptions::default()).unwrap();
    let TransactionRead { snapshot, query_manager, type_manager, thing_manager, function_manager, .. } = &tx;
    let query_str = "match $p isa person;";
    let parsed = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let pipeline = query_manager
        .prepare_read_pipeline(
            snapshot.clone(),
            type_manager,
            thing_manager.clone(),
            function_manager,
            PipelinePayload::from(parsed),
            query_str,
        )
        .unwrap();
    let (rows, _ctx) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    rows.collect_owned().unwrap().len()
}

// --- Variant A: inputs-stage ---

fn run_inputs_stage(database: &Arc<Database<WALClient>>, total_ops: usize, batch: usize) -> PhaseTimings {
    let mut timings = PhaseTimings::default();
    let txns = total_ops / batch;
    for batch_id in 0..txns {
        let t0 = Instant::now();
        let tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
        let t_open = Instant::now();

        let inputs: Vec<Vec<Option<String>>> = (0..batch)
            .map(|i| {
                let id = batch_id * batch + i;
                let age = (id % 100) as i64;
                vec![Some(quoted_string_literal(&format!("person_{id}"))), Some(age.to_string())]
            })
            .collect();
        let t_inputs = Instant::now();

        let parsed = typeql::parse_query(INPUTS_INSERT_QUERY).unwrap().into_structure().into_pipeline();
        let payload = PipelinePayload { parsed, inputs: Some(inputs) };
        let (tx, result) = execute_write_query_in_write(
            tx,
            QueryOptions::default_grpc(),
            payload,
            INPUTS_INSERT_QUERY.to_string(),
            ExecutionInterrupt::new_uninterruptible(),
        );
        result.unwrap();
        let t_exec = Instant::now();

        tx.commit().1.unwrap();
        let t_commit = Instant::now();

        timings.open_ns += (t_open - t0).as_nanos() as u64;
        timings.input_build_ns += (t_inputs - t_open).as_nanos() as u64;
        timings.parse_compile_exec_ns += (t_exec - t_inputs).as_nanos() as u64;
        timings.commit_ns += (t_commit - t_exec).as_nanos() as u64;
        timings.txns += 1;
    }
    timings
}

// --- Variant B: disjunction of `let` ---

// `let $name = "..."; let $age = N;` inside `{ ... } or { ... } or ...` — every
// row is a separate disjunction branch in a single match stage, then a single
// insert stage consumes the bound values. This pays full parse+compile cost
// per transaction (the query string grows linearly with the batch), which is
// what we want to compare against the inputs-stage path.
fn build_disjunction_query(batch_id: usize, batch: usize) -> String {
    let mut q = String::with_capacity(batch * 64);
    q.push_str("match\n");
    for i in 0..batch {
        let id = batch_id * batch + i;
        let age = (id % 100) as i64;
        if i > 0 {
            q.push_str(" or\n");
        }
        write!(&mut q, "  {{ let $name = \"person_{id}\"; let $age = {age}; }}").unwrap();
    }
    q.push_str(";\ninsert $p isa person, has name == $name, has age == $age;\n");
    q
}

fn run_disjunction_of_let(database: &Arc<Database<WALClient>>, total_ops: usize, batch: usize) -> PhaseTimings {
    let mut timings = PhaseTimings::default();
    let txns = total_ops / batch;
    for batch_id in 0..txns {
        let t0 = Instant::now();
        let tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
        let t_open = Instant::now();

        let query_str = build_disjunction_query(batch_id, batch);
        let t_inputs = Instant::now();

        let parsed = typeql::parse_query(&query_str).unwrap().into_structure().into_pipeline();
        let payload = PipelinePayload::from(parsed);
        let (tx, result) = execute_write_query_in_write(
            tx,
            QueryOptions::default_grpc(),
            payload,
            query_str,
            ExecutionInterrupt::new_uninterruptible(),
        );
        result.unwrap();
        let t_exec = Instant::now();

        tx.commit().1.unwrap();
        let t_commit = Instant::now();

        timings.open_ns += (t_open - t0).as_nanos() as u64;
        timings.input_build_ns += (t_inputs - t_open).as_nanos() as u64;
        timings.parse_compile_exec_ns += (t_exec - t_inputs).as_nanos() as u64;
        timings.commit_ns += (t_commit - t_exec).as_nanos() as u64;
        timings.txns += 1;
    }
    timings
}

fn quoted_string_literal(value: &str) -> String {
    // Inputs values are parsed server-side via `typeql::parse_value`; the
    // generated names here never contain quotes or backslashes so we skip
    // escaping.
    format!(r#""{value}""#)
}

#[derive(Default)]
struct PhaseTimings {
    txns: u64,
    open_ns: u64,
    input_build_ns: u64,
    parse_compile_exec_ns: u64,
    commit_ns: u64,
}

impl PhaseTimings {
    fn total_ns(&self) -> u64 {
        self.open_ns + self.input_build_ns + self.parse_compile_exec_ns + self.commit_ns
    }
}

fn report(label: &str, total_ops: usize, batch: usize, t: &PhaseTimings, wall_ns: u64) {
    let wall_ms = wall_ns as f64 / 1e6;
    let ops_per_sec = (total_ops as f64) / (wall_ns as f64 / 1e9);
    eprintln!();
    eprintln!("=== {label} | total_ops={total_ops} batch={batch} threads=1 ===");
    eprintln!(
        "  wall:   {wall_ms:>10.1} ms   ({ops_per_sec:>9.0} ops/s, {txns} txns)",
        txns = t.txns
    );
    let breakdown_total = t.total_ns().max(1) as f64;
    let pct = |ns: u64| (ns as f64 / breakdown_total) * 100.0;
    eprintln!(
        "  open:   {:>10.1} ms ({:>5.1}%)",
        t.open_ns as f64 / 1e6,
        pct(t.open_ns)
    );
    eprintln!(
        "  build:  {:>10.1} ms ({:>5.1}%)   <- input rows / disjunction string",
        t.input_build_ns as f64 / 1e6,
        pct(t.input_build_ns)
    );
    eprintln!(
        "  exec:   {:>10.1} ms ({:>5.1}%)   <- parse + compile + execute",
        t.parse_compile_exec_ns as f64 / 1e6,
        pct(t.parse_compile_exec_ns)
    );
    eprintln!(
        "  commit: {:>10.1} ms ({:>5.1}%)",
        t.commit_ns as f64 / 1e6,
        pct(t.commit_ns)
    );
}

fn run_one(label: &str, runner: fn(&Arc<Database<WALClient>>, usize, usize) -> PhaseTimings) {
    let total = total_ops();
    let batch = batch_size();
    let (_tmp_dir, database) = create_database();
    let start = Instant::now();
    let timings = runner(&database, total, batch);
    let wall_ns = start.elapsed().as_nanos() as u64;
    report(label, total, batch, &timings, wall_ns);
    let count = count_persons(&database);
    let expected = (total / batch) * batch;
    assert_eq!(
        count, expected,
        "{label}: expected {expected} persons inserted, found {count}"
    );
}

fn main() {
    init_tracing();
    eprintln!("Bulk-load comparison: inputs-stage vs disjunction-of-let");
    eprintln!("========================================================");
    eprintln!("total_ops = {}, batch = {}", total_ops(), batch_size());

    run_one("inputs-stage", run_inputs_stage);
    run_one("disjunction-of-let", run_disjunction_of_let);
}

// Set BENCH_PROFILE=1 to enable per-query profiling. The query layer gates
// `QueryProfile` measurement on `tracing::enabled!(Level::TRACE)` in the `query`
// module, and dumps the profile at INFO from `database::query`. We translate
// the env knob into the right filter so callers don't have to know that.
//
// Profile output is noisy: ~200 INFO-level dumps per variant plus a TRACE-
// level "Running write query: ..." event that includes the full query string
// (50 KiB for the 1000-branch disjunction). Pipe stderr to a file or grep for
// "Write query done" if you only want the profile.
fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let bench_profile = env::var("BENCH_PROFILE").is_ok();
    let filter = match env::var("RUST_LOG") {
        Ok(v) if !v.is_empty() => EnvFilter::new(v),
        _ if bench_profile => EnvFilter::new("query=trace,database=info,executor=info"),
        _ => return,
    };
    fmt().with_env_filter(filter).with_writer(std::io::stderr).with_target(true).init();
}
