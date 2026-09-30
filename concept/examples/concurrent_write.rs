/*
 * Concurrent write-path benchmark for the concept layer. N threads each run `commits` write snapshots of `batch`
 * items through the unchanged ThingManager and the NORMAL commit path (finalise, commit record, WAL append + fsync
 * wait, isolation validation, RocksDB write, status record), on a fresh storage per thread count.
 *
 * Modes:
 *   entities  - each item is an entity with a @key string attribute and an integer attribute (2 ownerships)
 *   relations - each item is an indexed binary relation between two random PERSISTED entities, which are
 *               pre-created in parallel first; `plays` is left unbounded so no exclusive plays locks are taken
 *
 * Prints aggregate throughput, the thread-average per-item cost of ops / finalise / commit, every commit stage from
 * CommitProfile, and storage size. Optional: a pprof CPU profile of the load phase, and the server's 50ms statistics
 * updater loop (database.rs make_update_statistics_fn) with its busy time and lag.
 *
 * Build (release; RocksDB compiles from source unless ROCKSDB_LIB_DIR points at a prebuilt lib):
 *   RUSTFLAGS="-C link-arg=-lstdc++" cargo build --release -p concept --example concurrent_write
 *
 * Run:
 *   concurrent_write <entities|relations> <batch> <commits-per-thread> <thread-counts: 1,4,8,10>
 *                    [persisted-entities=500000] [rocks-cache-mb=0 (test default 64MB)] [profile-hz=0] [stats-updater=0|1]
 *
 * Examples (relations, 10 threads, 4GB cache, no profile, no updater):
 *   concurrent_write relations 10000 10  10 500000   4096 0 0      # 1M relations over 500k entities
 *   concurrent_write relations 10000 800 10 40000000 4096 0 0      # 80M relations over 40M entities (~13GB RSS, ~25GB disk)
 *   concurrent_write entities  10000 200 10 0        4096 200 0    # 20M entities, 200Hz CPU profile
 *
 * Temp storage goes to $TMPDIR; at 80M relations expect ~25GB there (storage + WAL) until the process exits.
 */

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Barrier},
    thread,
    time::Instant,
};

use concept::{
    thing::{
        object::{Object, ObjectAPI},
        statistics::Statistics,
        thing_manager::ThingManager,
    },
    type_::{
        Ordering, OwnerAPI, PlayerAPI,
        annotation::{AnnotationCardinality, AnnotationKey},
        owns::OwnsAnnotation,
        relates::RelatesAnnotation,
        type_manager::{TypeManager, type_cache::TypeCache},
    },
};
use diagnostics::metrics::FsyncMetrics;
use durability::{DurabilitySequenceNumber, wal::WAL};
use encoding::{
    EncodingKeyspace,
    graph::{
        definition::definition_key_generator::DefinitionKeyGenerator, thing::vertex_generator::ThingVertexGenerator,
        type_::vertex_generator::TypeVertexGenerator,
    },
    value::{label::Label, value::Value, value_type::ValueType},
};
use options::byte_size::ByteSize;
use resource::profile::{CommitProfile, StorageCounters};
use storage::{
    MVCCStorage,
    durability_client::WALClient,
    keyspace::rocks_resources::RocksResources,
    snapshot::{CommittableSnapshot, SchemaSnapshot, WriteSnapshot},
};
use test_utils::create_tmp_storage_dir;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

fn xorshift(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

fn create_schema(storage: Arc<MVCCStorage<WALClient>>) {
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let mut snapshot: SchemaSnapshot<WALClient> = storage.clone().open_snapshot_schema();
    let name_type = type_manager.create_attribute_type(&mut snapshot, &Label::build("name", None)).unwrap();
    name_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();
    let age_type = type_manager.create_attribute_type(&mut snapshot, &Label::build("age", None)).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Integer).unwrap();
    let person_type = type_manager.create_entity_type(&mut snapshot, &Label::build("person", None)).unwrap();
    let owns_name = person_type
        .set_owns(&mut snapshot, &type_manager, &thing_manager, name_type, Ordering::Unordered, StorageCounters::DISABLED)
        .unwrap();
    owns_name.set_annotation(&mut snapshot, &type_manager, &thing_manager, OwnsAnnotation::Key(AnnotationKey)).unwrap();
    let owns_age = person_type
        .set_owns(&mut snapshot, &type_manager, &thing_manager, age_type, Ordering::Unordered, StorageCounters::DISABLED)
        .unwrap();
    owns_age
        .set_annotation(&mut snapshot, &type_manager, &thing_manager, OwnsAnnotation::Cardinality(AnnotationCardinality::new(1, Some(1))))
        .unwrap();
    let friendship_type = type_manager.create_relation_type(&mut snapshot, &Label::build("friendship", None)).unwrap();
    for role in ["a", "b"] {
        friendship_type
            .create_relates(&mut snapshot, &type_manager, &thing_manager, role, Ordering::Unordered, StorageCounters::DISABLED)
            .unwrap();
        let relates = friendship_type.get_relates_role_name(&snapshot, &type_manager, role).unwrap().unwrap();
        relates
            .set_annotation(&mut snapshot, &type_manager, &thing_manager, RelatesAnnotation::Cardinality(AnnotationCardinality::new(1, Some(1))))
            .unwrap();
        // plays stays at its default unbounded cardinality: no exclusive plays lock at commit
        person_type.set_plays(&mut snapshot, &type_manager, &thing_manager, relates.role(), StorageCounters::DISABLED).unwrap();
    }
    thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap_or_else(|e| panic!("{} errors", e.len()));
    snapshot.commit(&mut CommitProfile::disabled()).unwrap();
}

#[derive(Default)]
struct Stats {
    items: u64,
    commits: u64,
    conflicts: u64,
    ops_secs: f64,
    finalise_secs: f64,
    commit_secs: f64,
    stages_micros: HashMap<String, f64>,
}

/// Aggregate a pprof report into inclusive and self sample shares per symbol and print the top entries.
fn print_cpu_profile(label: &str, guard: pprof::ProfilerGuard<'_>) {
    let report = match guard.report().build() {
        Ok(report) => report,
        Err(err) => {
            println!("  cpu profile unavailable: {err}");
            return;
        }
    };
    let mut inclusive: HashMap<String, isize> = HashMap::new();
    let mut selfc: HashMap<String, isize> = HashMap::new();
    let mut total: isize = 0;
    for (frames, count) in report.data.iter() {
        total += count;
        let mut seen = std::collections::HashSet::new();
        for (depth, frame) in frames.frames.iter().enumerate() {
            for (i, sym) in frame.iter().enumerate() {
                let name = sym.name();
                if depth == 0 && i == 0 {
                    *selfc.entry(name.clone()).or_default() += count;
                }
                if seen.insert(name.clone()) {
                    *inclusive.entry(name).or_default() += count;
                }
            }
        }
    }
    if total == 0 {
        println!("  cpu profile: no samples");
        return;
    }
    let pct = |c: isize| c as f64 * 100.0 / total as f64;
    let mut rows: Vec<(String, isize)> = inclusive.into_iter().collect();
    rows.sort_by(|a, b| b.1.cmp(&a.1));
    println!("  cpu profile ({label}): {total} samples; inclusive% self% symbol");
    for (name, count) in rows.iter().take(45) {
        let s = selfc.get(name).copied().unwrap_or(0);
        println!("    {:6.1} {:5.1}  {}", pct(*count), pct(s), name.chars().take(110).collect::<String>());
    }
    let mut srows: Vec<(String, isize)> = selfc.into_iter().collect();
    srows.sort_by(|a, b| b.1.cmp(&a.1));
    println!("  top self ({label}):");
    for (name, count) in srows.iter().take(25) {
        println!("    {:6.1}  {}", pct(*count), name.chars().take(110).collect::<String>());
    }
}

fn record_profile(profile: &CommitProfile, stats: &mut Stats) {
    for line in format!("{profile}").lines() {
        let line = line.trim();
        if let Some(idx) = line.find(" micros: ") {
            let name = line[..idx].trim().to_string();
            if let Ok(v) = line[idx + " micros: ".len()..].trim().parse::<f64>() {
                *stats.stages_micros.entry(name).or_default() += v;
            }
        }
    }
}

struct Shared {
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: Arc<TypeManager>,
    thing_vertex_generator: Arc<ThingVertexGenerator>,
    statistics: Arc<Statistics>,
}

impl Shared {
    fn thing_manager(&self) -> ThingManager {
        ThingManager::new(self.thing_vertex_generator.clone(), self.type_manager.clone(), self.statistics.clone())
    }
}

/// `cache_mb == 0` uses the test utilities' default storage (64 MB block cache, 64 MB write buffers); otherwise a
/// storage with a `cache_mb` block cache and `cache_mb / 4` of write buffers, closer to a server configuration.
fn setup(cache_mb: u64) -> (Box<dyn std::any::Any>, Shared) {
    let (tmp, mut storage): (Box<dyn std::any::Any>, Arc<MVCCStorage<WALClient>>) = if cache_mb == 0 {
        let (tmp, storage) = create_core_storage();
        (Box::new(tmp), storage)
    } else {
        let storage_path = create_tmp_storage_dir();
        let wal = WAL::create(&storage_path, FsyncMetrics::disabled()).unwrap();
        let resources = RocksResources::new(ByteSize::mb(cache_mb), ByteSize::mb((cache_mb / 4).max(64)));
        let storage = Arc::new(
            MVCCStorage::create::<EncodingKeyspace>("db_storage", &storage_path, WALClient::new(wal), &resources).unwrap(),
        );
        (Box::new(storage_path), storage)
    };
    setup_concept_storage(&mut storage);
    create_schema(storage.clone());
    let type_cache = Arc::new(TypeCache::new(storage.clone(), storage.snapshot_watermark()).unwrap());
    let type_manager =
        Arc::new(TypeManager::new(Arc::new(DefinitionKeyGenerator::new()), Arc::new(TypeVertexGenerator::new()), Some(type_cache)));
    let thing_vertex_generator = Arc::new(ThingVertexGenerator::load(storage.clone()).unwrap());
    let mut statistics = Statistics::new(DurabilitySequenceNumber::MIN);
    statistics.may_synchronise(&storage).unwrap();
    (tmp, Shared { storage, type_manager, thing_vertex_generator, statistics: Arc::new(statistics) })
}

fn run_entities(shared: &Shared, thread_id: usize, batch: u64, commits: u64, barrier: Arc<Barrier>) -> Stats {
    let thing_manager = shared.thing_manager();
    let read = shared.storage.clone().open_snapshot_read();
    let person_type = shared.type_manager.get_entity_type(&read, &Label::build("person", None)).unwrap().unwrap();
    let name_type = shared.type_manager.get_attribute_type(&read, &Label::build("name", None)).unwrap().unwrap();
    let age_type = shared.type_manager.get_attribute_type(&read, &Label::build("age", None)).unwrap().unwrap();
    drop(read);
    let mut stats = Stats::default();
    barrier.wait();
    for c in 0..commits {
        let mut snapshot: WriteSnapshot<WALClient> = shared.storage.clone().open_snapshot_write();
        let t = Instant::now();
        for i in 0..batch {
            let n = c * batch + i;
            let person = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
            // 14-char name: inline string, unique across threads, so @key never conflicts
            let name = thing_manager
                .create_attribute(&mut snapshot, name_type, Value::String(format!("{thread_id:02}{n:012}").into()))
                .unwrap();
            let age = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer((n % 90) as i64)).unwrap();
            person.set_has_unordered(&mut snapshot, &thing_manager, &name, StorageCounters::DISABLED).unwrap();
            person.set_has_unordered(&mut snapshot, &thing_manager, &age, StorageCounters::DISABLED).unwrap();
        }
        stats.ops_secs += t.elapsed().as_secs_f64();
        let t = Instant::now();
        thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap_or_else(|e| panic!("{} errors", e.len()));
        stats.finalise_secs += t.elapsed().as_secs_f64();
        let mut profile = CommitProfile::new(true);
        profile.start();
        let t = Instant::now();
        let result = snapshot.commit(&mut profile);
        stats.commit_secs += t.elapsed().as_secs_f64();
        profile.end();
        record_profile(&profile, &mut stats);
        stats.commits += 1;
        match result {
            Ok(_) => stats.items += batch,
            Err(_) => stats.conflicts += 1,
        }
    }
    stats
}

fn run_relations(
    shared: &Shared,
    thread_id: usize,
    batch: u64,
    commits: u64,
    persons: Arc<Vec<Object>>,
    barrier: Arc<Barrier>,
) -> Stats {
    let thing_manager = shared.thing_manager();
    let read = shared.storage.clone().open_snapshot_read();
    let friendship_type = shared.type_manager.get_relation_type(&read, &Label::build("friendship", None)).unwrap().unwrap();
    let role_a = friendship_type.get_relates_role_name(&read, &shared.type_manager, "a").unwrap().unwrap().role();
    let role_b = friendship_type.get_relates_role_name(&read, &shared.type_manager, "b").unwrap().unwrap().role();
    drop(read);
    let mut stats = Stats::default();
    let mut seed = (thread_id as u64 + 1).wrapping_mul(0x9E3779B97F4A7C15) | 1;
    let n = persons.len() as u64;
    barrier.wait();
    for _ in 0..commits {
        let mut snapshot: WriteSnapshot<WALClient> = shared.storage.clone().open_snapshot_write();
        let t = Instant::now();
        for _ in 0..batch {
            seed = xorshift(seed);
            let x = persons[(seed % n) as usize];
            seed = xorshift(seed);
            let y = persons[(seed % n) as usize];
            let relation = thing_manager.create_relation(&mut snapshot, friendship_type).unwrap();
            relation.add_player(&mut snapshot, &thing_manager, role_a, x, StorageCounters::DISABLED).unwrap();
            relation.add_player(&mut snapshot, &thing_manager, role_b, y, StorageCounters::DISABLED).unwrap();
        }
        stats.ops_secs += t.elapsed().as_secs_f64();
        let t = Instant::now();
        thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap_or_else(|e| panic!("{} errors", e.len()));
        stats.finalise_secs += t.elapsed().as_secs_f64();
        let mut profile = CommitProfile::new(true);
        profile.start();
        let t = Instant::now();
        let result = snapshot.commit(&mut profile);
        stats.commit_secs += t.elapsed().as_secs_f64();
        profile.end();
        record_profile(&profile, &mut stats);
        stats.commits += 1;
        match result {
            Ok(_) => stats.items += batch,
            Err(_) => stats.conflicts += 1,
        }
    }
    stats
}

/// Persisted entities for the relation mode, created with `threads` writers in parallel through the normal commit
/// path. Names are `q<thread><n>`: 14 chars, inline, unique across threads.
fn precreate_persons(shared: &Arc<Shared>, count: u64, threads: usize) -> Vec<Object> {
    let per_thread = count.div_ceil(threads as u64);
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let shared = shared.clone();
            thread::spawn(move || {
                let thing_manager = shared.thing_manager();
                let read = shared.storage.clone().open_snapshot_read();
                let person_type = shared.type_manager.get_entity_type(&read, &Label::build("person", None)).unwrap().unwrap();
                let name_type = shared.type_manager.get_attribute_type(&read, &Label::build("name", None)).unwrap().unwrap();
                let age_type = shared.type_manager.get_attribute_type(&read, &Label::build("age", None)).unwrap().unwrap();
                drop(read);
                let start = t as u64 * per_thread;
                let end = (start + per_thread).min(count);
                let mut persons = Vec::with_capacity((end - start) as usize);
                let batch = 50_000u64;
                let mut done = start;
                while done < end {
                    let n = batch.min(end - done);
                    let mut snapshot: WriteSnapshot<WALClient> = shared.storage.clone().open_snapshot_write();
                    for i in done..done + n {
                        let person = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
                        let name = thing_manager
                            .create_attribute(&mut snapshot, name_type, Value::String(format!("q{t:02}{i:011}").into()))
                            .unwrap();
                        let age = thing_manager.create_attribute(&mut snapshot, age_type, Value::Integer((i % 90) as i64)).unwrap();
                        person.set_has_unordered(&mut snapshot, &thing_manager, &name, StorageCounters::DISABLED).unwrap();
                        person.set_has_unordered(&mut snapshot, &thing_manager, &age, StorageCounters::DISABLED).unwrap();
                        persons.push(Object::Entity(person));
                    }
                    thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap_or_else(|e| panic!("{} errors", e.len()));
                    snapshot.commit(&mut CommitProfile::disabled()).unwrap();
                    done += n;
                }
                persons
            })
        })
        .collect();
    let mut all = Vec::with_capacity(count as usize);
    for h in handles {
        all.extend(h.join().unwrap());
    }
    all
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).cloned().unwrap_or_else(|| "entities".to_string());
    let batch: u64 = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(10_000);
    let commits: u64 = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(10);
    let thread_counts: Vec<usize> = args.get(4).map(|s| s.split(',').map(|x| x.parse().unwrap()).collect()).unwrap_or(vec![1, 2, 4, 8]);
    let persisted: u64 = args.get(5).map(|s| s.parse().unwrap()).unwrap_or(500_000);
    let cache_mb: u64 = args.get(6).map(|s| s.parse().unwrap()).unwrap_or(0);
    let profile_hz: i32 = args.get(7).map(|s| s.parse().unwrap()).unwrap_or(0);
    let stats_updater: bool = args.get(8).map(|s| s == "1").unwrap_or(false);

    println!(
        "mode={mode} batch={batch} commits/thread={commits} threads={thread_counts:?}{} rocks-cache={} cpu-profile={} statistics-updater={}",
        if mode == "relations" { format!(" persisted-entities={persisted}") } else { String::new() },
        if cache_mb == 0 { "test default 64MB".to_string() } else { format!("{cache_mb}MB, write buffers {}MB", (cache_mb / 4).max(64)) },
        if profile_hz == 0 { "off".to_string() } else { format!("{profile_hz} Hz") },
        if stats_updater { "on, 50ms like the server" } else { "off" }
    );
    for &threads in &thread_counts {
        let (_tmp, shared) = setup(cache_mb);
        let shared = Arc::new(shared);
        let run_start = Instant::now();
        // Optional: the server's statistics updater, which re-decodes every commit record from the WAL every 50ms
        // (database.rs make_update_statistics_fn), so its CPU and its lag behind the watermark become visible.
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let updater = if stats_updater {
            let shared = shared.clone();
            let stop = stop.clone();
            Some(thread::spawn(move || {
                let mut statistics = (*shared.statistics).clone();
                let (mut syncs, mut busy) = (0u64, 0.0f64);
                loop {
                    if shared.storage.snapshot_watermark() > statistics.sequence_number {
                        let t = Instant::now();
                        statistics.may_synchronise(&shared.storage).expect("statistics sync failed");
                        busy += t.elapsed().as_secs_f64();
                        syncs += 1;
                    }
                    if stop.load(std::sync::atomic::Ordering::Relaxed) && shared.storage.snapshot_watermark() <= statistics.sequence_number {
                        break;
                    }
                    thread::sleep(std::time::Duration::from_millis(50));
                }
                (syncs, busy, statistics.sequence_number, statistics.total_count)
            }))
        } else {
            None
        };
        let persons = if mode == "relations" {
            let t = Instant::now();
            let persons = precreate_persons(&shared, persisted, threads);
            println!("  pre-created {} persisted entities with {threads} threads in {:.1}s", persons.len(), t.elapsed().as_secs_f64());
            Arc::new(persons)
        } else {
            Arc::new(Vec::new())
        };
        let barrier = Arc::new(Barrier::new(threads + 1));
        let mut handles = Vec::new();
        for thread_id in 0..threads {
            let shared = shared.clone();
            let barrier = barrier.clone();
            let persons = persons.clone();
            let mode = mode.clone();
            handles.push(thread::spawn(move || {
                if mode == "relations" {
                    run_relations(&shared, thread_id, batch, commits, persons, barrier)
                } else {
                    run_entities(&shared, thread_id, batch, commits, barrier)
                }
            }));
        }
        let guard = if profile_hz > 0 { Some(pprof::ProfilerGuard::new(profile_hz).unwrap()) } else { None };
        barrier.wait();
        let wall = Instant::now();
        let stats: Vec<Stats> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let wall = wall.elapsed().as_secs_f64();
        let writers_done = Instant::now();
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(guard) = guard {
            print_cpu_profile(&format!("{mode}, {threads} threads"), guard);
        }
        if let Some(updater) = updater {
            let (syncs, busy, seq, total) = updater.join().unwrap();
            println!(
                "  statistics updater: {syncs} syncs, {busy:.1}s busy over {:.1}s of pre-create + load ({:.0}% of one core), caught up {:.1}s after the writers finished; final seq {} (watermark {}), total_count {total}",
                run_start.elapsed().as_secs_f64(), busy * 100.0 / run_start.elapsed().as_secs_f64(), writers_done.elapsed().as_secs_f64(), seq, shared.storage.snapshot_watermark()
            );
        }

        let items: u64 = stats.iter().map(|s| s.items).sum();
        let conflicts: u64 = stats.iter().map(|s| s.conflicts).sum();
        let attempted: u64 = stats.iter().map(|s| s.commits * batch).sum();
        let per = |f: &dyn Fn(&Stats) -> f64| stats.iter().map(f).sum::<f64>() * 1e6 / attempted as f64;
        println!(
            "\n=== threads={threads}: {items} {mode} committed in {wall:.2}s wall -> {:.0} {mode}/s aggregate ({:.1} us wall per item); conflicts {conflicts}/{}",
            items as f64 / wall, wall * 1e6 / items.max(1) as f64, stats.iter().map(|s| s.commits).sum::<u64>()
        );
        println!(
            "  per item, thread-average us: ops {:.1} | finalise {:.1} | commit {:.1}",
            per(&|s| s.ops_secs), per(&|s| s.finalise_secs), per(&|s| s.commit_secs)
        );
        let mut stages: BTreeMap<String, f64> = BTreeMap::new();
        for s in &stats {
            for (k, v) in &s.stages_micros {
                *stages.entry(k.clone()).or_default() += v;
            }
        }
        println!("  commit stages, thread-average us per item:");
        for (name, micros) in stages {
            let per_item = micros / attempted as f64;
            if per_item >= 0.05 {
                println!("    {:<55} {:>7.2}", name, per_item);
            }
        }
        println!("  storage: ~{} MB, ~{} keys", shared.storage.estimate_size_in_bytes().unwrap() / 1_000_000, shared.storage.estimate_key_count().unwrap());
    }
}
