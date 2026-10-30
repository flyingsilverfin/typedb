/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/*
 * Has-reverse scan benchmark. Loads `ownerships` persons that each own one integer `age` (values `n % distinct`)
 * through the ThingManager and the normal commit path, then scans every has-reverse edge of `age`
 * single-threaded at four layers, over the same key range:
 *
 *   raw-rocksdb   rocksdb::DBRawIterator on a read-only open of a checkpoint of the keyspace, with TypeDB's
 *                 own RocksDB options and a block cache of the same size
 *   keyspace      MVCCStorage::iterate_keyspace_range: TypeDB's thin wrapper over the raw iterator (live storage)
 *   snapshot      ReadSnapshot::iterate_range: MVCC version resolution over the keyspace iterator
 *   has-reverse   ThingManager::get_has_reverse: snapshot iterator + decoding to (Has, count)
 *
 * The first pass of each layer warms the cache and is reported separately. Further variants (write snapshots, buffered
 * writes, multiple versions, point gets, short scans) are described at `measure_versions`. Env:
 *   HRSCAN_LAYERS=<name,...>   only measure these layers
 *   HRSCAN_VERSIONS=0|nomv     skip the variants / the multi-version commit
 *   HRSCAN_PROFILE=<dir>       also sample each layer with pprof (HRSCAN_PROFILE_SECS, default 5) into <dir>
 *
 * Run:
 *   has_reverse_scan <ownerships> [distinct-values=ownerships/10] [load-threads=8] [batch=10000] [passes=3]
 *                    [cache-mb=4096] [settle-secs=30]
 */

use std::{
    env,
    hint::black_box,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use concept::{
    thing::{object::ObjectAPI, statistics::Statistics, thing_manager::ThingManager},
    type_::{
        Ordering, OwnerAPI,
        type_manager::{TypeManager, type_cache::TypeCache},
    },
};
use diagnostics::metrics::FsyncMetrics;
use durability::{DurabilitySequenceNumber, wal::WAL};
use encoding::{
    EncodingKeyspace,
    graph::{
        Typed,
        definition::definition_key_generator::DefinitionKeyGenerator,
        thing::{edge::ThingEdgeHasReverse, vertex_generator::ThingVertexGenerator, vertex_object::ObjectVertex},
        type_::{vertex::TypeVertexEncoding, vertex_generator::TypeVertexGenerator},
    },
    value::{label::Label, value::Value, value_type::ValueType},
};
use lending_iterator::LendingIterator;
use bytes::byte_array::ByteArray;
use options::byte_size::ByteSize;
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use resource::profile::{CommitProfile, StorageCounters};
use storage::{
    MVCCStorage,
    durability_client::WALClient,
    key_range::KeyRange,
    key_value::{StorageKeyArray, StorageKeyReference},
    keyspace::{IteratorPool, KeyspaceSet, rocks_resources::RocksResources},
    recovery::checkpoint::CheckpointWriter,
    snapshot::{CommittableSnapshot, ReadableSnapshot, SchemaSnapshot, WritableSnapshot, WriteSnapshot},
};
use test_utils::create_tmp_storage_dir;
use test_utils_concept::{load_managers, setup_concept_storage};

#[path = "scan_common.rs"]
mod scan_common;
use scan_common::{arg, measure};

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

fn create_schema(storage: Arc<MVCCStorage<WALClient>>) {
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let mut snapshot: SchemaSnapshot<WALClient> = storage.clone().open_snapshot_schema();
    let age_type = type_manager.create_attribute_type(&mut snapshot, &Label::build("age", None)).unwrap();
    age_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Integer).unwrap();
    let person_type = type_manager.create_entity_type(&mut snapshot, &Label::build("person", None)).unwrap();
    person_type
        .set_owns(&mut snapshot, &type_manager, &thing_manager, age_type, Ordering::Unordered, StorageCounters::DISABLED)
        .unwrap();
    thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap_or_else(|e| panic!("{} errors", e.len()));
    snapshot.commit(&mut CommitProfile::disabled()).unwrap();
}

fn load(shared: &Arc<Shared>, ownerships: u64, distinct: u64, threads: u64, batch: u64) {
    let per_thread = ownerships.div_ceil(threads);
    let start = Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let shared = shared.clone();
            thread::spawn(move || {
                let thing_manager = shared.thing_manager();
                let read = shared.storage.clone().open_snapshot_read();
                let person_type =
                    shared.type_manager.get_entity_type(&read, &Label::build("person", None)).unwrap().unwrap();
                let age_type =
                    shared.type_manager.get_attribute_type(&read, &Label::build("age", None)).unwrap().unwrap();
                drop(read);
                let (first, end) = (t * per_thread, ((t + 1) * per_thread).min(ownerships));
                let mut n = first;
                while n < end {
                    let batch_end = (n + batch).min(end);
                    let mut snapshot: WriteSnapshot<WALClient> = shared.storage.clone().open_snapshot_write();
                    for i in n..batch_end {
                        let person = thing_manager.create_entity(&mut snapshot, person_type).unwrap();
                        let age = thing_manager
                            .create_attribute(&mut snapshot, age_type, Value::Integer((i % distinct) as i64))
                            .unwrap();
                        person.set_has_unordered(&mut snapshot, &thing_manager, &age, StorageCounters::DISABLED).unwrap();
                    }
                    thing_manager
                        .finalise(&mut snapshot, StorageCounters::DISABLED)
                        .unwrap_or_else(|e| panic!("{} errors", e.len()));
                    snapshot.commit(&mut CommitProfile::disabled()).expect("commit failed");
                    n = batch_end;
                    if t == 0 && (n - first) % (batch * 50) == 0 {
                        let done = (n - first) * threads;
                        let secs = start.elapsed().as_secs_f64();
                        println!("  load: ~{done} ownerships in {secs:.0}s ({:.0}/s)", done as f64 / secs);
                    }
                }
            })
        })
        .collect();
    handles.into_iter().for_each(|h| h.join().unwrap());
    let secs = start.elapsed().as_secs_f64();
    println!("loaded {ownerships} ownerships ({distinct} distinct values) in {secs:.1}s ({:.0}/s)", ownerships as f64 / secs);
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let ownerships = arg(&args, 1, 10_000_000);
    let distinct = arg(&args, 2, (ownerships / 10).max(1));
    let threads = arg(&args, 3, 8);
    let batch = arg(&args, 4, 10_000);
    let passes = arg(&args, 5, 3).max(1);
    let cache_mb = arg(&args, 6, 4096);
    let settle_secs = arg(&args, 7, 30);

    let storage_path = create_tmp_storage_dir();
    let wal = WAL::create(&storage_path, FsyncMetrics::disabled()).unwrap();
    let resources = RocksResources::new(ByteSize::mb(cache_mb), ByteSize::mb((cache_mb / 4).max(64)));
    let mut storage = Arc::new(
        MVCCStorage::create::<EncodingKeyspace>("db_storage", &storage_path, WALClient::new(wal), &resources).unwrap(),
    );
    setup_concept_storage(&mut storage);
    create_schema(storage.clone());
    let type_cache = Arc::new(TypeCache::new(storage.clone(), storage.snapshot_watermark()).unwrap());
    let type_manager = Arc::new(TypeManager::new(
        Arc::new(DefinitionKeyGenerator::new()),
        Arc::new(TypeVertexGenerator::new()),
        Some(type_cache),
    ));
    let thing_vertex_generator = Arc::new(ThingVertexGenerator::load(storage.clone()).unwrap());
    let mut statistics = Statistics::new(DurabilitySequenceNumber::MIN);
    statistics.may_synchronise_from_wal(&storage).unwrap();
    let shared =
        Arc::new(Shared { storage: storage.clone(), type_manager, thing_vertex_generator, statistics: Arc::new(statistics) });

    load(&shared, ownerships, distinct, threads, batch);

    // Flush the memtables, let compaction settle, then checkpoint the settled files for the raw read-only open
    let checkpoint = || {
        let writer = CheckpointWriter::new(storage.path()).unwrap();
        storage.checkpoint(&writer).unwrap();
        writer.finish().unwrap()
    };
    checkpoint();
    println!("flushed; settling for {settle_secs}s");
    thread::sleep(Duration::from_secs(settle_secs));
    let checkpoint = checkpoint();

    let thing_manager = shared.thing_manager();
    let snapshot = storage.clone().open_snapshot_read();
    let age_type = shared.type_manager.get_attribute_type(&snapshot, &Label::build("age", None)).unwrap().unwrap();
    let prefix = ThingEdgeHasReverse::prefix_from_attribute_type(
        age_type.get_value_type_without_source(&snapshot, &shared.type_manager).unwrap().unwrap().category(),
        age_type.vertex().type_id_(),
    );
    let keyspace = EncodingKeyspace::iter().find(|keyspace| keyspace.id() == prefix.keyspace_id()).unwrap();
    let prefix_bytes = prefix.bytes().to_vec();

    let raw_resources = RocksResources::new(ByteSize::mb(cache_mb), ByteSize::mb(64));
    let raw_db = rocksdb::DB::open_for_read_only(
        &keyspace.rocks_configuration(&raw_resources),
        checkpoint.directory.join(keyspace.name()),
        false,
    )
    .unwrap();
    let files: Vec<String> = (0..7)
        .map(|level| raw_db.property_value(&format!("rocksdb.num-files-at-level{level}")).unwrap().unwrap_or_default())
        .collect();
    println!(
        "keyspace {} (prefix length {:?}): files per level {:?}, ~{} keys, scanning prefix {:02x?}",
        keyspace.name(),
        keyspace.prefix_length(),
        files,
        raw_db.property_value("rocksdb.estimate-num-keys").unwrap().unwrap_or_default(),
        prefix_bytes
    );

    measure("raw-rocksdb", passes, ownerships, || {
        let mut read_options = rocksdb::ReadOptions::default();
        read_options.set_total_order_seek(true);
        let mut iterator = raw_db.raw_iterator_opt(read_options);
        iterator.seek(&prefix_bytes);
        let mut count = 0;
        while let Some(key) = iterator.key() {
            if !key.starts_with(&prefix_bytes) {
                break;
            }
            black_box(iterator.value());
            count += 1;
            iterator.next();
        }
        iterator.status().unwrap();
        count
    });

    let iterator_pool = IteratorPool::new();
    measure("keyspace", passes, ownerships, || {
        let range = KeyRange::new_within(prefix.clone(), ThingEdgeHasReverse::FIXED_WIDTH_ENCODING);
        let mut iterator = storage.iterate_keyspace_range(&iterator_pool, range, StorageCounters::DISABLED);
        let mut count = 0;
        while let Some(item) = iterator.next() {
            black_box(item.unwrap());
            count += 1;
        }
        count
    });

    measure("snapshot", passes, ownerships, || {
        let range = KeyRange::new_within(prefix.clone(), ThingEdgeHasReverse::FIXED_WIDTH_ENCODING);
        let mut iterator = snapshot.iterate_range(&range, StorageCounters::DISABLED);
        let mut count = 0;
        while let Some(item) = iterator.next() {
            black_box(item.unwrap());
            count += 1;
        }
        count
    });

    measure("has-reverse", passes, ownerships, || {
        let mut count = 0;
        for item in thing_manager.get_has_reverse(&snapshot, age_type, StorageCounters::DISABLED).unwrap() {
            let (has, repetitions) = item.unwrap();
            black_box((has.owner(), has.attribute()));
            count += repetitions;
        }
        count
    });

    if env::var("HRSCAN_VERSIONS").map(|v| v != "0").unwrap_or(true) {
        measure_versions(&storage, &thing_manager, &snapshot, age_type, &prefix, ownerships, passes);
    }
}

/// Variants exercising the other snapshot-iterator paths over the same key range (set `HRSCAN_VERSIONS=0` to skip,
/// or `HRSCAN_VERSIONS=nomv` to skip the multi-version commit). Rates count returned keys (or gets):
///   point-get     ReadSnapshot::get of every 10th key: one short-lived MVCC iterator per get
///   short-scans   one ReadSnapshot::iterate_range per attribute value (ownerships/distinct keys each)
///   snapshot-wr   write snapshot with no buffered writes in the range
///   snapshot-buf  write snapshot with buffered writes in the range: every 100th key re-put, every 100th (+50) deleted
///   *-mv-new      after committing a new version of every 10th key and deleting every 10th (+5) key, a new snapshot
///   *-mv-old      the same data, from a snapshot opened before that commit (skips the newer, invisible versions)
fn measure_versions(
    storage: &Arc<MVCCStorage<WALClient>>,
    thing_manager: &ThingManager,
    snapshot: &impl ReadableSnapshot,
    age_type: concept::type_::attribute_type::AttributeType,
    prefix: &storage::key_value::StorageKey<'static, { ThingEdgeHasReverse::LENGTH_PREFIX_FROM_TYPE }>,
    ownerships: u64,
    passes: u64,
) {
    let range = || KeyRange::new_within(prefix.clone(), ThingEdgeHasReverse::FIXED_WIDTH_ENCODING);
    fn count_range(snapshot: &impl ReadableSnapshot, range: &KeyRange<storage::key_value::StorageKey<'_, { ThingEdgeHasReverse::LENGTH_PREFIX_FROM_TYPE }>>) -> u64 {
        let mut iterator = snapshot.iterate_range(range, StorageCounters::DISABLED);
        let mut count = 0;
        while let Some(item) = iterator.next() {
            black_box(item.unwrap());
            count += 1;
        }
        count
    }
    fn count_has_reverse(
        thing_manager: &ThingManager,
        snapshot: &impl ReadableSnapshot,
        age_type: concept::type_::attribute_type::AttributeType,
    ) -> u64 {
        let mut count = 0;
        for item in thing_manager.get_has_reverse(snapshot, age_type, StorageCounters::DISABLED).unwrap() {
            let (has, repetitions) = item.unwrap();
            black_box((has.owner(), has.attribute()));
            count += repetitions;
        }
        count
    }

    // collected through the keyspace iterator (each key has a single version at this point), so that profiles of the
    // snapshot iterator only cover the measured scans
    let mut entries: Vec<(StorageKeyArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>)> = Vec::new();
    let iterator_pool = IteratorPool::new();
    let mut iterator = storage.iterate_keyspace_range(&iterator_pool, range(), StorageCounters::DISABLED);
    while let Some(item) = iterator.next() {
        let (key, value) = item.unwrap();
        const MVCC_SUFFIX_LENGTH: usize = 9; // sequence number + operation
        let key = StorageKeyArray::new_raw(prefix.keyspace_id(), ByteArray::copy(&key[..key.len() - MVCC_SUFFIX_LENGTH]));
        entries.push((key, ByteArray::copy(value)));
    }
    drop(iterator);
    assert_eq!(entries.len() as u64, ownerships);
    assert!(snapshot.get::<BUFFER_VALUE_INLINE>(StorageKeyReference::from(&entries[0].0), StorageCounters::DISABLED).unwrap().is_some());

    // short-lived iterators: one point get per key, and one range scan per attribute (all owners of one value)
    let gets: Vec<&StorageKeyArray<BUFFER_KEY_INLINE>> = entries.iter().step_by(10).map(|(key, _)| key).collect();
    measure("point-get", passes, gets.len() as u64, || {
        let mut count = 0;
        for key in &gets {
            let value = snapshot.get::<BUFFER_VALUE_INLINE>(StorageKeyReference::from(*key), StorageCounters::DISABLED);
            count += value.unwrap().is_some() as u64;
        }
        count
    });
    let mut attribute_prefixes: Vec<StorageKeyArray<BUFFER_KEY_INLINE>> = Vec::new();
    for (key, _) in &entries {
        let attribute_prefix = &key.bytes()[..key.bytes().len() - ObjectVertex::LENGTH];
        if attribute_prefixes.last().is_none_or(|last| last.bytes() != attribute_prefix) {
            attribute_prefixes.push(StorageKeyArray::new_raw(prefix.keyspace_id(), ByteArray::copy(attribute_prefix)));
        }
    }
    measure("short-scans", passes, ownerships, || {
        let mut count = 0;
        for attribute_prefix in &attribute_prefixes {
            let range = KeyRange::new_within(
                storage::key_value::StorageKey::<BUFFER_KEY_INLINE>::Reference(StorageKeyReference::from(attribute_prefix)),
                ThingEdgeHasReverse::FIXED_WIDTH_ENCODING,
            );
            let mut iterator = snapshot.iterate_range(&range, StorageCounters::DISABLED);
            while let Some(item) = iterator.next() {
                black_box(item.unwrap());
                count += 1;
            }
        }
        count
    });

    let write_snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    measure("snapshot-wr", passes, ownerships, || count_range(&write_snapshot, &range()));
    measure("has-rev-wr", passes, ownerships, || count_has_reverse(thing_manager, &write_snapshot, age_type));
    drop(write_snapshot);

    let mut buffered: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    let mut deleted = 0;
    for (i, (key, value)) in entries.iter().enumerate() {
        match i % 100 {
            0 => buffered.put_val(key.clone(), value.clone()),
            50 => {
                buffered.delete(key.clone());
                deleted += 1;
            }
            _ => (),
        }
    }
    measure("snapshot-buf", passes, ownerships - deleted, || count_range(&buffered, &range()));
    drop(buffered);

    if env::var("HRSCAN_VERSIONS").is_ok_and(|v| v == "nomv") {
        return;
    }
    let old_snapshot = storage.clone().open_snapshot_read();
    let mut versions: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    let mut deleted = 0;
    for (i, (key, value)) in entries.iter().enumerate() {
        match i % 10 {
            0 => versions.put_val(key.clone(), value.clone()),
            5 => {
                versions.delete(key.clone());
                deleted += 1;
            }
            _ => (),
        }
    }
    versions.commit(&mut CommitProfile::disabled()).unwrap();
    let writer = CheckpointWriter::new(storage.path()).unwrap();
    storage.checkpoint(&writer).unwrap();
    writer.finish().unwrap();
    let new_snapshot = storage.clone().open_snapshot_read();
    measure("snapshot-mv-new", passes, ownerships - deleted, || count_range(&new_snapshot, &range()));
    measure("has-rev-mv-new", passes, ownerships - deleted, || count_has_reverse(thing_manager, &new_snapshot, age_type));
    measure("snapshot-mv-old", passes, ownerships, || count_range(&old_snapshot, &range()));
}
