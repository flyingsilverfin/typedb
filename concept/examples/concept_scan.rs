/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/*
 * Concept iterator scan benchmark, a sibling of has_reverse_scan covering the other concept iterators.
 * Loads `persons` persons through the ThingManager. Each owns a short `nick` string (inline in the attribute id)
 * and a long `bio` string (hashed), with `persons / 10` distinct values each (persons 10k..10k+9 share one), and consecutive pairs of persons are
 * linked by a `friendship` relation (roles `a` and `b`, so relation-indexed). Then, single-threaded, scans these
 * key ranges, each at four layers like has_reverse_scan (raw-rocksdb on a checkpoint, keyspace, snapshot, and the
 * concept iterator decoding every item):
 *
 *   has            ThingManager::get_has (HasIterator), 2 x persons keys
 *   has-rev-nick   ThingManager::get_has_reverse(nick) (HasReverseIterator over inline strings), persons keys
 *   has-rev-bio    ThingManager::get_has_reverse(bio) (HasReverseIterator over hashed strings), persons keys
 *   links          ThingManager::get_links (LinksIterator), persons keys
 *   links-rev      ThingManager::get_links_reverse_by_player_type_range(person) (LinksReverseIterator), persons keys
 *   indexed        ThingManager::get_indexed_relations_in(friendship, person) (IndexedRelationsIterator), persons keys
 *   entities       ThingManager::get_entities_in(person) (InstanceIterator<Entity>), persons keys
 *   nick-attrs     ThingManager::get_attributes_in(nick) (AttributeIterator): persons / 10 attributes, whose concept
 *                  layer also walks the persons has-reverse keys to check each attribute is owned
 *
 * Rates are per key of the scanned range (per attribute for nick-attrs). Env: HRSCAN_LAYERS=<scan/layer,...> to
 * select, HRSCAN_PROFILE=<dir> to also sample each layer with pprof (see scan_common.rs).
 *
 * Run:
 *   concept_scan <persons> [load-threads=8] [batch=5000] [passes=3] [cache-mb=4096] [settle-secs=30]
 */

use std::{
    collections::HashMap,
    env,
    hint::black_box,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use bytes::byte_array::ByteArray;
use concept::{
    thing::{
        ThingAPI,
        attribute::Attribute,
        object::{Object, ObjectAPI},
        statistics::Statistics,
        thing_manager::ThingManager,
    },
    type_::{
        Ordering, OwnerAPI, PlayerAPI,
        attribute_type::AttributeType,
        entity_type::EntityType,
        object_type::ObjectType,
        relation_type::RelationType,
        role_type::RoleType,
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
        thing::{
            ThingVertex,
            edge::{ThingEdgeHas, ThingEdgeHasReverse, ThingEdgeIndexedRelation, ThingEdgeLinks},
            vertex_attribute::AttributeVertex,
            vertex_generator::ThingVertexGenerator,
            vertex_object::ObjectVertex,
        },
        type_::{vertex::TypeVertexEncoding, vertex_generator::TypeVertexGenerator},
    },
    layout::prefix::Prefix,
    value::{label::Label, value::Value, value_type::ValueType},
};
use lending_iterator::LendingIterator;
use options::byte_size::ByteSize;
use resource::{
    constants::snapshot::BUFFER_KEY_INLINE,
    profile::{CommitProfile, StorageCounters},
};
use storage::{
    MVCCStorage,
    durability_client::WALClient,
    key_range::KeyRange,
    key_value::{StorageKey, StorageKeyArray},
    keyspace::{IteratorPool, KeyspaceSet, rocks_resources::RocksResources},
    recovery::checkpoint::{CheckpointReader, CheckpointWriter},
    snapshot::{CommittableSnapshot, ReadSnapshot, ReadableSnapshot, SchemaSnapshot, WriteSnapshot},
};
use test_utils::create_tmp_storage_dir;
use test_utils_concept::{load_managers, setup_concept_storage};

#[path = "scan_common.rs"]
mod scan_common;
use scan_common::{arg, measure};

#[derive(Clone, Copy)]
struct Schema {
    person: EntityType,
    nick: AttributeType,
    bio: AttributeType,
    friendship: RelationType,
    role_a: RoleType,
    role_b: RoleType,
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

fn create_schema(storage: Arc<MVCCStorage<WALClient>>) {
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let mut snapshot: SchemaSnapshot<WALClient> = storage.clone().open_snapshot_schema();
    let person = type_manager.create_entity_type(&mut snapshot, &Label::build("person", None)).unwrap();
    for name in ["nick", "bio"] {
        let attribute_type = type_manager.create_attribute_type(&mut snapshot, &Label::build(name, None)).unwrap();
        attribute_type.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::String).unwrap();
        person
            .set_owns(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                attribute_type,
                Ordering::Unordered,
                StorageCounters::DISABLED,
            )
            .unwrap();
    }
    let friendship = type_manager.create_relation_type(&mut snapshot, &Label::build("friendship", None)).unwrap();
    for role in ["a", "b"] {
        let relates = friendship
            .create_relates(
                &mut snapshot,
                &type_manager,
                &thing_manager,
                role,
                Ordering::Unordered,
                StorageCounters::DISABLED,
            )
            .unwrap();
        person
            .set_plays(&mut snapshot, &type_manager, &thing_manager, relates.role(), StorageCounters::DISABLED)
            .unwrap();
    }
    thing_manager.finalise(&mut snapshot, StorageCounters::DISABLED).unwrap_or_else(|e| panic!("{} errors", e.len()));
    snapshot.commit(&mut CommitProfile::disabled()).unwrap();
}

fn read_schema(snapshot: &impl ReadableSnapshot, type_manager: &TypeManager) -> Schema {
    let attribute = |name| type_manager.get_attribute_type(snapshot, &Label::build(name, None)).unwrap().unwrap();
    let friendship = type_manager.get_relation_type(snapshot, &Label::build("friendship", None)).unwrap().unwrap();
    let role = |name| friendship.get_relates_role_name(snapshot, type_manager, name).unwrap().unwrap().role();
    Schema {
        person: type_manager.get_entity_type(snapshot, &Label::build("person", None)).unwrap().unwrap(),
        nick: attribute("nick"),
        bio: attribute("bio"),
        friendship,
        role_a: role("a"),
        role_b: role("b"),
    }
}

fn load(shared: &Arc<Shared>, persons: u64, threads: u64, batch: u64) {
    // persons 10k..10k+9 share their attribute values: concurrent transactions never create the same attribute (which
    // would be an isolation conflict), as each thread loads a range of pairs aligned to 5 pairs
    let pairs = persons / 2;
    let per_thread = pairs.div_ceil(threads).next_multiple_of(5);
    let start = Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let shared = shared.clone();
            thread::spawn(move || {
                let thing_manager = shared.thing_manager();
                let schema = read_schema(&shared.storage.clone().open_snapshot_read(), &shared.type_manager);
                let (first, end) = (t * per_thread, ((t + 1) * per_thread).min(pairs));
                let mut pair = first;
                while pair < end {
                    let batch_end = (pair + batch / 2).min(end);
                    let mut snapshot: WriteSnapshot<WALClient> = shared.storage.clone().open_snapshot_write();
                    for p in pair..batch_end {
                        let relation = thing_manager.create_relation(&mut snapshot, schema.friendship).unwrap();
                        for (i, role) in [(2 * p, schema.role_a), (2 * p + 1, schema.role_b)] {
                            let person = thing_manager.create_entity(&mut snapshot, schema.person).unwrap();
                            let value = i / 10;
                            let nick = Value::String(format!("n{value}").into());
                            let bio = Value::String(format!("the biography of person number {value}").into());
                            for (attribute_type, value) in [(schema.nick, nick), (schema.bio, bio)] {
                                let attribute =
                                    thing_manager.create_attribute(&mut snapshot, attribute_type, value).unwrap();
                                person
                                    .set_has_unordered(&mut snapshot, &thing_manager, &attribute, StorageCounters::DISABLED)
                                    .unwrap();
                            }
                            relation
                                .add_player(
                                    &mut snapshot,
                                    &thing_manager,
                                    role,
                                    Object::Entity(person),
                                    StorageCounters::DISABLED,
                                )
                                .unwrap();
                        }
                    }
                    thing_manager
                        .finalise(&mut snapshot, StorageCounters::DISABLED)
                        .unwrap_or_else(|e| panic!("{} errors: {:?}", e.len(), e.first()));
                    snapshot.commit(&mut CommitProfile::disabled()).expect("commit failed");
                    pair = batch_end;
                    if t == 0 && (pair - first) % (batch * 25) == 0 {
                        let done = 2 * (pair - first) * threads;
                        let secs = start.elapsed().as_secs_f64();
                        println!("  load: ~{done} persons in {secs:.0}s ({:.0}/s)", done as f64 / secs);
                    }
                }
            })
        })
        .collect();
    handles.into_iter().for_each(|h| h.join().unwrap());
    let secs = start.elapsed().as_secs_f64();
    println!("loaded {persons} persons in {secs:.1}s ({:.0}/s)", persons as f64 / secs);
}

/// Measures one key range at the raw-rocksdb, keyspace and snapshot layers, then its concept iterator.
struct Layers<'a> {
    storage: &'a MVCCStorage<WALClient>,
    snapshot: &'a ReadSnapshot<WALClient>,
    checkpoint: CheckpointReader,
    cache_mb: u64,
    passes: u64,
    raw_dbs: HashMap<u8, rocksdb::DB>,
    iterator_pool: IteratorPool,
}

impl Layers<'_> {
    fn scan<const N: usize>(
        &mut self,
        name: &str,
        prefix: StorageKey<'static, N>,
        keys: u64,
        concept_items: u64,
        concept_scan: impl FnMut() -> u64,
    ) {
        let prefix = StorageKey::<BUFFER_KEY_INLINE>::Array(StorageKeyArray::new_raw(
            prefix.keyspace_id(),
            ByteArray::copy(prefix.bytes()),
        ));
        let prefix_bytes = prefix.bytes().to_vec();
        let keyspace = EncodingKeyspace::iter().find(|keyspace| keyspace.id() == prefix.keyspace_id()).unwrap();
        let (checkpoint, cache_mb) = (&self.checkpoint, self.cache_mb);
        let raw_db = self.raw_dbs.entry(keyspace.id().0).or_insert_with(|| {
            let resources = RocksResources::new(ByteSize::mb(cache_mb), ByteSize::mb(64));
            rocksdb::DB::open_for_read_only(
                &keyspace.rocks_configuration(&resources),
                checkpoint.directory.join(keyspace.name()),
                false,
            )
            .unwrap()
        });
        println!("{name}: keyspace {}, prefix {prefix_bytes:02x?}", keyspace.name());
        measure(&format!("{name}/raw"), self.passes, keys, || {
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
        let fixed_width = keyspace_fixed_width(&prefix_bytes);
        measure(&format!("{name}/keyspace"), self.passes, keys, || {
            let range = KeyRange::new_within(prefix.clone(), fixed_width);
            let mut iterator = self.storage.iterate_keyspace_range(&self.iterator_pool, range, StorageCounters::DISABLED);
            let mut count = 0;
            while let Some(item) = iterator.next() {
                black_box(item.unwrap());
                count += 1;
            }
            count
        });
        measure(&format!("{name}/snapshot"), self.passes, keys, || {
            let range = KeyRange::new_within(prefix.clone(), fixed_width);
            let mut iterator = self.snapshot.iterate_range(&range, StorageCounters::DISABLED);
            let mut count = 0;
            while let Some(item) = iterator.next() {
                black_box(item.unwrap());
                count += 1;
            }
            count
        });
        measure(&format!("{name}/concept"), self.passes, concept_items, concept_scan);
    }
}

/// Whether the keys under this prefix have a fixed width, by the prefix byte that starts every key.
fn keyspace_fixed_width(prefix: &[u8]) -> bool {
    encoding::layout::prefix::Prefix::from_prefix_id(encoding::layout::prefix::PrefixID::new(prefix[0]))
        .unwrap()
        .fixed_width_keys()
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let persons = arg(&args, 1, 5_000_000).next_multiple_of(10);
    let threads = arg(&args, 2, 8);
    let batch = arg(&args, 3, 5_000).max(2);
    let passes = arg(&args, 4, 3).max(1);
    let cache_mb = arg(&args, 5, 4096);
    let settle_secs = arg(&args, 6, 30);
    let distinct = persons / 10;

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

    load(&shared, persons, threads, batch);

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
    let schema = read_schema(&snapshot, &shared.type_manager);
    let mut layers = Layers {
        storage: &storage,
        snapshot: &snapshot,
        checkpoint,
        cache_mb,
        passes,
        raw_dbs: HashMap::new(),
        iterator_pool: IteratorPool::new(),
    };
    let string_category = ValueType::String.category();
    let person_type = ObjectType::Entity(schema.person);

    layers.scan("has", ThingEdgeHas::prefix(), 2 * persons, 2 * persons, || {
        let mut count = 0;
        for item in thing_manager.get_has(&snapshot, StorageCounters::DISABLED) {
            let (has, repetitions) = item.unwrap();
            black_box((has.owner(), has.attribute()));
            count += repetitions;
        }
        count
    });
    for (name, attribute_type) in [("has-rev-nick", schema.nick), ("has-rev-bio", schema.bio)] {
        let prefix = ThingEdgeHasReverse::prefix_from_attribute_type(string_category, attribute_type.vertex().type_id_());
        layers.scan(name, prefix, persons, persons, || {
            let mut count = 0;
            for item in thing_manager.get_has_reverse(&snapshot, attribute_type, StorageCounters::DISABLED).unwrap() {
                let (has, repetitions) = item.unwrap();
                black_box((has.owner(), has.attribute()));
                count += repetitions;
            }
            count
        });
    }
    layers.scan("links", ThingEdgeLinks::prefix(), persons, persons, || {
        let mut count = 0;
        for item in thing_manager.get_links(&snapshot, StorageCounters::DISABLED) {
            let (links, repetitions) = item.unwrap();
            black_box((links.relation(), links.player(), links.role_type()));
            count += repetitions;
        }
        count
    });
    layers.scan("links-rev", ThingEdgeLinks::prefix_reverse(), persons, persons, || {
        let mut count = 0;
        let range = person_type..=person_type;
        for item in thing_manager.get_links_reverse_by_player_type_range(&snapshot, &range, StorageCounters::DISABLED) {
            let (links, repetitions) = item.unwrap();
            black_box((links.relation(), links.player(), links.role_type()));
            count += repetitions;
        }
        count
    });
    let indexed_prefix = ThingEdgeIndexedRelation::prefix_relation_type(schema.friendship.vertex().type_id_());
    layers.scan("indexed", indexed_prefix, persons, persons, || {
        let mut count = 0;
        let range = person_type..=person_type;
        let iterator = thing_manager
            .get_indexed_relations_in(&snapshot, schema.friendship, range, StorageCounters::DISABLED)
            .unwrap();
        for item in iterator {
            let (players, repetitions) = item.unwrap();
            black_box(players);
            count += repetitions;
        }
        count
    });
    let entity_prefix =
        ObjectVertex::build_prefix_type(Prefix::VertexEntity, schema.person.vertex().type_id_(), ObjectVertex::KEYSPACE);
    layers.scan("entities", entity_prefix, persons, persons, || {
        let mut count = 0;
        for entity in thing_manager.get_entities_in(&snapshot, schema.person, StorageCounters::DISABLED) {
            black_box(entity.unwrap().vertex());
            count += 1;
        }
        count
    });
    let attribute_prefix = AttributeVertex::build_prefix_type(
        Prefix::VertexAttribute,
        schema.nick.vertex().type_id_(),
        AttributeVertex::keyspace_for_category(string_category),
    );
    layers.scan("nick-attrs", attribute_prefix, distinct, distinct, || {
        let mut count = 0;
        for attribute in thing_manager.get_attributes_in(&snapshot, schema.nick, StorageCounters::DISABLED).unwrap() {
            let attribute: Attribute = attribute.unwrap();
            black_box(attribute.vertex());
            count += 1;
        }
        count
    });
}
