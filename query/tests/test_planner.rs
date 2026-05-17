use concept::thing::thing_manager::ThingManager;
use concept::type_::type_manager::TypeManager;
use encoding::graph::definition::definition_key_generator::DefinitionKeyGenerator;
use function::function_manager::FunctionManager;
use query::query_cache::QueryCache;
use query::query_manager::QueryManager;
use resource::profile::CommitProfile;
use std::sync::Arc;
use concept::type_::type_manager::type_cache::TypeCache;
use executor::ExecutionInterrupt;
use executor::pipeline::stage::StageIterator;
use storage::MVCCStorage;
use storage::durability_client::WALClient;
use storage::snapshot::CommittableSnapshot;
use test_utils::{TempDir, init_logging};
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
        // update statistics
        Arc::get_mut(&mut self.thing_manager.statistics()).unwrap().may_synchronise(&self.storage).unwrap();
        // update type manager
        let definition_key_gen = self.type_manager.definition_key_generator();
        let vertex_gen = self.type_manager.type_vertex_generator();
        let cache = Arc::new(TypeCache::new(self.storage(), self.storage.snapshot_watermark()).unwrap());
        self.type_manager = Arc::new(TypeManager::new(definition_key_gen, vertex_gen, Some(cache)));
    }
}

fn setup() -> Context{
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new(Some(Arc::new(QueryCache::new())));
    Context { _tmp_dir, storage, type_manager, function_manager, query_manager, thing_manager }
}

fn define_schema(
    context: &mut Context,
    query: &str,
) {
    let mut snapshot = context.storage.clone().open_snapshot_schema();
    let schema_query = typeql::parse_query(query).unwrap().into_structure().into_schema();
    context.query_manager
        .execute_schema(&mut snapshot, &context.type_manager, &context.thing_manager, &context.function_manager, schema_query, query)
        .unwrap();
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    context.refresh();
}

fn commit_writes(
    context: &mut Context,
    queries: &[String],
) {
    let mut snapshot = context.storage.clone().open_snapshot_write();
    for query in queries {
        let parsed_query = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
        let pipeline = context.query_manager
            .prepare_write_pipeline(snapshot, &context.type_manager, context.thing_manager.clone(), &context.function_manager, &parsed_query, query)
            .unwrap();
        // TODO: verify that this has eagerly already executed, if yes, leave a comment here saying 'executes eagerly'
        let (_iterator, context) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
        snapshot = Arc::into_inner(context.snapshot).unwrap();
    }
    snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
    context.refresh();
}

fn execute_read(
    context: &Context,
    query: &str
) {
    let mut snapshot = Arc::new(context.storage.clone().open_snapshot_read());
    let parsed_query = typeql::parse_query(query).unwrap().into_structure().into_pipeline();
    let pipeline = context.query_manager
        .prepare_read_pipeline(snapshot.clone(), &context.type_manager, context.thing_manager.clone(), &context.function_manager, &parsed_query, query)
        .unwrap();
    // todo later - decide if we want to return query profile or the prepared pipeline (or both?)
    let (iterator, context) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let _ = iterator.collect_owned().unwrap();
}

#[test]
fn has_2_join() {
    let mut context = setup();

    let schema = "define\
      entity owner_1 owns key_1 @key, owns join_attr;\
      entity owner_2 owns key_2 @key, owns join_attr;\
      attribute key_1, value integer;\
      attribute key_2, value integer;\
      attribute join_attr, value integer";
    define_schema(&mut context, schema);

    // TODO: use test-local constants here and above for type labels. Also use instance count constants
    // note: always does connections via key
    let data_spec = DataSpec {
        // load entities, relations, or attributes [optionally] first
        instances: vec![
            InstanceSpec { type_: "owner_1", count: 1, key: Some("key_1") },
            InstanceSpec { type_: "owner_2", count: 1, key: Some("key_2") },
        ],
        // load has (attributes don't have to be inserted before)
        has: vec![
            HasSpec {
                owner_type: "owner_1",
                attr_type: "join_attr",
                count_each: 1,
                count_total: 1,
                attribute_generator: |i| i,
            },
            HasSpec {
                owner_type: "owner_2",
                attr_type: "join_attr",
                count_each: 1,
                count_total: 1,
                attribute_generator: |i| i,
            },
        ],
    };

    let query = "match \
    $e1 isa owner_1, has join_attr $join;\
    $e2 isa owner_2, has join_attr $join;";

    context.query_manager.prepare_read_pipeline()
}
