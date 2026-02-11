/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
pub(crate) mod iterator;

use std::{
    borrow::Borrow,
    collections::BTreeMap,
    path::Path,
    sync::{Arc, RwLock},
};

use bytes::{byte_array::ByteArray, Bytes};
use error::typedb_error;
use primitive::key_range::KeyRange;
use resource::{
    constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE},
    profile::StorageCounters,
};

use crate::{
    keyspaces::{KeyspaceId, KeyspaceSet, Keyspaces, KeyspacesError},
    memory::iterator::InMemoryRangeIterator,
    KVStore, KVStoreError, KVStoreID,
};

pub struct InMemoryKVStore {
    name: &'static str,
    id: KVStoreID,
    data: Arc<RwLock<BTreeMap<ByteArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>>>>,
}

impl InMemoryKVStore {
    fn new(name: &'static str, id: KVStoreID) -> Self {
        Self { name, id, data: Arc::new(RwLock::new(BTreeMap::new())) }
    }

    pub fn open_keyspaces<KS: KeyspaceSet>() -> Result<Keyspaces, KeyspacesError> {
        let mut keyspaces = Keyspaces::new();
        for keyspace in KS::iter() {
            keyspaces.validate_new_keyspace(keyspace)?;
            let kv = InMemoryKVStore::new(keyspace.name(), keyspace.id().into());
            keyspaces.keyspaces.push(KVStore::InMemory(kv));
            keyspaces.index[keyspace.id().0 as usize] = Some(KeyspaceId(keyspaces.keyspaces.len() as u8 - 1));
        }
        Ok(keyspaces)
    }

    pub fn id(&self) -> KVStoreID {
        self.id
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn KVStoreError>> {
        let mut data = self.data.write().unwrap();
        data.insert(ByteArray::copy(key), ByteArray::copy(value));
        Ok(())
    }

    pub fn get<M, V>(&self, key: &[u8], mut mapper: M) -> Result<Option<V>, Box<dyn KVStoreError>>
    where
        M: FnMut(&[u8]) -> V,
    {
        let data = self.data.read().unwrap();
        Ok(data.get(key).map(|value| mapper(value)))
    }

    pub fn get_prev<M, T>(&self, key: &[u8], mut mapper: M) -> Option<T>
    where
        M: FnMut(&[u8], &[u8]) -> T,
    {
        let data = self.data.read().unwrap();
        data.range(..=ByteArray::<BUFFER_KEY_INLINE>::copy(key)).next_back().map(|(k, v)| mapper(k, v))
    }

    pub fn iterate_range<const PREFIX_INLINE_SIZE: usize>(
        &self,
        range: &KeyRange<Bytes<'_, PREFIX_INLINE_SIZE>>,
        storage_counters: StorageCounters,
    ) -> InMemoryRangeIterator {
        InMemoryRangeIterator::new(&self.data.read().unwrap(), range, storage_counters)
    }

    pub fn write<K, V>(&self, kv_iterator: impl Iterator<Item = (K, V)>) -> Result<(), Box<dyn KVStoreError>>
    where
        K: Borrow<[u8]>,
        V: Borrow<[u8]>,
    {
        let mut data = self.data.write().unwrap();
        kv_iterator.for_each(|(k, v)| {
            data.insert(ByteArray::copy(k.borrow()), ByteArray::copy(v.borrow()));
        });
        Ok(())
    }

    pub fn checkpoint(&self, _checkpoint_dir: &Path) -> Result<(), Box<dyn KVStoreError>> {
        Ok(())
    }

    pub fn delete(self) -> Result<(), Box<dyn KVStoreError>> {
        Ok(())
    }

    pub fn reset(&mut self) -> Result<(), Box<dyn KVStoreError>> {
        let mut data = self.data.write().unwrap();
        data.clear();
        Ok(())
    }

    pub fn estimate_size_in_bytes(&self) -> Result<u64, Box<dyn KVStoreError>> {
        let data = self.data.read().unwrap();
        let total: u64 = data.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum();
        Ok(total)
    }

    pub fn estimate_key_count(&self) -> Result<u64, Box<dyn KVStoreError>> {
        let data = self.data.read().unwrap();
        Ok(data.len() as u64)
    }
}

impl std::fmt::Debug for InMemoryKVStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryKVStore").field("name", &self.name).field("id", &self.id).finish_non_exhaustive()
    }
}

typedb_error! {
    pub InMemoryKVError(component = "InMemory KV error", prefix = "MKV") {
        Write(1, "InMemory KV error writing to kv store {name}.", name: &'static str),
    }
}

impl std::fmt::Display for InMemoryKVError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use error::TypeDBError;
        write!(f, "{}", self.format_code_and_description())
    }
}

impl std::error::Error for InMemoryKVError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl KVStoreError for InMemoryKVError {}
