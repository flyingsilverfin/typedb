/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
pub mod iterator;
pub mod keyspaces;
pub mod memory;
pub mod rocks;

use std::{borrow::Borrow, path::Path};

use bytes::Bytes;
use error::TypeDBError;
use primitive::key_range::KeyRange;
use resource::profile::StorageCounters;

use crate::{
    iterator::KVRangeIterator,
    keyspaces::{KeyspaceSet, Keyspaces, KeyspacesError},
    memory::InMemoryKVStore,
    rocks::RocksKVStore,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KVBackend {
    RocksDB,
    InMemory,
}

#[derive(Debug)]
pub enum KVStore {
    RocksDB(RocksKVStore),
    InMemory(InMemoryKVStore),
}

impl KVStore {
    pub fn open_keyspaces<KS: KeyspaceSet>(
        storage_dir: &Path,
        backend: KVBackend,
    ) -> Result<Keyspaces, KeyspacesError> {
        match backend {
            KVBackend::RocksDB => RocksKVStore::open_keyspaces::<KS>(storage_dir),
            KVBackend::InMemory => InMemoryKVStore::open_keyspaces::<KS>(),
        }
    }

    pub fn id(&self) -> KVStoreID {
        match self {
            Self::RocksDB(s) => s.id(),
            Self::InMemory(s) => s.id(),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::RocksDB(s) => s.name(),
            Self::InMemory(s) => s.name(),
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn TypeDBError>> {
        match self {
            Self::RocksDB(s) => s.put(key, value),
            Self::InMemory(s) => s.put(key, value),
        }
    }

    pub fn get<M, V>(&self, key: &[u8], mapper: M) -> Result<Option<V>, Box<dyn TypeDBError>>
    where
        M: FnMut(&[u8]) -> V,
    {
        match self {
            Self::RocksDB(s) => s.get(key, mapper),
            Self::InMemory(s) => s.get(key, mapper),
        }
    }

    pub fn get_prev<M, T>(&self, key: &[u8], mapper: M) -> Option<T>
    where
        M: FnMut(&[u8], &[u8]) -> T,
    {
        match self {
            Self::RocksDB(s) => s.get_prev(key, mapper),
            Self::InMemory(s) => s.get_prev(key, mapper),
        }
    }

    pub fn iterate_range<const PREFIX_INLINE_SIZE: usize>(
        &self,
        range: &KeyRange<Bytes<'_, PREFIX_INLINE_SIZE>>,
        storage_counters: StorageCounters,
    ) -> KVRangeIterator {
        match self {
            Self::RocksDB(s) => KVRangeIterator::RocksDB(s.iterate_range(range, storage_counters)),
            Self::InMemory(s) => KVRangeIterator::InMemory(s.iterate_range(range, storage_counters)),
        }
    }

    pub fn write<K, V>(&self, kv_iterator: impl Iterator<Item = (K, V)>) -> Result<(), Box<dyn TypeDBError>>
    where
        K: Borrow<[u8]>,
        V: Borrow<[u8]>,
    {
        match self {
            Self::RocksDB(s) => s.write(kv_iterator),
            Self::InMemory(s) => s.write(kv_iterator),
        }
    }

    pub fn checkpoint(&self, checkpoint_dir: &Path) -> Result<(), Box<dyn TypeDBError>> {
        match self {
            Self::RocksDB(s) => s.checkpoint(checkpoint_dir),
            Self::InMemory(s) => s.checkpoint(checkpoint_dir),
        }
    }

    pub fn delete(self) -> Result<(), Box<dyn TypeDBError>> {
        match self {
            Self::RocksDB(s) => s.delete(),
            Self::InMemory(s) => s.delete(),
        }
    }

    pub fn reset(&mut self) -> Result<(), Box<dyn TypeDBError>> {
        match self {
            Self::RocksDB(s) => s.reset(),
            Self::InMemory(s) => s.reset(),
        }
    }

    pub fn estimate_size_in_bytes(&self) -> Result<u64, Box<dyn TypeDBError>> {
        match self {
            Self::RocksDB(s) => s.estimate_size_in_bytes(),
            Self::InMemory(s) => s.estimate_size_in_bytes(),
        }
    }

    pub fn estimate_key_count(&self) -> Result<u64, Box<dyn TypeDBError>> {
        match self {
            Self::RocksDB(s) => s.estimate_key_count(),
            Self::InMemory(s) => s.estimate_key_count(),
        }
    }
}

pub type KVStoreID = usize;
