/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, error::Error, fmt, path::PathBuf};

use resource::internal_database_prefix;
use serde::{Serialize, de::DeserializeOwned};
use tracing::{Level, event};
use uuid::Uuid;

pub const CACHE_DB_NAME_PREFIX: &str = concat!(internal_database_prefix!(), "cache-");

// A single-threaded configurable cache which prioritizes using a simple in-memory storage, but
// spills the excessive data not fitting into the memory requirements over to disk.
#[derive(Debug)]
pub struct SpilloverCache<T: Serialize + DeserializeOwned + Clone> {
    memory_storage: HashMap<String, T>,
    disk_storage_path: PathBuf,
    disk_storage: Option<rocksdb::DB>,
    memory_size_limit: usize,
}

impl<T: Serialize + DeserializeOwned + Clone> SpilloverCache<T> {
    pub fn new(disk_storage_dir: &PathBuf, name_prefix: Option<&str>, memory_size_limit: usize) -> Self {
        assert!(disk_storage_dir.is_dir(), "SpilloverCache requires a disk storage path to a directory!");
        let unique_db_name = Uuid::new_v4().to_string();
        let disk_storage_path =
            disk_storage_dir.join(format!("{}{}{}", CACHE_DB_NAME_PREFIX, name_prefix.unwrap_or(""), unique_db_name));

        SpilloverCache { memory_storage: HashMap::new(), disk_storage_path, disk_storage: None, memory_size_limit }
    }

    pub fn insert(&mut self, key: String, value: T) -> Result<(), CacheError> {
        self.remove(&key)?;
        match self.memory_storage.len() < self.memory_size_limit {
            true => {
                self.memory_storage.insert(key, value);
                Ok(())
            }
            false => self.disk_storage_insert(key, value),
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<T>, CacheError> {
        match self.memory_storage.get(key).cloned() {
            Some(value) => Ok(Some(value)),
            None => self.disk_storage_get(key),
        }
    }

    pub fn remove(&mut self, key: &str) -> Result<(), CacheError> {
        match self.memory_storage.remove(key) {
            Some(_) => Ok(()),
            None => self.disk_storage_remove(key),
        }
    }

    fn disk_storage_insert(&mut self, key: String, value: T) -> Result<(), CacheError> {
        if self.disk_storage.is_none() {
            let rocks_db = rocksdb::DB::open(&Self::rocks_configuration(), &self.disk_storage_path)
                .map_err(|source| CacheError::DiskStorageAccess { source })?;
            self.disk_storage = Some(rocks_db);
        }
        let serialized = bincode::serialize(&value).map_err(|_| CacheError::DiskStorageSerialization {})?;
        self.disk_storage
            .as_mut()
            .unwrap()
            .put(key, serialized)
            .map_err(|source| CacheError::DiskStorageAccess { source })
    }

    fn disk_storage_get(&self, key: &str) -> Result<Option<T>, CacheError> {
        if let Some(disk_storage) = &self.disk_storage {
            if let Some(bytes) = disk_storage.get(key).map_err(|source| CacheError::DiskStorageAccess { source })? {
                return bincode::deserialize(&bytes)
                    .map(|value| Some(value))
                    .map_err(|_| CacheError::DiskStorageDeserialization {});
            }
        }
        Ok(None)
    }

    fn disk_storage_remove(&mut self, key: &str) -> Result<(), CacheError> {
        match &mut self.disk_storage {
            Some(disk_storage) => disk_storage.delete(key).map_err(|source| CacheError::DiskStorageAccess { source }),
            None => Ok(()),
        }
    }

    fn rocks_configuration() -> rocksdb::Options {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options
    }
}

impl<T: Serialize + DeserializeOwned + Clone> Drop for SpilloverCache<T> {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.disk_storage)); // release its files
        if let Err(e) = std::fs::remove_dir_all(&self.disk_storage_path) {
            // Can be cleaned up by the cache's user
            event!(Level::TRACE, "Failed to delete a temporary DB directory {:?}: {e}", self.disk_storage_path);
        }
    }
}

// A single-threaded multi-map with the same policy as `SpilloverCache`: values are appended per key
// in memory until the memory limit (counted in values, not keys) is reached, after which new values
// spill over to disk. Values are only ever taken out of the map all at once, per key.
#[derive(Debug)]
pub struct SpilloverMultiMap<T: Serialize + DeserializeOwned + Clone> {
    memory_storage: HashMap<String, Vec<T>>,
    memory_value_count: usize,
    memory_size_limit: usize,
    disk_storage_path: PathBuf,
    disk_storage: Option<rocksdb::DB>,
    disk_value_count: usize,
    next_disk_sequence: u64,
}

impl<T: Serialize + DeserializeOwned + Clone> SpilloverMultiMap<T> {
    const KEY_LENGTH_PREFIX_SIZE: usize = size_of::<u32>();
    const SEQUENCE_SUFFIX_SIZE: usize = size_of::<u64>();

    pub fn new(disk_storage_dir: &PathBuf, name_prefix: Option<&str>, memory_size_limit: usize) -> Self {
        assert!(disk_storage_dir.is_dir(), "SpilloverMultiMap requires a disk storage path to a directory!");
        let unique_db_name = Uuid::new_v4().to_string();
        let disk_storage_path =
            disk_storage_dir.join(format!("{}{}{}", CACHE_DB_NAME_PREFIX, name_prefix.unwrap_or(""), unique_db_name));
        SpilloverMultiMap {
            memory_storage: HashMap::new(),
            memory_value_count: 0,
            memory_size_limit,
            disk_storage_path,
            disk_storage: None,
            disk_value_count: 0,
            next_disk_sequence: 0,
        }
    }

    pub fn push(&mut self, key: &str, value: T) -> Result<(), CacheError> {
        if self.memory_value_count < self.memory_size_limit {
            self.memory_storage.entry(key.to_owned()).or_default().push(value);
            self.memory_value_count += 1;
            Ok(())
        } else {
            self.disk_storage_push(key, value)
        }
    }

    // Removes and returns every value pushed for the key, from memory and disk alike.
    pub fn take(&mut self, key: &str) -> Result<Vec<T>, CacheError> {
        let mut values = self.memory_storage.remove(key).unwrap_or_default();
        self.memory_value_count -= values.len();
        if self.disk_storage.is_some() {
            self.disk_storage_take(key, &mut values)?;
        }
        Ok(values)
    }

    // The number of values currently held, in memory and on disk.
    pub fn len(&self) -> usize {
        self.memory_value_count + self.disk_value_count
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // The number of distinct keys. Scans the disk storage, so it is meant for reporting, not hot paths.
    pub fn key_count(&self) -> Result<usize, CacheError> {
        let mut count = self.memory_storage.len();
        let Some(disk_storage) = &self.disk_storage else { return Ok(count) };
        let mut previous: Option<Vec<u8>> = None;
        for item in disk_storage.iterator(rocksdb::IteratorMode::Start) {
            let (disk_key, _) = item.map_err(|source| CacheError::DiskStorageAccess { source })?;
            let key_bytes = &disk_key[Self::KEY_LENGTH_PREFIX_SIZE..disk_key.len() - Self::SEQUENCE_SUFFIX_SIZE];
            if previous.as_deref() == Some(key_bytes) {
                continue;
            }
            let in_memory = std::str::from_utf8(key_bytes).is_ok_and(|key| self.memory_storage.contains_key(key));
            if !in_memory {
                count += 1;
            }
            previous = Some(key_bytes.to_vec());
        }
        Ok(count)
    }

    // Disk keys are `[key length: u32][key bytes][sequence: u64]`, so all values of one key are
    // contiguous, distinct keys never share a prefix, and values keep their insertion order.
    fn disk_key_prefix(key: &str) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::KEY_LENGTH_PREFIX_SIZE + key.len() + Self::SEQUENCE_SUFFIX_SIZE);
        bytes.extend_from_slice(&(key.len() as u32).to_be_bytes());
        bytes.extend_from_slice(key.as_bytes());
        bytes
    }

    fn disk_storage_push(&mut self, key: &str, value: T) -> Result<(), CacheError> {
        if self.disk_storage.is_none() {
            let rocks_db = rocksdb::DB::open(&Self::rocks_configuration(), &self.disk_storage_path)
                .map_err(|source| CacheError::DiskStorageAccess { source })?;
            self.disk_storage = Some(rocks_db);
        }
        let serialized = bincode::serialize(&value).map_err(|_| CacheError::DiskStorageSerialization {})?;
        let mut disk_key = Self::disk_key_prefix(key);
        disk_key.extend_from_slice(&self.next_disk_sequence.to_be_bytes());
        self.disk_storage
            .as_ref()
            .unwrap()
            .put_opt(disk_key, serialized, &Self::write_options())
            .map_err(|source| CacheError::DiskStorageAccess { source })?;
        self.next_disk_sequence += 1;
        self.disk_value_count += 1;
        Ok(())
    }

    fn disk_storage_take(&mut self, key: &str, values: &mut Vec<T>) -> Result<(), CacheError> {
        let disk_storage = self.disk_storage.as_ref().unwrap();
        let prefix = Self::disk_key_prefix(key);
        let mut batch = rocksdb::WriteBatch::default();
        let mut taken = 0;
        for item in disk_storage.iterator(rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward)) {
            let (disk_key, bytes) = item.map_err(|source| CacheError::DiskStorageAccess { source })?;
            if !disk_key.starts_with(&prefix) {
                break;
            }
            values.push(bincode::deserialize(&bytes).map_err(|_| CacheError::DiskStorageDeserialization {})?);
            batch.delete(&disk_key);
            taken += 1;
        }
        if taken > 0 {
            disk_storage
                .write_opt(batch, &Self::write_options())
                .map_err(|source| CacheError::DiskStorageAccess { source })?;
            self.disk_value_count -= taken;
        }
        Ok(())
    }

    fn rocks_configuration() -> rocksdb::Options {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options
    }

    // The storage is scratch space that is deleted with the map, so it never needs to be recovered.
    fn write_options() -> rocksdb::WriteOptions {
        let mut options = rocksdb::WriteOptions::default();
        options.disable_wal(true);
        options
    }
}

impl<T: Serialize + DeserializeOwned + Clone> Drop for SpilloverMultiMap<T> {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.disk_storage)); // release its files
        if let Err(e) = std::fs::remove_dir_all(&self.disk_storage_path) {
            // Can be cleaned up by the map's user
            event!(Level::TRACE, "Failed to delete a temporary DB directory {:?}: {e}", self.disk_storage_path);
        }
    }
}

#[derive(Clone, Debug)]
pub enum CacheError {
    DiskStorageAccess { source: rocksdb::Error },
    DiskStorageSerialization,
    DiskStorageDeserialization,
}

impl fmt::Display for CacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CacheError::DiskStorageAccess { source } => write!(f, "Cannot access disk storage, {source}"),
            CacheError::DiskStorageSerialization => write!(f, "Internal error: cannot write data to the disk storage"),
            CacheError::DiskStorageDeserialization => {
                write!(f, "Internal error: disk storage is corrupted and data cannot be read")
            }
        }
    }
}

impl Error for CacheError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CacheError::DiskStorageAccess { source } => Some(source),
            CacheError::DiskStorageSerialization => None,
            CacheError::DiskStorageDeserialization => None,
        }
    }
}

#[cfg(test)]
pub mod tests {
    use test_utils::{TempDir, create_tmp_storage_dir};

    use crate::{SpilloverCache, SpilloverMultiMap};
    macro_rules! put {
        ($cache:ident, $key:literal, $value:literal) => {
            $cache.insert($key.to_owned(), $value.to_owned()).unwrap()
        };
    }
    macro_rules! get {
        ($cache:ident, $key:literal) => {
            $cache.get($key).unwrap().as_ref().map(String::as_str)
        };
    }

    fn create_cache_in_tmpdir() -> (TempDir, SpilloverCache<String>) {
        let tmp_dir = create_tmp_storage_dir();
        let cache: SpilloverCache<String> = SpilloverCache::new(&tmp_dir.as_ref().to_path_buf(), Some("unit_test"), 1);
        (tmp_dir, cache)
    }

    #[test]
    fn test_insert_spillover_duplicates() {
        let (tmp_dir, mut cache) = create_cache_in_tmpdir();
        put!(cache, "key1", "value1");
        assert_eq!(get!(cache, "key1"), Some("value1"));
        put!(cache, "key1", "value2");
        assert_eq!(get!(cache, "key1"), Some("value2"));
    }

    #[test]
    fn test_delete_insert_duplicates() {
        let (tmp_dir, mut cache) = create_cache_in_tmpdir();
        put!(cache, "key1", "value1");
        assert_eq!(get!(cache, "key1"), Some("value1"));

        put!(cache, "key2", "value2_1");
        assert_eq!(cache.get("key2").unwrap().unwrap(), "value2_1");

        cache.remove("key1").unwrap();
        assert_eq!(get!(cache, "key1"), None);

        put!(cache, "key2", "value2_2");
        assert_eq!(get!(cache, "key2"), Some("value2_2"));

        cache.remove("key2").unwrap();
        assert_eq!(get!(cache, "key2"), None);
    }
    fn create_multimap_in_tmpdir(memory_size_limit: usize) -> (TempDir, SpilloverMultiMap<String>) {
        let tmp_dir = create_tmp_storage_dir();
        let map = SpilloverMultiMap::new(&tmp_dir.as_ref().to_path_buf(), Some("unit_test"), memory_size_limit);
        (tmp_dir, map)
    }

    #[test]
    fn multimap_take_returns_values_from_memory_and_disk_in_order() {
        let (_tmp_dir, mut map) = create_multimap_in_tmpdir(2);
        map.push("a", "a1".to_owned()).unwrap();
        map.push("b", "b1".to_owned()).unwrap();
        // memory is full: everything below spills to disk
        map.push("a", "a2".to_owned()).unwrap();
        map.push("c", "c1".to_owned()).unwrap();
        map.push("a", "a3".to_owned()).unwrap();
        assert_eq!(map.len(), 5);
        assert_eq!(map.key_count().unwrap(), 3);

        assert_eq!(map.take("a").unwrap(), vec!["a1", "a2", "a3"]);
        assert_eq!(map.take("a").unwrap(), Vec::<String>::new());
        assert_eq!(map.len(), 2);
        assert_eq!(map.key_count().unwrap(), 2);

        assert_eq!(map.take("c").unwrap(), vec!["c1"]);
        assert_eq!(map.take("b").unwrap(), vec!["b1"]);
        assert!(map.is_empty());
        assert_eq!(map.key_count().unwrap(), 0);
    }

    #[test]
    fn multimap_keys_sharing_a_prefix_stay_separate_on_disk() {
        let (_tmp_dir, mut map) = create_multimap_in_tmpdir(0);
        map.push("ab", "1".to_owned()).unwrap();
        map.push("a", "2".to_owned()).unwrap();
        map.push("abc", "3".to_owned()).unwrap();
        map.push("ab", "4".to_owned()).unwrap();
        assert_eq!(map.key_count().unwrap(), 3);
        assert_eq!(map.take("ab").unwrap(), vec!["1", "4"]);
        assert_eq!(map.take("a").unwrap(), vec!["2"]);
        assert_eq!(map.take("abc").unwrap(), vec!["3"]);
        assert!(map.is_empty());
    }

    #[test]
    fn multimap_without_spillover_never_touches_disk() {
        let (_tmp_dir, mut map) = create_multimap_in_tmpdir(10);
        map.push("k", "v1".to_owned()).unwrap();
        map.push("k", "v2".to_owned()).unwrap();
        assert_eq!(map.key_count().unwrap(), 1);
        assert_eq!(map.take("missing").unwrap(), Vec::<String>::new());
        assert_eq!(map.take("k").unwrap(), vec!["v1", "v2"]);
        assert!(map.is_empty());
    }
}
