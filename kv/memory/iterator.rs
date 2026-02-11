/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, collections::BTreeMap, ops::Bound};

use bytes::{byte_array::ByteArray, util::increment, Bytes};
use lending_iterator::{LendingIterator, Seekable};
use primitive::key_range::{KeyRange, RangeEnd, RangeStart};
use resource::{
    constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE},
    profile::StorageCounters,
};

use crate::{
    iterator::{accept_value, ContinueCondition},
    KVStoreError,
};

pub struct InMemoryRangeIterator {
    data: Vec<(ByteArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>)>,
    position: usize,
    continue_condition: ContinueCondition,
    is_finished: bool,
    storage_counters: StorageCounters,
}

impl InMemoryRangeIterator {
    pub(crate) fn new<const INLINE_BYTES: usize>(
        btree: &BTreeMap<ByteArray<BUFFER_KEY_INLINE>, ByteArray<BUFFER_VALUE_INLINE>>,
        range: &KeyRange<Bytes<'_, INLINE_BYTES>>,
        storage_counters: StorageCounters,
    ) -> Self {
        let start_bound: Bound<ByteArray<BUFFER_KEY_INLINE>> = match range.start() {
            RangeStart::Inclusive(bytes) => Bound::Included(ByteArray::copy(bytes.as_ref())),
            RangeStart::ExcludeFirstWithPrefix(bytes) => Bound::Excluded(ByteArray::copy(bytes.as_ref())),
            RangeStart::ExcludePrefix(bytes) => {
                let mut cloned: ByteArray<BUFFER_KEY_INLINE> = ByteArray::copy(bytes.as_ref());
                cloned.increment().unwrap();
                Bound::Included(cloned)
            }
        };

        let end_bound: Bound<ByteArray<BUFFER_KEY_INLINE>> = match range.end() {
            RangeEnd::WithinStartAsPrefix => {
                let mut end = ByteArray::<BUFFER_KEY_INLINE>::copy(range.start().get_value().as_ref());
                increment(&mut end).unwrap();
                Bound::Excluded(end)
            }
            RangeEnd::EndPrefixInclusive(end) => {
                let mut end = ByteArray::<BUFFER_KEY_INLINE>::copy(end.as_ref());
                increment(&mut end).unwrap();
                Bound::Excluded(end)
            }
            RangeEnd::EndPrefixExclusive(end) => Bound::Excluded(ByteArray::copy(end.as_ref())),
            RangeEnd::Unbounded => Bound::Unbounded,
        };

        let data: Vec<_> = btree.range((start_bound, end_bound)).map(|(k, v)| (k.clone(), v.clone())).collect();

        let continue_condition = match range.end() {
            RangeEnd::WithinStartAsPrefix => {
                ContinueCondition::ExactPrefix(ByteArray::from(&**range.start().get_value()))
            }
            RangeEnd::EndPrefixInclusive(end) => ContinueCondition::EndPrefixInclusive(ByteArray::from(&**end)),
            RangeEnd::EndPrefixExclusive(end) => ContinueCondition::EndPrefixExclusive(ByteArray::from(&**end)),
            RangeEnd::Unbounded => ContinueCondition::Always,
        };

        storage_counters.increment_raw_seek();

        Self { data, position: 0, continue_condition, is_finished: false, storage_counters }
    }
}

impl LendingIterator for InMemoryRangeIterator {
    type Item<'a>
        = Result<(&'a [u8], &'a [u8]), Box<dyn KVStoreError>>
    where
        Self: 'a;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.is_finished || self.position >= self.data.len() {
            self.is_finished = true;
            return None;
        }

        let (ref key, ref value) = self.data[self.position];
        let result: Result<(&[u8], &[u8]), Box<dyn KVStoreError>> = Ok((&key[..], &value[..]));

        if !accept_value(&self.continue_condition, &result) {
            self.is_finished = true;
            return None;
        }

        self.position += 1;
        self.storage_counters.increment_raw_advance();
        Some(result)
    }
}

impl Seekable<[u8]> for InMemoryRangeIterator {
    fn seek(&mut self, key: &[u8]) {
        if self.is_finished {
            return;
        }
        let remaining = &self.data[self.position..];
        let offset = remaining.partition_point(|(k, _)| &k[..] < key);
        self.position += offset;
        self.storage_counters.increment_raw_seek();
    }

    fn compare_key(&self, item: &Self::Item<'_>, key: &[u8]) -> Ordering {
        crate::iterator::compare_key(item, key)
    }
}
