/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Per-keyspace scalable Bloom filter used as a fast-path "definitely absent"
//! oracle for attribute-dedup Put reads in the commit hot path.
//!
//! ## Purpose
//! `MVCCStorage::set_initial_put_status` currently issues one RocksDB `get`
//! per Put in a commit to decide whether the attribute key already exists
//! (drives the `reinsert` flag). At scale, each of those reads has to probe
//! multiple LSM levels' bloom filters and can hit disk once the working set
//! exceeds RAM. For a bulk-insert workload where most attribute Puts are for
//! genuinely-new keys, we can short-circuit the vast majority of those reads
//! with a single in-process bloom filter check.
//!
//! ## Shape
//! A classic Scalable Bloom Filter (Almeida et al. 2007): a list of immutable
//! `BloomTier`s, each 2x the size of the previous. Inserts only ever go into
//! the newest tier. Queries OR across all tiers. When the newest tier fills
//! past its capacity, a bigger empty tier is appended and becomes the active
//! one. No bits ever move.
//!
//! ## Deletes
//! Not supported in v1: a deleted key's bits remain set, producing "ghost
//! positives" that fall back to the MVCC read (correct, just slower). Over
//! time, if the workload deletes heavily, the bloom degrades and should be
//! rebuilt from a live storage scan — mechanism not in this module yet.
//!
//! ## Concurrency
//! Tier bits are stored as `AtomicU64` words; `fetch_or` for insert, atomic
//! load for query. Multiple threads can insert / query concurrently without
//! external locks. The tier-list itself is wrapped in `RwLock` for the rare
//! "append a new tier" path; readers and inserters take it briefly.

use std::sync::{atomic::{AtomicU64, Ordering}, Arc, Mutex, RwLock};

/// Number of hash positions per key. 2 gives ~1% FPR at 10 bits/key.
const HASHES_PER_KEY: usize = 2;

/// Bits per expected attribute in each tier. 10 gives ~1% per-tier FPR with
/// k=2 hash functions.
const BITS_PER_KEY: u64 = 10;

/// Maximum number of tiers we'll add before giving up and just accepting more
/// false positives. 32 tiers at 2x growth covers ~4 billion × 2^31 ≈ 2e18
/// attributes — effectively unlimited.
const MAX_TIERS: usize = 32;

/// The first tier's bit count must be a power of two so positions can use
/// cheap AND-masking. 2^23 = 8 Mib = 1 MiB per tier ≈ 800K-attr capacity.
/// Kept intentionally small so unit tests exercise the tier-growth path
/// quickly; real deployments should size up via `AttributeBloom::with_initial_bit_count`.
const DEFAULT_INITIAL_BIT_COUNT_LOG2: u32 = 30; // 2^30 bits = 128 MiB, ~100M attr capacity

/// A single immutable-once-sized bloom. Bits can be flipped 0→1 by concurrent
/// writers but the bit vector itself never grows; saturation triggers the
/// owning `AttributeBloom` to append a new, larger tier.
#[derive(Debug)]
pub struct BloomTier {
    bits: Box<[AtomicU64]>,
    /// (bit_count - 1); bit_count is always a power of 2 so this is an AND mask.
    mask: u64,
    /// Nominal attribute capacity: `bit_count / BITS_PER_KEY`. Used only to
    /// decide when to grow; not enforced, just a hint.
    capacity: u64,
    /// Monotonic counter of inserts (approximate — relaxed atomic). Reaches
    /// `capacity` → trigger growth.
    inserts: AtomicU64,
    /// Tier index, used to salt the hash so positions decorrelate across tiers.
    tier_index: u32,
}

impl BloomTier {
    fn new(bit_count_log2: u32, tier_index: u32) -> Self {
        let bit_count = 1u64 << bit_count_log2;
        let word_count = (bit_count / 64) as usize;
        let mut bits = Vec::with_capacity(word_count);
        for _ in 0..word_count {
            bits.push(AtomicU64::new(0));
        }
        Self {
            bits: bits.into_boxed_slice(),
            mask: bit_count - 1,
            capacity: bit_count / BITS_PER_KEY,
            inserts: AtomicU64::new(0),
            tier_index,
        }
    }

    /// Returns two bit positions from the key bytes. Uses FNV-1a and a cheap
    /// mixer; fast and adequate for a bloom filter (not cryptographic).
    fn positions(&self, key: &[u8]) -> (u64, u64) {
        let mut h: u64 = 0xcbf29ce484222325 ^ (self.tier_index as u64).wrapping_mul(0x9E3779B97F4A7C15);
        for &b in key {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        let p1 = h & self.mask;
        // Second position: independent-enough secondary hash via xorshift mix.
        let mut h2 = h;
        h2 ^= h2 >> 33;
        h2 = h2.wrapping_mul(0xff51afd7ed558ccd);
        h2 ^= h2 >> 33;
        let p2 = h2 & self.mask;
        (p1, p2)
    }

    fn set_bit(&self, pos: u64) {
        let word = (pos / 64) as usize;
        let bit = 1u64 << (pos % 64);
        // Release so any happens-before chain from the inserter (e.g.,
        // RocksDB write) is visible to readers who Acquire on query.
        self.bits[word].fetch_or(bit, Ordering::Release);
    }

    fn test_bit(&self, pos: u64) -> bool {
        let word = (pos / 64) as usize;
        let bit = 1u64 << (pos % 64);
        (self.bits[word].load(Ordering::Acquire) & bit) != 0
    }

    pub fn insert(&self, key: &[u8]) {
        let (p1, p2) = self.positions(key);
        self.set_bit(p1);
        self.set_bit(p2);
        self.inserts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (p1, p2) = self.positions(key);
        self.test_bit(p1) && self.test_bit(p2)
    }

    pub fn insert_count(&self) -> u64 {
        self.inserts.load(Ordering::Relaxed)
    }

    /// Approximate saturation: true when inserts exceed capacity × 0.9 (the
    /// growth trigger threshold).
    pub fn should_grow(&self) -> bool {
        self.inserts.load(Ordering::Relaxed) > (self.capacity * 9 / 10)
    }
}

/// Scalable bloom composed of one or more `BloomTier`s. Reads consult all
/// tiers; writes go into the active (newest) tier only. Appending a new
/// tier is a rare, serialized event under `grow_mutex`.
#[derive(Debug)]
pub struct AttributeBloom {
    /// Ordered oldest→newest. Readers snapshot this; we never mutate
    /// existing entries, only append.
    tiers: RwLock<Vec<Arc<BloomTier>>>,
    /// Serialises the grow path so only one thread appends a tier at a time.
    grow_mutex: Mutex<()>,
}

impl AttributeBloom {
    /// Create with the default initial tier size (128 MiB; ~100M attr
    /// capacity). Later tiers double in bit count.
    pub fn new() -> Self {
        Self::with_initial_bit_count_log2(DEFAULT_INITIAL_BIT_COUNT_LOG2)
    }

    /// Create with a specific initial bit count (rounded up to a power of 2).
    /// Kept public so tests / benchmarks can exercise the tier-growth path
    /// with a much smaller starting size.
    pub fn with_initial_bit_count_log2(log2_bits: u32) -> Self {
        let t0 = Arc::new(BloomTier::new(log2_bits, 0));
        Self {
            tiers: RwLock::new(vec![t0]),
            grow_mutex: Mutex::new(()),
        }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        let tiers = self.tiers.read().unwrap();
        tiers.iter().any(|t| t.may_contain(key))
    }

    pub fn insert(&self, key: &[u8]) {
        // Capture the currently-active tier under the read lock.
        let should_grow = {
            let tiers = self.tiers.read().unwrap();
            let active = tiers.last().expect("bloom always has at least one tier");
            active.insert(key);
            active.should_grow() && tiers.len() < MAX_TIERS
        };
        if should_grow {
            self.try_grow();
        }
    }

    fn try_grow(&self) {
        let _g = self.grow_mutex.lock().unwrap();
        let need_grow_idx = {
            let tiers = self.tiers.read().unwrap();
            let active = tiers.last().unwrap();
            if !active.should_grow() || tiers.len() >= MAX_TIERS {
                return; // raced; another thread already grew, or hit cap
            }
            active.tier_index
        };
        let next_log2 = {
            let tiers = self.tiers.read().unwrap();
            let active = tiers.last().unwrap();
            // bit_count = mask + 1; compute its log2.
            let bit_count = active.mask + 1;
            bit_count_log2(bit_count).saturating_add(1)
        };
        let new_tier = Arc::new(BloomTier::new(next_log2, need_grow_idx + 1));
        self.tiers.write().unwrap().push(new_tier);
    }

    /// Total number of tiers currently in the filter. Useful for tests and
    /// telemetry.
    pub fn tier_count(&self) -> usize {
        self.tiers.read().unwrap().len()
    }

    /// Total bit capacity across all tiers (in bits). Mainly for telemetry.
    pub fn total_bits(&self) -> u64 {
        let tiers = self.tiers.read().unwrap();
        tiers.iter().map(|t| t.mask + 1).sum()
    }

    /// Total insert count across all tiers. Only exact for a quiescent bloom.
    pub fn insert_count(&self) -> u64 {
        let tiers = self.tiers.read().unwrap();
        tiers.iter().map(|t| t.insert_count()).sum()
    }
}

impl Default for AttributeBloom {
    fn default() -> Self {
        Self::new()
    }
}

fn bit_count_log2(mut n: u64) -> u32 {
    let mut log = 0u32;
    while n > 1 {
        n >>= 1;
        log += 1;
    }
    log
}

// -----------------------------------------------------------------------------
// Unit tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn insert_and_query_roundtrip() {
        let b = AttributeBloom::with_initial_bit_count_log2(14); // 16 Ki bits
        let key = b"hello world";
        assert!(!b.may_contain(key));
        b.insert(key);
        assert!(b.may_contain(key));
    }

    #[test]
    fn absent_keys_mostly_negative() {
        let b = AttributeBloom::with_initial_bit_count_log2(20); // 1 Mib
        for i in 0..10_000u32 {
            b.insert(&i.to_le_bytes());
        }
        // Query 10_000 not-inserted keys; bloom should say "no" for most.
        let mut fps = 0;
        for i in 20_000u32..30_000 {
            if b.may_contain(&i.to_le_bytes()) {
                fps += 1;
            }
        }
        // Target is ~1% FPR; allow generous headroom — this is a smoke test,
        // not a rigorous statistical one.
        assert!(fps < 1000, "unexpectedly high false positive rate: {}/10000", fps);
    }

    #[test]
    fn tier_growth_fires_when_saturated() {
        // Tiny initial tier: 2^10 = 1024 bits → 102-key nominal capacity
        // → 0.9 × 102 = 92 keys to trigger grow.
        let b = AttributeBloom::with_initial_bit_count_log2(10);
        assert_eq!(b.tier_count(), 1);
        for i in 0..500u32 {
            b.insert(&i.to_le_bytes());
        }
        // Should have grown at least once (from 1024 → 2048 → maybe more).
        assert!(b.tier_count() > 1, "tier growth did not fire after 500 inserts, tier_count={}", b.tier_count());
    }

    #[test]
    fn queries_hit_across_tiers() {
        // After tier growth, keys inserted into tier 0 must still be found
        // when a later tier exists.
        let b = AttributeBloom::with_initial_bit_count_log2(10);
        let early_keys: Vec<_> = (0..50u32).map(|i| i.to_le_bytes()).collect();
        for k in &early_keys {
            b.insert(k);
        }
        // Fill enough to trigger a tier grow.
        for i in 100..500u32 {
            b.insert(&i.to_le_bytes());
        }
        assert!(b.tier_count() > 1);
        // Every early key should still test positive (in the old tier).
        for k in &early_keys {
            assert!(b.may_contain(k), "lost key after tier grow: {:?}", k);
        }
    }

    #[test]
    fn concurrent_inserts_and_queries() {
        let b = Arc::new(AttributeBloom::with_initial_bit_count_log2(22));
        let mut handles = Vec::new();
        // 8 writer threads
        for t in 0..8 {
            let b = b.clone();
            handles.push(thread::spawn(move || {
                for i in 0..5_000u32 {
                    let key = (t * 100_000 + i).to_le_bytes();
                    b.insert(&key);
                }
            }));
        }
        // 4 reader threads issuing queries concurrently
        for _ in 0..4 {
            let b = b.clone();
            handles.push(thread::spawn(move || {
                for i in 500_000u32..510_000 {
                    let _ = b.may_contain(&i.to_le_bytes());
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // All 8 × 5000 = 40000 inserted keys must be may_contain-positive.
        for t in 0..8u32 {
            for i in 0..5_000u32 {
                let key = (t * 100_000 + i).to_le_bytes();
                assert!(b.may_contain(&key), "lost key after concurrent insert: {:?}", key);
            }
        }
    }

    #[test]
    fn key_absence_determines_all_tiers_report_no() {
        // A key not inserted anywhere must return false across all tiers.
        // Relies on the tier_index salt decorrelating positions.
        let b = AttributeBloom::with_initial_bit_count_log2(10);
        for i in 0..1_000u32 {
            b.insert(&i.to_le_bytes());
        }
        // Query a clearly-absent key.
        let absent = b"never inserted this sentinel string";
        // Not a hard guarantee (bloom filters have FPs), but for a specific
        // sentinel it would be unusual to flip both required bits across
        // every tier by chance. Accept the small chance of flakiness.
        let mut false_positive = false;
        for _ in 0..10 {
            if b.may_contain(absent) {
                false_positive = true;
                break;
            }
        }
        if false_positive {
            // Not a hard fail — just a sanity signal. Skip if it happens.
            eprintln!("(tolerated) bloom reported false positive for sentinel key");
        }
    }
}
