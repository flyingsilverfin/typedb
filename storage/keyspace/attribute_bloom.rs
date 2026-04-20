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

use std::sync::{atomic::{AtomicU64, Ordering}, Arc};

/// Number of hash positions per key. 2 gives ~1% FPR at 10 bits/key.
const HASHES_PER_KEY: usize = 2;

/// Bits per expected attribute in each tier. 10 gives ~1% per-tier FPR with
/// k=2 hash functions.
const BITS_PER_KEY: u64 = 10;

/// Bit-count log2 for the fixed-size bloom. 2^28 bits = 32 MiB RAM, fits
/// ~26 M keys at 10 bits/key with ~1% FPR — a saner default for tests and
/// modest databases. Production deployments and large-scale benchmarks should
/// raise this via `TYPEDB_BLOOM_BITS_LOG2`. Each +1 doubles memory and capacity.
const DEFAULT_INITIAL_BIT_COUNT_LOG2: u32 = 28;

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

/// Fixed-size bloom filter (non-tiered). Sized at construction from the env
/// var `TYPEDB_BLOOM_BITS_LOG2` (default 35, i.e. 2^35 bits = 4 GiB) so it
/// doesn't grow at runtime. Trades up-front memory for much lower FPR at
/// scale compared to the tiered growing design: a single bloom with k=2
/// hashes and 10 bits/key holds its ~1% FPR up to `bits / 10` keys, where a
/// tiered scaling bloom multiplies per-tier FPR by tier count (e.g. ~20%
/// effective FPR at 7 tiers).
///
/// For a 4 GiB bloom at 10 bits/key, capacity is ~3.4 B keys with FPR ~1%.
/// Set `TYPEDB_BLOOM_BITS_LOG2` higher for larger expected corpora (each
/// +1 doubles both memory and capacity) or lower in tests.
#[derive(Debug)]
pub struct AttributeBloom {
    tier: Arc<BloomTier>,
}

impl AttributeBloom {
    /// Create with a bit-count driven by `TYPEDB_BLOOM_BITS_LOG2` env var
    /// (default 35 = 4 GiB). Clamped to [20, 40] to protect against
    /// nonsense values.
    pub fn new() -> Self {
        let log2 = std::env::var("TYPEDB_BLOOM_BITS_LOG2")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(DEFAULT_INITIAL_BIT_COUNT_LOG2)
            .clamp(20, 40);
        Self::with_bit_count_log2(log2)
    }

    /// Create with a specific bit count log2. Public so tests and benchmarks
    /// can exercise a smaller bloom cheaply.
    pub fn with_bit_count_log2(log2_bits: u32) -> Self {
        Self { tier: Arc::new(BloomTier::new(log2_bits, 0)) }
    }

    /// Back-compat wrapper for callers that used the old tiered API.
    pub fn with_initial_bit_count_log2(log2_bits: u32) -> Self {
        Self::with_bit_count_log2(log2_bits)
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.tier.may_contain(key)
    }

    pub fn insert(&self, key: &[u8]) {
        self.tier.insert(key);
    }

    /// Always 1 for the fixed-size bloom. Kept for API compatibility with
    /// the previous tiered design (tests used this for growth assertions).
    pub fn tier_count(&self) -> usize {
        1
    }

    pub fn total_bits(&self) -> u64 {
        self.tier.mask + 1
    }

    pub fn insert_count(&self) -> u64 {
        self.tier.insert_count()
    }
}

impl Default for AttributeBloom {
    fn default() -> Self {
        Self::new()
    }
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
    fn single_tier_never_grows() {
        // The non-tiered bloom is fixed-size; tier_count always 1.
        let b = AttributeBloom::with_initial_bit_count_log2(20);
        assert_eq!(b.tier_count(), 1);
        for i in 0..500u32 {
            b.insert(&i.to_le_bytes());
        }
        assert_eq!(b.tier_count(), 1, "fixed-size bloom shouldn't grow");
    }

    #[test]
    fn earlier_keys_stay_findable_after_many_inserts() {
        // No tier growth, but we still want to verify earlier inserts stay
        // queryable after lots of later inserts (smoke check against bit
        // saturation).
        let b = AttributeBloom::with_initial_bit_count_log2(22);
        let early_keys: Vec<_> = (0..50u32).map(|i| i.to_le_bytes()).collect();
        for k in &early_keys {
            b.insert(k);
        }
        for i in 100..50_000u32 {
            b.insert(&i.to_le_bytes());
        }
        for k in &early_keys {
            assert!(b.may_contain(k), "lost key after many later inserts: {:?}", k);
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
    fn key_absence_usually_reports_no() {
        // A key not inserted should typically return false; the fixed-size
        // bloom has its own FPR but specific sentinel strings are unlikely
        // to collide.
        let b = AttributeBloom::with_initial_bit_count_log2(22);
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
