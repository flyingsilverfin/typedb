# WAL design notes

Orientation to the WAL append path as it stands on the `attribute-bloom`
branch. Covers the invariants, the concurrency model, and the reasoning
behind the defensive code — intended to be read alongside `wal.rs`.

## The path at a glance

```
sequenced_write(record):
  compress payload (off-lock)

  [allocation_lock held — microseconds]
    seq    = next_sequence_number_arc.fetch_add(1)
    offset = active_file.state.next_offset.fetch_add(size)
    if offset + size > MAX_WAL_FILE_SIZE:
      rotate (still under allocation_lock)
      retry
    slot   = active_file.state.claim_slot(offset, size)
  [allocation_lock released]

  pwrite(active_file.fd, payload, offset)        # lock-free syscall
  active_file.state.complete_slot(slot)          # advances completed_end
  request fsync (wakes fsync thread via Condvar)
```

## Invariants

The reader path (iteration, recovery, stats-sync) depends on these. The
writer path has to preserve every one of them.

1. **Seq monotonicity in file layout.** Records appear in a WAL file in
   increasing sequence-number order. Guaranteed by the allocation_lock
   atomically pairing seq and offset (one lock acquire → one `(seq, offset)`
   tuple → one pwrite position).

2. **Contiguous prefix.** Every byte in `[0, completed_end)` is part of a
   complete, validly-written record. Guaranteed by the in-flight-slot
   tracking: `completed_end` only advances past an offset once that
   reservation's pwrite has returned.

3. **Record-boundary `completed_end`.** `completed_end` is always at the
   end of some record, never mid-record. Falls out of invariant 2:
   reservations are contiguous by construction (fetch_add), and a slot's
   reservation covers exactly one record.

4. **File name ↔ first-seq.** For a WAL file named `wal-NNNN`, its first
   record has sequence number `NNNN`. Guaranteed because rotation happens
   under the allocation_lock, and the rotation code names the new file by
   the seq that the *next* allocator will observe.

5. **File ordering.** `files: RwLock<Vec<File>>` is append-only; each new
   file has `start > previous.start`. Rotation pushes to the end; the list
   stays sorted by start.

6. **`len` field trust.** A header at position `P` within
   `[0, completed_end)` truthfully describes `record_len` bytes of body at
   `[P + HEADER_LEN, P + HEADER_LEN + len)`. Guaranteed because the
   reservation covered the whole record, and completed_end only advanced
   past the reservation after its pwrite returned.

7. **pwrite visibility.** A fresh `StdFile::open` reading bytes below
   `completed_end` sees the same bytes the writer wrote — no tearing.
   `write_all_at` may issue multiple `pwrite` calls for a single logical
   buffer, but readers that respect `completed_end` never try to read
   bytes before the writer has published them as complete.

## How correctness is enforced

### Writer side

- **`allocation_lock` serialises `(seq, offset, slot, rotation)`.** Two
  separate atomics can't express an ordered pair across threads: a thread
  preempted between its `next_seq.fetch_add` and `next_offset.fetch_add`
  used to let a later writer grab both a higher seq AND a lower offset, and
  (after rotation) a stale writer would append a low seq into a high-keyed
  file. The mutex closes that race. pwrite still happens outside the lock
  (it's the expensive part).

- **`highest_completed_end` caps `completed_end`.** Overflow-path writers
  fetch_add `next_offset` *before* discovering they've overshot. That
  inflates `next_offset` past real data. Without a cap, `try_advance_completed`
  could advance `completed_end` to the inflated value and readers would
  fall off the end of the OS file. `highest_completed_end` tracks
  `max(offset + size)` across successful pwrites and is the true upper
  bound.

- **`ArcSwap<File>` for hot-path active-file lookup.** Avoids the RwLock-
  read atomic-counter ops on every allocation. Rotation updates the
  ArcSwap under the allocation_lock. Fast path is a single relaxed load.

- **Condvar wake for the fsync thread.** Commits signal a Condvar when
  they become the first subscriber on an empty bucket; fsync thread waits
  with `WAL_SYNC_INTERVAL` as a max-timeout. Eliminates the 80% empty
  wakeups of the fixed-interval loop.

### Reader side

Readers (recovery, stats sync, replay iterators) race with live writers.
Three defensive layers:

- **`limit = self.file.len()` snapshot per read.** `read_one_record`,
  `skip_one_record`, and `peek_sequence_number` take one snapshot of
  `completed_end` at entry and check `pos + HEADER_LEN <= limit` (and for
  reads, `pos + HEADER_LEN + body_len <= limit`) before attempting any
  read. A record whose body hasn't yet been fsynced or flushed gets an
  early `None` return instead of an `UnexpectedEof` error.

- **`partial_at_end` flag on `FileReader`.** When `read_one_record` or
  `skip_one_record` defers because the body isn't yet visible, it sets
  `partial_at_end`. `RecordIterator::next` checks the flag and stops
  iteration instead of rolling onto the next WAL file — if it did roll,
  the pending record's `StatusRecord` in the next file would surface
  without its `CommitRecord`, causing recovery to fail on a dangling
  reference.

- **`pos_before` check in seek loops.** `RecordIterator::new` and
  `FileRecordIterator::new` use `skip_one_record` to advance past records
  when seeking to a start sequence number. If `skip_one_record` no-ops
  (because the next record is partial), the loop would update
  `current_start` to the same seq forever. The `pos_before == pos_after`
  check catches that and breaks.

## In-flight slot tracking

Each `File` has a fixed 256-slot lock-free ring for in-flight pwrite
bookkeeping. Each slot is `{state: AtomicU8, offset: AtomicU64, size:
AtomicU64}`, 64-byte aligned.

Allocation:
- `claim_slot` linear-probes from `slot_hint` for a FREE slot, CAS to
  RESERVED, stores offset + size.
- Under the allocation_lock only one thread claims at a time; cache line
  contention with concurrent completers (who set state to COMPLETED then
  FREE via reaping) is bounded.

Completion:
- `complete_slot` stores COMPLETED via Release, then calls
  `try_advance_completed`.
- `try_advance_completed` scans all 256 slots, finds the smallest
  RESERVED offset (if any), uses that as the upper bound on
  `completed_end` (or falls back to `highest_completed_end`). Advances
  via `fetch_max`. Reaps COMPLETED slots whose writes now sit below
  the watermark.

## Known-tricky corners

- **Rotation race with AsyncUnsequencedWriter.** The async unsequenced
  writer takes `files.write()` independently of `allocation_lock`. The
  sequenced-write rotation path checks `files.files.last().start ==
  current.start` before creating a new file so it doesn't double-rotate
  if the async writer beat it to it. After rotation, both paths store the
  new active into the ArcSwap.

- **Stale `active_file` in the allocator.** An allocator loads active,
  fetch_adds offset, then checks for overflow. If overflow, it rotates and
  retries the loop with a fresh active load. Since the allocator holds
  `allocation_lock` throughout, no other sequenced writer can race it; only
  the async writer can rotate concurrently, and the rotation code handles
  that explicitly.

- **`trim_corrupted_tail` rewinds `next_offset` and `completed_end`.** This
  runs only on startup (before any concurrent access). Stores (not CAS)
  are safe because no other writer is alive yet.

## What's next

With the allocation mutex in place and group-commit via Condvar, the
dominant remaining cost at 10T is the mutex queue (~15 ms lock_wait per
commit). Paths that would reduce it further:

1. **Per-file packed atomic `(seq_prefix, offset)`** + ArcSwap for active
   + seal-then-swap rotation. Lock-free hot path. Needs careful sealing
   to preserve invariant 4 (file.start = first seq in file); without it,
   a race between a still-claimed allocator on the old file and a fresh
   allocator on the new file can produce duplicate seqs.

2. **Async durability mode.** Commits return before fsync durability ack.
   Trades the D in ACID for throughput on workloads where loss-window is
   tolerable. Needs explicit opt-in per transaction (or a db-wide
   relaxed-commit flag).

3. **RocksDB tuning.** Bigger memtable + fewer writer threads at higher
   concurrency — reduces the background compaction CPU pressure that
   currently shares cores with the bench workers.
