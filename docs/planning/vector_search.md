# Fixed-Length Arrays & Vector Similarity Search for TypeDB — MVP Plan

**Status:** Design
**Feature gate:** `UnimplementedFeature::FixedLengthArrays`, `UnimplementedFeature::SimilaritySearch`

---

## 1. Summary

Introduce **fixed-length array primitives** (`double[1024]`, `integer[512]`, `boolean[8]`, etc.) as immutable value types on TypeDB attributes, and **approximate nearest-neighbor similarity search** over `double[N]` attributes via HNSW indexing. The feature is designed for embedding-based AI workloads where bulk-loading millions of vectors in a single transaction is the expected write pattern.

Ships as a **preview feature** gated by existing `UnimplementedFeature` variants. All machinery can merge to main incrementally; the feature is invisible until the gate is removed.

---

## 2. MVP Scope

### In scope

- **Fixed-length array types** `T[N]` on all existing TypeDB primitive types, where `T ∈ {Boolean, Integer, Double, Decimal, Date, DateTime, DateTimeTZ, Duration, String}` and `1 ≤ N ≤ 65536`
- **Array literals** `[e₁, e₂, ..., eₙ]` disambiguated by type context
- **`@index(hnsw)` annotation** on `double[N]` attribute types (requires `@independent`)
- **`similarity_search(type, target, [index], [max_distance])`** built-in function, returning a streaming iterator of attribute instances
- **Cosine similarity** as the metric
- **Segment-based HNSW construction**: build index for ≥100k entries in a write transaction; brute-force for smaller
- **int16 per-vector quantization** inside HNSW graph for speed/memory
- **Exact f64 re-check** at yield time to enforce `max_distance` precisely
- **Post-filter hybrid search** via natural TypeQL pattern composition
- **Preview gating** via `UnimplementedFeature` variants
- **Drivers and BDD tests** for the full surface

### Out of scope (future iterations)

- **Pre-filter / single-stage filtering** (predicate pushdown into HNSW traversal). See §10.
- **HNSW tuning parameters** (`m`, `ef_construction`) exposed in TypeQL -- hardcoded defaults only
- **Non-cosine metrics** (Euclidean, inner product, Hamming)
- **HNSW on integer/other array types** -- quantization trick doesn't generalize, user need is niche
- **`float` (f32) primitive type** for honest embedding storage
- **Binary / bit-array / sparse vector types** with specialized indexes
- **Matryoshka prefix-distance extrapolation** (runtime optimization, MariaDB Part IV technique)
- **Distance value exposure** in results (`let ($emb, $dist) in ...` syntax)
- **Least-similar / farthest-neighbor search**

---

## 3. User-Facing Specification

### 3.1 Schema

```typeql
define
  attribute pdf-embedding @independent @index(hnsw), value double[1024];
  attribute feature-flags, value boolean[32];
  attribute counts, value integer[128];
  entity document, owns pdf-embedding, owns feature-flags;
```

Rules:
- `T[N]` requires a positive integer literal `N ≤ 65536`
- `@index(hnsw)` valid only on `double[N]` attributes
- `@index(hnsw)` requires `@independent` (enforced, not auto-implied)
- One `@index` per attribute type

### 3.2 Insertion

```typeql
insert
  $d isa document,
    has pdf-embedding [0.12, -0.34, ..., 0.87],
    has feature-flags [true, false, true, ...];
```

- Array length must match declared `N` exactly
- Element types must match base primitive
- Arrays are immutable: no partial update of elements

### 3.3 Query

```typeql
match
  let $emb in similarity_search(pdf-embedding, $target);
  $doc isa document, has pdf-embedding $emb, has category "science";
limit 10;
```

**Function signature:**
```
similarity_search(
    type: <attribute-type>,             # required, type reference
    target: double[N],                  # required, matches type's dimensionality
    index: boolean = true,              # optional, false forces brute force
    max_distance: double = unbounded    # optional, cosine distance threshold
) -> stream of attribute instances
```

Semantics:
- Returns a **lazy stream** of attribute instances in approximate cosine-similarity order (nearest first)
- Stream terminates when HNSW frontier is exhausted or `max_distance` bound is reached
- With `index=false`, performs brute-force scan over all attribute instances of the type
- `max_distance` is enforced by exact f64 re-check before yielding (see §6.5)

**Count limits are query-level**, not function arguments:
- Users apply `limit 10` at the pipeline level
- This ensures consistent semantics regardless of whether post-filter or (future) pushdown is used
- The executor uses the pipeline `limit` as a hint to bound work internally

### 3.4 Hybrid search (post-filter)

The canonical pattern:
```typeql
match
  let $emb in similarity_search(pdf-embedding, $target);
  $doc isa document, has pdf-embedding $emb, has category "science";
limit 10;
```

Execution: `similarity_search` streams candidates in similarity order; downstream pattern filters them; `limit` stops the pull. In MVP this is post-filter only -- the planner does not push the graph pattern into the iterator. See §10 for pre-filter plans.

---

## 4. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│ Driver (Rust / Python / Java / Node.js)                          │
│   FixedArray type, protobuf/JSON serialization, param binding   │
└────────────────────────┬─────────────────────────────────────────┘
                         │ gRPC / HTTP
┌────────────────────────┴─────────────────────────────────────────┐
│ typedb-protocol (external crate)                                 │
│   FixedArray message, ValueTypeWithParams extension              │
└────────────────────────┬─────────────────────────────────────────┘
                         │
┌────────────────────────┴─────────────────────────────────────────┐
│ Server                                                            │
│  ┌─────────────────┐  ┌──────────────────────────────────────┐  │
│  │ TypeQL parser   │  │ Query planner / executor             │  │
│  │ (pest grammar)  │→ │ SimilaritySearchExecutor             │  │
│  └─────────────────┘  │   • HNSW iterator                    │  │
│                       │   • brute-force iterator             │  │
│  ┌─────────────────┐  │   • exact-distance re-check          │  │
│  │ Schema / types  │  └──────────────────────────────────────┘  │
│  │ (ValueType::Array) │            │                              │
│  └─────────────────┘              ▼                              │
│                       ┌──────────────────────────────────────┐   │
│                       │ HNSW index (per attribute type)      │   │
│                       │   • segment-based, immutable         │   │
│                       │   • int16 quantized vectors          │   │
│                       │   • graph edges in RocksDB KV        │   │
│                       └──────────────────────────────────────┘   │
│                                    │                              │
│                       ┌──────────────────────────────────────┐   │
│                       │ Attribute storage (RocksDB)          │   │
│                       │   • full f64 canonical vectors       │   │
│                       │   • existing keyspace layout         │   │
│                       └──────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
```

### Key architectural decisions

| Decision | Rationale |
|----------|-----------|
| Arrays as primitive values (not collections) | Immutable, fixed-length matches the embedding use case; reuses attribute machinery |
| HNSW graph as KV pairs in RocksDB (shadow-table pattern) | ACID for free, storage-engine-agnostic, matches MariaDB's validated approach |
| In-memory graph is a **page cache**, not required-resident | Enables 10M+ vector indexes on reasonable hardware |
| Per-segment immutable HNSW | Lock-free reads; aligns with existing LSM segment model |
| int16 per-vector quantization inside index | ~50% memory, ~30-50% distance speedup, no global state |
| Canonical f64 vector stays in attribute store | Exact retrieval; quantization is internal to index only |
| mHNSW with leniency factor λ=1.2 | ~10x faster construction at comparable recall (MariaDB Part III) |
| Streaming pull-based iterator | Natural fit for consumer-driven limits; enables future predicate pushdown |
| Post-filter only in MVP | Pre-filter / pushdown is a planner optimization that can be added without API change |
| Count limits at pipeline level, not function args | Avoids order-dependent semantics; matches SQL `LIMIT` after `WHERE` |

---

## 5. TypeQL Layer

### 5.1 Files to modify

| File | Change |
|------|--------|
| `typeql/rust/parser/typeql.pest` | Grammar: `value_type_array`, `annotation_index`, `array_literal`, named function arguments |
| `typeql/rust/common/token.rs` | Tokens: `HNSW`, `SIMILARITY_SEARCH`, `ANNOTATION_INDEX` |
| `typeql/rust/annotation.rs` | New `Annotation::Index(IndexAnnotation)` variant |
| `typeql/rust/type_.rs` | Extend `ValueType` → `ValueType::Array(PrimitiveType, u32)` |
| `typeql/rust/expression/mod.rs` | AST node for `similarity_search` call |
| `typeql/rust/parser/define/type_.rs` | Parse `value T[N]` in attribute declarations |
| `typeql/rust/parser/annotation.rs` | Parse `@index(hnsw)` |
| `typeql/rust/parser/literal.rs` | Parse array literals, dispatch by type context |
| `typeql/rust/parser/statement/single.rs` | Handle `let $v in similarity_search(...)` (reuses existing `statement_in`) |

### 5.2 Grammar additions

```pest
// Value type extension
value_type = { value_type_array | value_type_primitive | label }
value_type_array = { value_type_primitive ~ BRACKET_OPEN ~ integer_literal ~ BRACKET_CLOSE }

// @index annotation
annotation_index = { ANNOTATION_INDEX ~ PAREN_OPEN ~ index_type ~ PAREN_CLOSE }
index_type = { HNSW }
ANNOTATION_INDEX = @{ "@index" }
HNSW = { "hnsw" }

annotation = { /* existing */ | annotation_index }

// Array literal
array_literal = { BRACKET_OPEN ~ literal ~ ( COMMA ~ literal )* ~ BRACKET_CLOSE }

// Named function arguments (for similarity_search options)
expression_function_args = { expression ~ ( COMMA ~ ( named_arg | expression ) )* }
named_arg = { identifier ~ ASSIGN ~ expression }

// similarity_search recognized as builtin name
SIMILARITY_SEARCH = { "similarity_search" }
builtin_func_name = { /* existing */ | SIMILARITY_SEARCH }
```

### 5.3 AST and validation

- `ValueType::Array(base, n)`: two array types with different `base` or `n` are distinct types
- `@index(hnsw)` validation (at type-check time): attribute must have `value double[N]`; attribute must also have `@independent`
- Array literal validation (at expression-type-check time): length must match expected `N`; elements must be compatible with `T`
- `similarity_search` first argument is resolved as a **type label reference**, not a variable binding; the return type is bound by this reference

### 5.4 Query planner

`similarity_search` is recognized as a **source operator** -- a leaf producing bindings. The planner places it first in the pipeline. Downstream patterns become consumers of its stream. A pipeline-level `limit` stops the pull.

For MVP, no predicate pushdown. The planner may, however, propagate `limit` down as an internal hint (see §6.4).

---

## 6. Server / Core Layer

### 6.1 Value encoding

**Files to modify:**

| File | Change |
|------|--------|
| `encoding/value/value.rs` | Add `Value::Array(FixedArrayValue)` variant |
| `encoding/value/value_type.rs` | Add `ValueType::Array(PrimitiveValueType, u32)` |
| **NEW** `encoding/value/array_bytes.rs` | `ValueEncodable` impl for fixed-length arrays |
| `answer/variable_value.rs` | Extend `VariableValue` for array values |

```rust
pub enum FixedArrayValue {
    Boolean(Box<[bool]>),
    Integer(Box<[i64]>),
    Double(Box<[f64]>),
    Decimal(Box<[Decimal]>),
    Date(Box<[NaiveDate]>),
    DateTime(Box<[NaiveDateTime]>),
    DateTimeTZ(Box<[DateTime<TimeZone>]>),
    Duration(Box<[Duration]>),
    String(Box<[Box<str>]>),
}
```

**On-disk encoding:** raw little-endian bytes for fixed-width types, length-prefixed concatenation for strings. Arrays live in the attribute value slot (keyed by IID). Not embedded in keys (too large for prefix optimization).

A `double[1024]` attribute value is ~8 KB. This is larger than typical attribute values; may warrant benchmarking against existing RocksDB keyspace configurations to confirm compression and block-size tuning behave well.

### 6.2 Schema validation

**Files to modify:**

| File | Change |
|------|--------|
| `concept/type_/annotation.rs` | Add `Annotation::Index(IndexAnnotation)` |
| `concept/type_/type_manager.rs` | Validate `@index(hnsw)` constraints |
| `concept/type_/constraint.rs` | Map `@index` to constraint during schema commit |

Validation rules enforced at schema commit:
- `@index(hnsw)` only on attribute types with `ValueType::Array(Double, _)`
- `@index(hnsw)` co-occurs with `@independent`
- At most one `@index` per attribute type
- Rejection returns `UnimplementedFeature::SimilaritySearch` error while gated

### 6.3 HNSW index

**NEW module:** `/opt/project/index/hnsw/`

```
index/hnsw/
  mod.rs             -- public API
  graph.rs           -- HnswGraph: paged access, layer traversal
  insert.rs          -- insertion with mHNSW leniency (λ=1.2)
  search.rs          -- search algorithm (used by iterator)
  iterator.rs        -- HnswSearchIterator (streaming, pull-based)
  distance.rs        -- cosine distance on f64 (exact) and int16 (quantized)
  quantize.rs        -- per-vector int16 quantization
  serialize.rs       -- KV serialization for graph edges and quantized data
  config.rs          -- HnswConfig { m: 16, ef_construction: 10, λ: 1.2, ef_search: 50 }
```

**KV layout** (new RocksDB keyspace or reuse existing):

```
# Node data: quantized vector + magnitudes + per-layer neighbor lists
Key:   [hnsw_node_prefix][type_id][node_iid]
Value: [max_layer:u8]
       [int16_vector: N * 2 bytes]
       [scale: f32]
       [magnitude_sq: f32]
       [layer_0_neighbors: M * iid_size]
       [layer_1_neighbors: ...]
       ...

# Index metadata (one row per @index(hnsw) attribute type)
Key:   [hnsw_meta_prefix][type_id]
Value: [entry_point_iid][max_layer][num_elements][M][ef_construction][λ_x1000]
```

**Rationale for co-locating vector + edges per node:** HNSW traversal does a distance computation for every node it visits plus a read of its neighbor list. Single KV read per node is cache-friendly; splitting vector and edges would double the read count.

**mHNSW leniency** (from MariaDB Part III):
- During greedy descent, consider all neighbors within `λ × d_nearest` (not just the single nearest)
- Default λ=1.2 (20% leniency); allows `ef_construction=10` with recall comparable to standard HNSW at `ef_construction=200`
- For cosine specifically, apply sigmoid-adaptive leniency: more greedy far from target, more lenient near the target

**Quantization** (from MariaDB Part II):
- Per-vector: `α = 32767 / max(|coord|)`, then `int16 = round(coord × α)`
- Store `α` (f32) and `|vec|²` (f32, in quantized units) alongside the int16 vector
- Distance computations use quantized data; scales combine at distance time
- For cosine specifically, **scales cancel entirely** in the distance formula -- quantized dot products are computed directly, no scale arithmetic in the hot path

**Search iterator** (core novelty):
```rust
pub struct HnswSearchIterator<'a> {
    graph: &'a HnswGraph,
    target_quantized: Box<[i16]>,
    target_scale: f32,
    candidates: BinaryHeap<Reverse<(OrderedFloat<f32>, NodeId)>>,
    visited: BloomFilter,
    max_distance: Option<f32>,
    ef_search: usize,
    relaxed_threshold: Option<f32>,  // max_distance * (1 + ε) for quant tolerance
}

impl Iterator for HnswSearchIterator<'_> {
    type Item = (NodeId, f32);
    fn next(&mut self) -> Option<(NodeId, f32)> {
        // 1. Pop nearest candidate from frontier
        // 2. Visit neighbors, add unvisited to frontier
        // 3. Check against relaxed_threshold (quantized distance)
        // 4. Yield (node_id, quantized_distance)
        // 5. Return None when frontier exhausted
    }
}
```

### 6.4 Query execution

**NEW file:** `executor/instruction/similarity_search_executor.rs`

**Files to modify:**
| File | Change |
|------|--------|
| `ir/translation/` | Translate `similarity_search` AST → IR |
| `compiler/executable/match_.rs` | Compile IR → executable plan with `SimilaritySearchExecutor` |

**Multi-segment search strategy:**

At query time, the attribute type may have multiple segments with mixed index state:
- Segments with HNSW: use `HnswSearchIterator`
- Segments without HNSW (< 100k threshold): brute-force iterator that reads all attribute values, computes f64 cosine distance, pushes into a local priority queue sized by the downstream pull rate

The executor maintains a **k-way merge** across segment iterators via a min-heap ordered by (quantized or exact) distance. Each call to `next()` pops the segment with the currently-nearest head, yields it, and advances that segment's iterator.

**Exact distance re-check:**
Before yielding each candidate to the caller:
1. Fetch the canonical f64 vector from attribute storage (one KV read)
2. Compute exact cosine distance
3. Apply the user's `max_distance` threshold
4. Yield the attribute binding if it passes; otherwise skip and pull the next candidate

**Cost:** one extra attribute fetch + one f64 distance per *yielded* candidate (not per visited node). Negligible compared to HNSW traversal cost.

**Hint propagation:** the pipeline's `limit` is passed to the executor as a hint to size `ef_search` adaptively. If `limit=10`, use `ef_search=max(50, 10)`. Without a limit, use `ef_search=50` as default.

### 6.5 `max_distance` semantics under approximation

Two sources of inexactness are separated:
- **HNSW graph structure** may miss true near-neighbors entirely. Documented as expected behavior; users who need completeness use `index=false`.
- **int16 quantization** perturbs computed distances slightly. Resolved by the exact f64 re-check at yield time.

**Guarantee:** every result yielded has true f64 cosine distance ≤ `max_distance`.
**Non-guarantee:** not every vector with true distance ≤ `max_distance` is found (this is fundamental to approximate search).

Users who need radius-query completeness should use `index=false`.

### 6.6 Transactions and bulk loading

Bulk loading expectation: **1-10M vectors per transaction**, explicitly slow and RAM-heavy.

**Write path integration:**
- As each `double[N]` value is written into an attribute type with `@index(hnsw)`:
  - Quantize to int16, compute magnitude and scale
  - Insert into in-memory HNSW graph incrementally (amortizes O(N log N) cost)
- At commit (in `thing_manager.finalise()` before `snapshot.commit()`):
  - Count entries written to each HNSW-indexed attribute type
  - If ≥ 100k: serialize graph edges + quantized data to the HNSW keyspace
  - If < 100k: discard graph; segment is marked brute-force-only
- On rollback: discard in-memory graph, no durable writes

**Memory considerations:**
| Scale | Int16 quantized graph | f64 canonical (attribute store) |
|-------|------------------------|----------------------------------|
| 1M × double[1024] | ~2 GB + ~128 MB edges | ~8 GB (disk, paged) |
| 10M × double[1024] | ~20 GB + ~1.3 GB edges | ~80 GB (disk, paged) |

The attribute store flows through the existing `WriteSnapshot` → `WriteBatches` → RocksDB pipeline (memory-mapped / paged naturally). The in-memory HNSW graph is the main RAM pressure during bulk load. For very large loads, use memory-mapped temp files for the quantized vector buffer.

**Memory limit:** introduce a configurable per-transaction memory budget. Reject transactions that would exceed it with a clear error.

### 6.6a Post-commit: in-memory graph as page cache

After commit, the HNSW graph for a segment is **immutable and durable** in RocksDB. Readers do not need it fully in RAM.

- Graph nodes load on demand via the existing RocksDB block cache
- Hot nodes (entry point, upper layers, frequently-traversed regions) stay cached
- A 10M-vector index does not require 20 GB of RAM for reads -- working set size determines effective RAM usage, governed by block cache

This **page-cache** model (per MariaDB Part I) is distinct from "load the whole graph in memory" and is what makes the feature viable at scale. Call it out explicitly in the implementation: reader code paths must issue KV reads per node, not assume whole-graph residency.

### 6.7 Segment compaction

Existing RocksDB-driven compaction merges segments. When segments with HNSW indices are compacted:
- Merge the attribute data (existing machinery)
- Rebuild the HNSW index for the merged segment (one-time cost per compaction)

If one input segment was brute-force-only and the other had HNSW, the merged segment gets a rebuilt HNSW if the merged size ≥ 100k.

Compaction runs in the background on existing RocksDB compaction threads. No new thread machinery required.

---

## 7. Protocol Layer

The protocol lives in the external `typedb-protocol` crate.

### 7.1 Protobuf changes

```protobuf
message FixedArray {
  uint32 length = 1;
  oneof elements {
    DoubleArray   double_elements   = 2;
    Int64Array    integer_elements  = 3;
    BoolArray     boolean_elements  = 4;
    StringArray   string_elements   = 5;
    DecimalArray  decimal_elements  = 6;
    // ... one variant per base primitive
  }
}

message DoubleArray  { repeated double values = 1 [packed = true]; }
message Int64Array   { repeated int64  values = 1 [packed = true]; }
message BoolArray    { repeated bool   values = 1 [packed = true]; }
message StringArray  { repeated string values = 1; }
message DecimalArray { /* existing Decimal encoding, repeated */ }

// Extension of ValueType
message ValueTypeWithParams {
  ValueTypeProto base_type = 1;
  oneof params { uint32 array_length = 2; }  // present when T[N]
}
```

Wire size: `double[1024]` ≈ 8 KB packed. A top-10 result set ≈ 80 KB. Acceptable for gRPC streaming.

### 7.2 Server serialization

**Files to modify:**

| File | Change |
|------|--------|
| `server/service/grpc/concept.rs` | Serialize `FixedArrayValue` → protobuf `FixedArray` |
| `server/service/http/message/query/concept.rs` | JSON: `{"kind": "attribute", "value": [...], "valueType": "double[1024]"}` |
| `server/service/grpc/row.rs` | Include array attributes in `ConceptRow` |

### 7.3 Query parameters

Target vectors pass as bound parameters, not inline TypeQL literals:
```
similarity_search(pdf-embedding, $target, max_distance=0.3)
// $target supplied as FixedArray in QueryParameters
```

Avoids parsing thousands of literal elements per query.

### 7.4 Streaming

No new RPCs. Results flow through existing `Transaction.Query` answer streams (gRPC and HTTP). Backpressure comes for free.

---

## 8. Drivers

Each driver needs: fixed-length array type, protobuf/JSON serialization, query parameter binding.

| Language | Type | Key detail |
|----------|------|------------|
| **Rust** | `FixedArray` enum with typed variants | Direct protobuf conversion |
| **Python** | `FixedArray` class with numpy interop | `to_numpy()`, `from_numpy()`. **Highest priority** -- ML users live in Python. |
| **Java** | `FixedArray` with `double[]`, `long[]` | Defensive copies |
| **Node.js** | `FixedArray` with `Float64Array`, `BigInt64Array` | Use TypedArrays for numeric types |

All drivers: support passing array values as query parameters (not inline TypeQL).

---

## 9. Behaviour Tests

**Files to modify / create:**

| File | Change |
|------|--------|
| `tests/behaviour/steps/params/lib.rs` | Parameter types: array values, `@index(hnsw)` annotation, `similarity_search` arguments |
| `tests/behaviour/steps/concept/type_/attribute_type.rs` | Steps for defining array attribute types |
| `tests/behaviour/steps/concept/thing/attribute.rs` | Steps for inserting/matching array attributes |
| `tests/behaviour/steps/query/` | Steps for similarity_search queries and results |
| NEW `.feature` files | Scenarios below |

### 9.1 Test scenarios

**Schema (positive):**
- Define `attribute pdf-embedding @independent @index(hnsw), value double[1024]`
- Define `attribute feature-flags, value boolean[32]` (no index)
- Define `attribute timeseries, value datetime[24]`

**Schema (negative):**
- Reject `value double[1024]` without `[N]`
- Reject `N=0` or `N > 65536`
- Reject `@index(hnsw)` on `value integer[512]` (only `double[N]` allowed)
- Reject `@index(hnsw)` on `value string[10]`
- Reject `@index(hnsw)` without `@independent`
- Two `@index(hnsw)` on one attribute → reject

**Insertion:**
- Insert `double[3]` attribute with exact length
- Reject wrong length (too long, too short)
- Reject wrong element type (strings in a `double[N]`)
- Bulk insert 150k vectors → HNSW index built (verify via introspection or later query latency)
- Bulk insert 50k vectors → no HNSW index; brute-force fallback works

**Search (basic):**
- Insert 1000 vectors with known coordinates; `similarity_search` yields nearest first
- Brute-force (`index=false`) yields same nearest (with possibly different tie-break)
- `max_distance` threshold filters results below the bound

**Search (streaming):**
- Stream more than top-K; verify iterator yields multiple results lazily
- Stop pulling mid-stream; verify no over-computation

**Hybrid (post-filter):**
- `similarity_search` + `has category "science"` pattern returns only science docs in similarity order
- Filter that matches nothing returns empty result set

**Multi-segment:**
- Insert 150k vectors in txn 1, then 50k in txn 2; query spans both (HNSW + brute-force segments) and yields merged-by-similarity results

**Feature gate:**
- With gate active, `value double[1024]` rejected with `UnimplementedFeature` error
- With gate active, `similarity_search` call rejected with `UnimplementedFeature` error

---

## 10. Future Iterations (Non-goals for MVP)

Tracked here so the MVP preserves optionality without committing to the work.

### 10.1 Pre-filter / single-stage filtering (most requested)

**Goal:** when a query like
```typeql
match
  let $emb in similarity_search(pdf-embedding, $target);
  $doc isa document, has pdf-embedding $emb, has category "science";
limit 10;
```
has a selective filter (`has category "science"`), evaluate the filter first and inject the candidate IID set into the HNSW iterator as a predicate.

**Why post-MVP:**
- Requires planner extension: identify constraints reachable from the bound variable, estimate selectivity, decide pushdown
- Requires executor extension: HNSW iterator accepts optional `Fn(NodeId) -> bool` predicate; brute-force executor accepts optional IID set
- Requires cost model: when to pre-filter (tiny set, brute force) vs. single-stage (medium set, HNSW with predicate) vs. post-filter (non-selective)
- Safe to add later because the user-facing syntax doesn't change -- the query stays declarative, the planner gets smarter

**Note on semantics:** the pipeline `limit` (not a `top_k` function argument) is what makes this safe. Because the count is applied *after* the filter, pre-filter and post-filter produce identical result sets -- only performance differs.

### 10.2 Distance value exposure

```typeql
let ($emb, $distance) in similarity_search(pdf-embedding, $target);
```

Requires tuple-destructuring in `let ... in`. Check whether `statement_in` grammar already supports this; if not, grammar extension is small.

### 10.3 Tuning parameters on `@index(hnsw)`

```typeql
attribute pdf-embedding @independent @index(hnsw, m=32, ef_construction=200) ...
```

Requires parameterized annotation parsing (grammar and schema). Exposing `m`, `ef_construction`, `λ` lets power users tune the recall/speed trade-off.

### 10.4 Matryoshka prefix distance extrapolation

From MariaDB Part IV: compute distance on a vector prefix, extrapolate to full length. Auto-detect applicability via statistical check on first 10k distance computations. 10-30% QPS improvement for Matryoshka-trained embeddings (OpenAI, Gemini families). Pure executor optimization, no API change.

### 10.5 `float` (f32) primitive type

Most real embeddings are produced as f32 by the model. Currently we store them as f64 (2x waste). A new `float` primitive with `float[N]` arrays would halve attribute storage with no practical precision loss. `@index(hnsw)` would extend to `float[N]` naturally.

### 10.6 Additional distance metrics

Euclidean, Hamming (for future binary array types). Keep cosine as default; add as named option:
```typeql
similarity_search(pdf-embedding, $target, metric="euclidean")
```

### 10.7 Binary / bit-packed / sparse vector types

Different indexes (binary HNSW for Hamming, inverted file for sparse). Distinct feature; mentioned here for completeness.

### 10.8 HNSW on integer arrays

Not planned. Quantization trick doesn't generalize, and users wanting integer ANN typically want Hamming (binary) or Jaccard (sparse) -- different indexes.

### 10.9 Least-similar / farthest-neighbor search

Niche use case. For cosine, users can negate the target vector today. No dedicated support planned.

### 10.10 Distance hints / oversampling controls

Advanced users may want to expose `ef_search` per query. Not exposed in MVP; defaults plus query-level `limit` hint should cover typical cases.

---

## 11. Phasing & Dependencies

```
Phase 0: Feature gate (UnimplementedFeature variants)
    |
    +---> Phase 1: TypeQL grammar, AST, type check
    |         |
    +---> Phase 2: typedb-protocol (parallel)
              |
              v
         Phase 3a: Value encoding (FixedArrayValue, array_bytes.rs)
              |
              +---> Phase 3b: Schema validation (@index(hnsw))
              |         |
              +---> Phase 3c: HNSW core (graph, insert, search, iterator, quantize)
              |         |
              |         v
              |    Phase 3d: Query execution (SimilaritySearchExecutor, merge, re-check)
              |         |
              v         v
         Phase 4: Drivers (Rust, Python, Java, Node.js — parallel)
              |
              v
         Phase 5: Behaviour tests (specs written early, implemented alongside)
```

**Critical path:** Phase 0 → Phase 1 → Phase 3c (HNSW) → Phase 3d (executor) → Phase 5 (integration).

**Parallelizable:**
- Phase 1 and Phase 2
- Phase 3b and Phase 3c
- All driver languages in Phase 4
- Phase 5 test specifications can be drafted alongside any earlier phase

---

## 12. Risks

| Risk | Mitigation |
|------|------------|
| Memory pressure during bulk load | mmap'd temp files for quantized graph; configurable txn memory limit; document requirements |
| HNSW recall quality | Benchmark against SIFT1M, GloVe during development; compare against a reference HNSW implementation |
| Graph ambiguity (`[1.0, 2.0]` literal conflicting with future list syntax) | Context-dependent parsing — array literal valid only where array type expected |
| Quantization error exceeding user expectations | Exact f64 re-check at yield enforces `max_distance` precisely; document approximation semantics |
| Block cache eviction under memory pressure | Existing RocksDB cache tuning applies; HNSW workload is random-access, may warrant a dedicated cache for hot nodes |
| Users expecting `top_k` on `similarity_search` (vector DB convention) | Clear documentation; `limit` at pipeline level is the TypeQL-native form |
| Planner putting `similarity_search` in the wrong place in the pipeline | Mark it as a source operator explicitly; existing TypeQL planner respects this pattern |
| Feature interaction with schema migrations | Array types are immutable once declared; changing `N` or base type requires redefinition, same as any type change |

---

## 13. Open Questions (to resolve during implementation)

1. **Dedicated RocksDB keyspace for HNSW data** vs. reusing existing `OptimisedPrefix25`? The large value sizes (node data with int16 vector + edges) suggest a dedicated keyspace with tuned block cache.
2. **Tuple destructuring in `let ... in`**: does the current grammar support `let ($a, $b) in f(...)`? Matters for future distance-value exposure; doesn't block MVP.
3. **Quantization relaxation ε**: what's the right tolerance for the internal `max_distance × (1 + ε)` search bound? Benchmark-driven; likely 1-2% works.
4. **Compaction-time re-quantization**: leave quantized data untouched when compacting, or re-quantize with potentially-improved scheme? MVP answer: leave untouched for simplicity.
5. **Protobuf compatibility**: does extending `Value` in the existing protocol require a major version bump for `typedb-protocol`? Coordinate with the protocol maintainers.
6. **Per-txn memory budget default**: what's sane? Needs benchmarking. Suggested starting point: 16 GB per write transaction, configurable.

---

## 14. References

- MariaDB Vector, Part I — Storage architecture (shadow-table pattern)
- MariaDB Vector, Part II — In-memory representation, int16 quantization, SIMD
- MariaDB Vector, Part III — mHNSW leniency factor, adaptive cosine leniency
- MariaDB Vector, Part IV — Matryoshka prefix distance extrapolation (future work)
- Malkov & Yashunin, "Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs" (2016)
