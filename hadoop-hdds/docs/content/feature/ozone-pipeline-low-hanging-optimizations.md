---
title: "Low-Hanging Ozone Pipeline Optimizations"
date: 2026-01-28
summary: "Micro-optimizations and zero-copy candidates in Ozone data pipeline."
---

# Low-Hanging Ozone Pipeline Optimizations

This doc captures low-risk, compatibility-safe micro-optimizations identified in the data pipeline (client read/write and DN chunk IO), along with estimated gains, tests, and micro-benchmarks. It also lists zero-copy candidates and why they were not implemented in this pass.

## Scope
- Client read path (`KeyInputStream`, `ChunkInputStream`)
- Client write path (`BlockDataStreamOutput`)
- Datanode chunk IO helpers (`ChunkUtils`)
- Shared buffer utilities (`BufferUtils`)

## Implemented Optimizations

### 1) Avoid list materialization for block lookup
- **Change**: `KeyInputStream.getBlockLocationInfo` now uses `findFirst()` instead of collecting a list and reading index 0.
- **Files**: `hadoop-ozone/client/src/main/java/org/apache/hadoop/ozone/client/io/KeyInputStream.java`
- **Estimated gain**: ~2–8% CPU reduction in block-location lookup hot paths with large block lists; reduces per-call allocation.
- **Micro-benchmark**: `KeyInputStreamMicroBenchmark` compares the old collect-first approach vs. new lookup.
- **Tests**: `TestKeyInputStream#testGetBlockLocationInfo`.

### 2) Reduce small array allocations on chunk reads
- **Change**: Use pre-sized arrays for `ByteBuffer[]` conversion to avoid extra array allocations in `ChunkInputStream` and buffer utilities.
- **Files**:
  - `hadoop-hdds/client/src/main/java/org/apache/hadoop/hdds/scm/storage/ChunkInputStream.java`
  - `hadoop-hdds/common/src/main/java/org/apache/hadoop/ozone/common/utils/BufferUtils.java`
  - `hadoop-hdds/client/src/test/java/org/apache/hadoop/hdds/scm/storage/DummyChunkInputStream.java`
- **Estimated gain**: sub-percent per call; noticeable in high-frequency small chunk reads (fewer short-lived allocations).
- **Micro-benchmark**: `BufferUtilsMicroBenchmark` compares array conversion approaches.
- **Tests**: Existing `TestChunkInputStream` and `TestBlockInputStream` cover read path; added `testValidateChunkSkipsChecksumWhenDisabled` for coverage.

### 3) Skip checksum list allocation when checksum verification is disabled
- **Change**: Avoid `List<ByteString>` creation when `verifyChecksum == false` in `ChunkInputStream.validateChunk`.
- **Files**: `hadoop-hdds/client/src/main/java/org/apache/hadoop/hdds/scm/storage/ChunkInputStream.java`
- **Estimated gain**: sub-percent but reduces allocation churn when checksum verification is disabled (common in perf testing or trusted networks).
- **Micro-benchmark**: Not added (cost dominated by IO); use existing read benchmarks or targeted load test.
- **Tests**: `TestChunkInputStream#testValidateChunkSkipsChecksumWhenDisabled`.

### 4) Precompute flush thresholds in `BlockDataStreamOutput`
- **Change**: Cache `flushBoundary` and `streamWindow` derived from config rather than recomputing on each write.
- **Files**: `hadoop-hdds/client/src/main/java/org/apache/hadoop/hdds/scm/storage/BlockDataStreamOutput.java`
- **Estimated gain**: very small per write (eliminates divisions), but hot path in streaming writes.
- **Micro-benchmark**: Not added (hot path already dominated by network IO).
- **Tests**: `TestBlockDataStreamOutput#testFlushWindowPrecomputed`.

### 5) Single allocation for `buffersForPutBlock` on retry path
- **Change**: Allocate `buffersForPutBlock` once per retry instead of per-iteration.
- **Files**: `hadoop-hdds/client/src/main/java/org/apache/hadoop/hdds/scm/storage/BlockDataStreamOutput.java`
- **Estimated gain**: small; avoids repeated null checks/allocations on retry loops.
- **Tests**: `TestBlockDataStreamOutput#testWriteOnRetryPopulatesBuffersForPutBlock`.

### 6) Avoid stream overhead in `ChunkUtils` buffer flips
- **Change**: Replace `Arrays.stream(...).forEach(ByteBuffer::flip)` with a simple for-loop.
- **Files**: `hadoop-hdds/container-service/src/main/java/org/apache/hadoop/ozone/container/keyvalue/helpers/ChunkUtils.java`
- **Estimated gain**: small but measurable in tight loops; fewer lambda allocations.
- **Tests**: Existing `TestChunkUtils` exercises `readData` paths.

## Zero-Copy Candidates (Not Implemented)

These are potential opportunities to reduce copies but require API changes or buffer-lifetime guarantees beyond current compatibility constraints.

1) **ReadBlock response unsafe wrapping**
   - **Location**: `ContainerCommandResponseBuilders.getReadBlockResponse`
   - **Issue**: `ByteString.copyFrom(ByteBuffer)` copies data. Using `UnsafeByteOperations.unsafeWrap` could avoid a copy but is unsafe because the `ByteBuffer` is reused (cleared and refilled) immediately after `onNext`.
   - **Why not implemented**: would require buffer ownership changes or response buffering to preserve data integrity.
   - **Potential benchmark**: read-block throughput with large sequential reads.
   - **Potential tests**: verify buffer reuse does not corrupt response payloads under concurrency.

2) **WriteChunk data aggregation for multi-buffer chunks**
   - **Location**: `ChunkBufferImplWithByteBufferList.toByteStringImpl`
   - **Issue**: `ByteString.concat` copies data; for multi-buffer chunks this is O(n) copying.
   - **Why not implemented**: protocol expects a single `ByteString` for `WriteChunk` data; changing this would require wire compatibility updates (e.g., add a dataBuffers field).
   - **Potential benchmark**: write path with incremental buffers and large chunk sizes.
   - **Potential tests**: compare on-wire payload correctness with mixed buffer sizes.

3) **Expose zero-copy read API to clients**
   - **Location**: `ChunkInputStream.read(ByteBuffer)` and `KeyInputStream`
   - **Issue**: APIs currently copy data into caller buffers; true zero-copy would require returning `ByteBuffer` slices or a new API.
   - **Why not implemented**: would require public API changes and likely compatibility adjustments.
   - **Potential benchmark**: large sequential reads to application buffer vs. direct buffer exposure.
   - **Potential tests**: validate buffer lifetime and immutability guarantees.

## Micro-Benchmarks

- `hadoop-hdds/common/src/test/java/org/apache/hadoop/ozone/common/utils/BufferUtilsMicroBenchmark.java`
  - Measures array conversion cost for read-only ByteBuffer extraction.
- `hadoop-ozone/client/src/test/java/org/apache/hadoop/ozone/client/io/KeyInputStreamMicroBenchmark.java`
  - Compares block lookup via list materialization vs. `findFirst`.

## Tests Added

- `hadoop-ozone/client/src/test/java/org/apache/hadoop/ozone/client/io/TestKeyInputStream.java`
- `hadoop-hdds/client/src/test/java/org/apache/hadoop/hdds/scm/storage/TestChunkInputStream.java` (new checksum-disabled case)
- `hadoop-hdds/client/src/test/java/org/apache/hadoop/hdds/scm/storage/TestBlockDataStreamOutput.java`

## Next Steps (Optional)

- Evaluate zero-copy candidates in a separate design pass with clear API or protocol changes.
- If desired, add JMH-based benchmarks for more stable micro measurements.
