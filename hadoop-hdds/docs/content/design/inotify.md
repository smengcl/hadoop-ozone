---
title: Inotify API for Ozone
summary: Design for an inotify-style event stream for Ozone with prefix and op-type filtering
date: 2025-02-15
author: Codex
---
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Inotify API for Ozone

Ozone clients and operators often need near-real-time visibility into namespace
changes for auditing, cache invalidation, replication workflows, or downstream
synchronization. Today, Ozone exposes DB delta updates through OM for Recon
synchronization, but there is no user-facing, prefix-filterable event stream.

This document proposes an inotify-style API in Ozone that supports:
- path prefix filtering
- event type filtering by read vs write
- resumable sequencing
- best-effort IN_ACCESS events

The design is intentionally conservative. It reuses existing OM DB delta updates
(DBUpdates) for write events and adds a small, in-memory ring buffer for read
access events. This minimizes new persistence and avoids changing OM’s write
path for the initial release.

## Goals

- Provide a readable, stable API to watch filesystem events.
- Support path prefix filtering with optional recursion control.
- Allow clients to resume from sequence cursors.
- Make IN_ACCESS available with negligible overhead per read open.
- Scale to hundreds of listeners without per-listener state in OM.

## Non-Goals

- Track every individual data read chunk.
- Implement Linux inotify watch descriptor semantics.
- Provide guaranteed delivery when WAL history or the access buffer is lost.

## Key Constraints

- OM DB deltas are recorded in RocksDB WAL and exposed via `getDBUpdates(...)`.
- DBUpdates can be expensive if the caller is far behind.
- Reads do not mutate OM DB, so IN_ACCESS must be recorded separately.
- In HA, read access events are not replicated across OM nodes.

## Overview of the Proposed Design

The design separates the event stream into two sources:

1) **Write events** from OM DBUpdates, decoded on OM into filesystem events.
2) **Read access events** from a lightweight OM ring buffer, triggered on
   read-open/key-lookup RPCs.

Both streams are returned by a unified inotify API that supports op-type
filters: READ, WRITE, or READ_AND_WRITE.

## API Design

### Request

The inotify request accepts two cursors and filtering options:

- `writeSequenceNumber` (required): cursor for write events.
- `accessSequenceNumber` (optional): cursor for access events.
- `limitCount` (optional): max number of events returned.
- `pathPrefix` (optional): path prefix filter.
- `recursive` (optional, default true): subtree matching.
- `opType` (optional): READ, WRITE, READ_AND_WRITE.

### Response

The response returns current cursors and data for both streams:

- `writeSequenceNumber`, `latestWriteSequenceNumber`.
- `accessSequenceNumber`, `latestAccessSequenceNumber`.
- `events[]` (write events).
- `accessEvents[]` (read access events) or merged with `eventType=ACCESS`.
- `overflow` (WAL gap for write events).
- `accessOverflow` (ring buffer wrap for access events).
- `dbUpdateSuccess` (mirrors DBUpdates failure state).

### Event Format

Each event includes:
- `eventType`: CREATE, DELETE, MODIFY, MOVED_FROM, MOVED_TO, ATTRIB,
  CLOSE_WRITE, ACCESS.
- `path` (and `srcPath` for rename events).
- `isDir`.
- `timestamp` (optional).
- `sequenceNumber`.

### Filesystem API

OzoneFS exposes a `getInotifyEventInputStream(...)` method and advertises
support via `OzonePathCapabilities`.

## Event Source Details

### Write Events (DBUpdates-backed)

OM already supports `getDBUpdates(...)`. The inotify service reuses the same
RocksDB WAL scan (`getUpdatesSince(...)`) but decodes write batches on the OM
side to emit filesystem-level events. The decoding logic follows the same
pattern as Recon’s `OMDBUpdatesHandler`, but is hosted in OM so it can resolve
FSO paths.

Mapping rules (initial set):
- PUT on `KEY_TABLE` or `FILE_TABLE`: CREATE if new, MODIFY if existing.
- DELETE on `KEY_TABLE` or `FILE_TABLE` or `DELETED_*`: DELETE.
- OPEN_* write followed by final commit: CLOSE_WRITE.
- Rename operations: MOVED_FROM and MOVED_TO.
- SetTimes or ACL updates: ATTRIB.

### Read Access Events (OM ring buffer)

IN_ACCESS is defined as “open for read / key lookup.” These operations are
handled by OM and provide a stable trigger point that does not require
instrumenting data reads at the DN.

Implementation:
- A fixed-size ring buffer stores `{seq, event}` entries.
- A global `accessSeq` increments on each read-open event.
- On overflow, the buffer overwrites older entries.
- Access events are read via `getAccessEvents(sinceSeq, max)`.

This design makes IN_ACCESS best-effort. If the buffer wraps, clients receive
`accessOverflow=true` and must resume from the latest sequence.

## Filtering Semantics

### Prefix Filtering

- Events are filtered after resolving the full path.
- For rename events, either old or new path may match.
- `recursive=false` matches exact path or direct children only.

### Operation Type Filtering

- READ: only ACCESS events.
- WRITE: all non-ACCESS events.
- READ_AND_WRITE: returns both (default).

## Resource and Performance Considerations

### DBUpdates Cost

`RDBStore.getUpdatesSince(...)` scans RocksDB WAL and buffers write batches in
memory until `limitCount` or `ozone.om.delta.update.data.size.max.limit`
(default 1024MB) is reached. Large lag can be CPU, IO, and heap intensive.

Mitigations:
- Conservative defaults for `limitCount` in the inotify API.
- Optional lower size cap for inotify responses.
- Early filtering after decode to limit response size.
- Metrics for bytes scanned, decode time, and response size.

### Access Ring Buffer Cost

- O(1) per event (counter increment + slot write).
- No per-listener state, so hundreds of listeners are supported.
- Pull-based, so clients control read frequency.

## OM HA Impact

- DBUpdates are served from the local OM RocksDB WAL. Followers can be behind
  the leader, so `latestWriteSequenceNumber` may be stale if a client hits a
  follower. Clients should target the leader for inotify.
- On leader failover, WAL availability may cause `overflow=true` for write
  events if the cursor is too old on the new leader. Clients must resync.
- IN_ACCESS ring buffer is not replicated across OM nodes. Followers cannot
  serve IN_ACCESS, and access events are lost on leader change. The stream will
  be aborted on failover; clients should reconnect to the new leader and resume.
- Optional enhancement: include leader identity and term in responses so
  clients can detect leader changes and reset cursors proactively.

## Trade-offs and Alternatives

### DBUpdates-backed approach (proposed)

Pros:
- Reuses existing WAL infrastructure and sequencing.
- No new write amplification for write events.
- HA-friendly with existing OM leader semantics.

Cons:
- WAL scans can be heavy for large lag.
- Filtering is post-decode, not pre-scan.
- WAL truncation can force overflow/resync.

### Dedicated Event Log/Table

Pros:
- Smaller, purpose-built entries.
- Can be indexed by prefix for efficient filtering.
- Longer retention independent of WAL.

Cons:
- Adds write amplification on OM critical path.
- New persistence, retention, and compaction complexity.

### Push Streaming

Pros:
- Lower polling overhead, near-real-time delivery.

Cons:
- Requires backpressure and per-listener state.
- Still needs a source (DBUpdates or log).

Decision:
- Start with DBUpdates for write events and a ring buffer for IN_ACCESS.
- Revisit a dedicated event log if DBUpdates overhead proves too high.

## Failure Semantics

- WAL gap: `overflow=true`, client resync from latest.
- DBUpdates failure: `dbUpdateSuccess=false`, no events.
- Ring buffer wrap: `accessOverflow=true`, client resync from latest.

## CLI: `ozone sh watch`

The `ozone sh watch` command tails inotify events from a given prefix. It
supports read/write filters, JSON output, and sequence resumption.

Usage mockups:

```
ozone sh watch o3://vol1/bucket1/prefix/
```

```
ozone sh watch --recursive=false o3://vol1/bucket1/dir/
```

```
ozone sh watch --types=create,delete,move,access o3://vol1/bucket1/
```

```
ozone sh watch --op-type=read o3://vol1/bucket1/
```

```
ozone sh watch --since-seq 12345 o3://vol1/bucket1/
```

```
ozone sh watch --json o3://vol1/bucket1/
```

Default output format:

```
[seq=12346 ts=2024-07-10T12:01:02Z] ACCESS  o3://vol1/bucket1/a.txt
[seq=12347 ts=2024-07-10T12:01:03Z] CREATE  o3://vol1/bucket1/new.txt
[seq=12348 ts=2024-07-10T12:01:04Z] MOVE    o3://vol1/bucket1/old -> o3://vol1/bucket1/new
```

## Open Questions

- Which OM read RPCs should emit IN_ACCESS (exact list)?
- Access control model: admin-only vs read-permission.
- Merge access events with write events or keep separate lists.
- Precise `recursive=false` behavior on edge cases (trailing slash).
