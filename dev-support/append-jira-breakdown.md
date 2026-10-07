<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Append support: proposed Jira breakdown

Local draft. Nothing here has been filed. Source: `hadoop-hdds/docs/content/design/append.md` on branch `HDDS-4333-append-oep`. Umbrella: [HDDS-4333](https://issues.apache.org/jira/browse/HDDS-4333). Design subtask: [HDDS-4373](https://issues.apache.org/jira/browse/HDDS-4373).

Each row is one proposed subtask of HDDS-4333, sized to be one reviewable PR. IDs (`AP-nn`) are placeholders until real Jira keys exist. "Depends on" lists hard ordering only. The feature stays disabled (`ozone.om.append.enabled=false`) until phase 6.

The "POC" column records what the local proof of concept on branch `HDDS-4333-append-POC` covers: `done` (implemented and tested there), `partial` (simplified, see the note), or `no` (not attempted).

## Phase 0. Design

| ID | Summary | Scope | Depends on | POC |
| --- | --- | --- | --- | --- |
| AP-00 | Add append design documentation (HDDS-4373) | Review and merge the OEP. Record decisions on ordinary writer admission, replicated renewal, rename support, and partial reclamation. | | done (local) |

## Phase 1. Protocol, metadata, and gates

| ID | Summary | Scope | Depends on | POC |
| --- | --- | --- | --- | --- |
| AP-01 | Add append session metadata to OmKeyInfo | `appendOwnerSessionId` on the committed record. `appendSession` (phase, prefixLength, prefixBlockCount, openedAt, lastRenewedAt) on the open record. Protobuf fields, both codecs, `copyObject`, builder. Codec round trip tests. Snapshot copies never authorize a writer. | AP-00 | done |
| AP-02 | Add APPEND layout feature, `ozone.om.append.enabled`, and result codes | New `OMLayoutFeature`. Runtime flag (default false) documented in `ozone-default.xml`. Admission requires both. New `Status` codes for append conflicts and unsupported append. Old OM and pre-finalization rejection tests. | AP-00 | done |
| AP-03 | Add AppendFile and RenewAppendLeases to the OM client protocol | Protobuf messages, `OzoneManagerProtocol`, client side translator, `OmUtils` request classification. Conflict reply carries writer kind, session, phase, applied `lastRenewedAt`, and remaining time until recovery eligibility. | AP-01, AP-02 | done |

## Phase 2. OM ownership and session lifecycle

| ID | Summary | Scope | Depends on | POC |
| --- | --- | --- | --- | --- |
| AP-04 | OM admission for AppendFile (FSO) | `OMFileAppendRequest`: resolve file, check layout, ACL, gate, existing append owner, ordinary hsync writer, and any ordinary open session (cache aware lookup limited to the file). Reserve the committed record and create the suffix only open record in one transaction. Audit and metrics. | AP-03 | done (also checks the file ACLs with the native authorizer) |
| AP-05 | Derived append session index | In memory map from bucket scoped session ID to current open DB key. Updated on admission, rename, and session removal during Ratis application. Rebuilt at startup and checkpoint installation. Readiness before serving session RPCs. | AP-04 | partial (built lazily by a full open table scan) |
| AP-06 | Session scoped AllocateBlock for append | Resolve the open record through the session index. Require matching owner and `ACTIVE` phase. Add allocations to the suffix only. Revalidate SCM allocations at apply time. | AP-05 | done (authorized against the file of the session) |
| AP-07 | Append publication on hsync and close | Commit merges the committed prefix with the validated suffix under the bucket lock. Preserve concurrent attribute updates and the published lower bound. Charge the quota delta once. Hsync keeps ownership; close clears the owner, removes the open record and index entry, and hands unused private allocations to deletion. Empty append keeps contents, length, and mtime. | AP-06 | done (authorized against the file of the session) |
| AP-08 | Fence ordinary writers against append reservations | Reject create, overwrite, and rewrite while append owns the file. Every commit path (including older clients) fails when the committed record is owned by a different session. Late commit of a terminated overwrite fails during and after append. | AP-04 | done (clients older than append get `NOT_SUPPORTED_OPERATION`) |
| AP-09 | Replicated lease renewal | `OMAppendLeaseRenewRequest`: bounded batch, per session result, leader supplied timestamp, monotonic `lastRenewedAt`, no ancestor walk, no mtime change. | AP-05 | partial (no metrics) |
| AP-10 | Rename while append is active | FSO file rename moves the open row with the committed row and updates the index. Ancestor rename needs no row rewrite. Session RPCs never resolve the stale path. A new file at the old path never receives the session's blocks. Remove the FSO hsync rename rejection only for append owners. | AP-05, AP-07 | done |
| AP-11 | Delete and recursive delete with an active append session | Direct delete marks the open record `INVALIDATED` and removes the index entry. Reachability guard for allocate, publish, and recovery after an ancestor was deleted. `DirectoryDeletingService` invalidates unreachable sessions, including descendants pinned by a snapshot. | AP-05, AP-07 | partial (descendants kept by a snapshot are not invalidated) |
| AP-12 | Append aware lease recovery | `OMRecoverLeaseRequest` recognizes append owners, including before the first hsync. `ACTIVE` to `RECOVERING` fences the writer. Completion publishes the validated suffix, never below the published lower bound, and removes the session. Soft limit uses `lastRenewedAt`. Repeated recovery joins the existing phase. | AP-07, AP-09 | done |
| AP-13 | Append aware open key expiry | `getExpiredOpenKeys()` classifies append sessions before the generic and hsync branches. Hard limit applies to `lastRenewedAt`. `INVALIDATED` rows are cleaned regardless of age. Published suffix blocks are never moved to the deleted table from the open row. | AP-11, AP-12 | done |
| AP-14 | LEGACY bucket layout support | Same admission, publication, rename, recovery, and cleanup on `keyTable` and `openKeyTable`. Layout aware `OMRecoverLeaseRequest`. Reject OBJECT_STORE. No change to LEGACY path normalization. | AP-07, AP-10, AP-12 | done (batch rename skips a reserved key; no recovery of plain hsync keys in LEGACY) |
| AP-15 | Writer state audit | Appendix B: enumerate `HSYNC_CLIENT_ID`, `isHsync()`, and append field call sites, record the disposition of each, and add classification tests (ordinary, hsync, append before and after hsync, EC append, snapshot copy). | AP-13 | partial (lof, abort, expiry, recovery classify; no full audit) |

## Phase 3. Client and filesystem

| ID | Summary | Scope | Depends on | POC |
| --- | --- | --- | --- | --- |
| AP-16 | Client writer integration for append | `ClientProtocol.appendFile`, `RpcClient`, `OzoneBucket`. Reuse `KeyOutputStream` with a prefix length and suffix only stream entries. Commit and hsync submit suffix locations and total length. Original replication config. | AP-07 | done |
| AP-17 | Append hsync always publishes an advancing length | Append sessions bypass the same block OM skip in the shared writer. Unchanged state sync skips publication. Datanode success then OM failure leaves hsync unsuccessful. | AP-16 | done |
| AP-18 | Client lease renewer | Per client scheduled renewal through `RenewAppendLeases`, batched, independent of data progress. Stream is fenced locally when renewal reports the session gone. Interval config with margin under the soft limit. | AP-09, AP-16 | done |
| AP-19 | OzoneFS `append()` for ofs and o3fs | `append(Path, int, Progressable)` and the append builder in both filesystems and adapters. Stream positioned at EOF. Reject directories, snapshot paths, OBS, unsupported flags. `FS_APPEND` path capability from layout and server state. Fix `LEASE_RECOVERABLE` capability to check the layout. | AP-16, AP-18 | done |
| AP-20 | Append triggered recovery and bounded waiting | On an append owner conflict, poll through the read path, fail early when `lastRenewedAt` advances, drive `recoverLease` after server reported eligibility, admit at the recovered EOF. Monotonic total deadline. Same instance conflicts fail promptly. `isFileClosed()` recognizes append ownership. | AP-12, AP-19 | partial (coarse polling, no same instance fail fast) |
| AP-21 | Encrypted append | `CryptoOutputStream` with `streamOffset` at the admitted prefix length, same key version and IV. Reject GDPR and any mode that cannot resume. Tests with unaligned offsets. | AP-16 | done (GDPR rejected) |
| AP-22 | EC append | New block groups published on close through `ECKeyOutputStream`. No hsync capability. Recovery discards the unpublished suffix. Read and seek across the partial stripe boundary. | AP-16 | done (close only publication; partial and full stripe, repeated append, recovery, degraded read) |
| AP-23 | RATIS streaming append | `KeyDataStreamOutput` and its entry pool with the same prefix and suffix handling. | AP-16 | partial (client API `appendStreamFile` only; `FileSystem.append` does not stream) |
| AP-24 | Hadoop append contract test | Nested `AbstractContractAppendTest` subclass in `AbstractOzoneContractTest`. Enable `fs.contract.supports-append` for supported combinations only. | AP-10, AP-19 | done (ofs and o3fs on FSO and LEGACY) |
| AP-25 | Shell and HttpFS validation | `hadoop fs -appendToFile` and HttpFS append through the filesystem API. | AP-19 | no |

## Phase 4. Snapshots, garbage collection, and accounting

| ID | Summary | Scope | Depends on | POC |
| --- | --- | --- | --- | --- |
| AP-26 | Retain blocks shared between appended versions | Safety prerequisite. `ReclaimableKeyFilter` must not reclaim a deleted version whose blocks overlap any retained snapshot version of a different length. Conservative whole version retention first. Keep snapshot diff equality unchanged. | AP-07 | done |
| AP-27 | Bounded snapshot reads for appended files | Snapshot path reads never treat the last block as under construction and enforce captured lengths for read, seek, and EOF. Owner and session fields in snapshot copies are ignored. | AP-07 | no |
| AP-28 | Partial block reclamation | Filter returns eligible and retained block subsets with validated chain context. `getPendingDeletionKeys()` submits only eligible blocks. Update only purge requests persist the residual version after SCM acknowledgement. Namespace decremented only when the last remainder is removed. | AP-26 | partial (previous snapshot only; residual size shrinks) |
| AP-29 | Residual deletion records and replay safety | Deletion progress discriminator in the existing deleted table record. Expected state guards. Exactly once accounting across the snapshot and active stores. Snapshot move carries residual metadata. | AP-28 | no |
| AP-30 | Per block group EC replicated size | `OmKeyInfo.getReplicatedSize()` sums per group for EC. Used consistently by publication deltas, quota repair, deletion, and snapshot accounting. | AP-22 | partial (append delta, multipart overwrite and EC quota repair; snapshot filter, deleted data calculator and lifecycle not) |
| AP-31 | Snapshot diff and exclusive size for appended files | Append reports as a modification, including append then rename. Exclusive and referenced size recognize shared blocks across versions of different lengths. | AP-26 | partial (whole version exclusive size) |

## Phase 5. Operations and observability

| ID | Summary | Scope | Depends on | POC |
| --- | --- | --- | --- | --- |
| AP-32 | Show append sessions in `ozone admin om lof` | Writer kind, session ID, phase, prefix and published lengths, suffix block count, last renewal. Bounded output. | AP-04 | done |
| AP-33 | Append metrics, audit, and lifecycle logs | Counters and latencies for admission outcomes, active sessions, renewals, fencing, recovery, publication. Structured logs with session ID and phase. No tokens or full location lists. | AP-07, AP-12 | partial (audit actions, admission counters) |
| AP-34 | Blocked reclamation diagnostic | Rate limited warning and per bucket read only view naming the snapshot that blocks reference resolution. | AP-28 | no |
| AP-35 | Administrator abort of an ordinary open writer | Not part of append v1 in the design. OM Ratis request that removes one exact open record (path plus client ID), admin only, explicit confirmation in the CLI, audited, race safe against a concurrent commit, blocks handed to the existing deletion path. Late allocate and commit fail. | AP-08 | done |
| AP-36 | User and operator documentation | Feature page, configuration reference, recovery procedures, known limits (EC durability, small blocks). | AP-19 | partial (Admin.md only) |

## Phase 6. Validation and activation

| ID | Summary | Scope | Depends on | POC |
| --- | --- | --- | --- | --- |
| AP-37 | Upgrade and cross version compatibility tests | New client with old OM, old client with new OM, pre-finalization rejection, old client reads of extended files, downgrade refusal after finalization. | AP-19 | partial (pre-finalization rejection, old client status and old reader at unit level; no cross version cluster run) |
| AP-38 | Failure injection suite | OM failover and restart, lost admission and commit responses, retry cache boundaries, datanode and pipeline failure during suffix write, quota failure. | AP-20 | done (OM restart, failover, snapshot install, retry cache, datanode failure, quota) |
| AP-39 | Solr acceptance test | Docker compose environment with Solr index and transaction logs on `ofs://`. Graceful restart reopens tlogs through append. Crash restart uses explicit lease recovery before append. Documents the Solr and Hadoop versions tested. | AP-19 | done |
| AP-40 | HBase validation | WAL and recovery checks with the target HBase release, including the shared soft limit. | AP-20 | done (HBase 2.6.7 compose test; HBase does not call append) |
| AP-41 | Performance and scale evaluation | Freon append workload. Small session growth, publication latency against the create path, renewal load, session index rebuild, snapshot GC traversal. Record operating ranges. | AP-28 | partial (local Solr comparison with HDFS only, see `append-solr-benchmark.md`) |
| AP-42 | Formal model and trace validation | TLA+ model of admission, publication, recovery, snapshot, and partial GC. Validate implementation traces against it. | AP-28 | no |
| AP-43 | Enable append by default | Flip the default after AP-37 through AP-42 pass. Release note. | AP-37, AP-38, AP-41, AP-42 | no |

## Units added by the POC

Work that the POC needed and that has no row above. Each is one commit on the POC branch.

| ID | Summary | Scope | Depends on | POC |
| --- | --- | --- | --- | --- |
| AP-44 | Reject append session requests for a file under a deleted directory | Reachability guard from AP-11, split out: allocate, publish, and recovery commit of a session fail once an ancestor directory was deleted. | AP-06, AP-07, AP-11 | done |
| AP-45 | Recover only the suffix of an append session in client lease recovery | `LeaseRecoveryClientDNHandler` and `recoverLease` in ofs and o3fs finalize the unpublished suffix blocks only and never touch the committed prefix. | AP-12, AP-19 | done |
| AP-46 | Integration tests for append | Mini cluster coverage in `TestHSync` and `TestOzoneAtRestEncryption`: append, hsync, reopen, rename, delete, recovery, encrypted append. | AP-19, AP-20, AP-21 | done |
| AP-47 | Do not reclaim an uncommitted block that a snapshot references | Not append specific. A block that an hsync published and a later commit dropped must not be reclaimed while a snapshot still references it. | | done (stopgap, needs its own upstream fix) |
| AP-48 | Authorize append session requests against the file of the session | AllocateBlock, commit, renewal, and RecoverLease of a session check ACLs on the file that owns the session, also after a rename. | AP-06, AP-07, AP-09, AP-10 | done (directory walk in `preExecute`) |
| AP-49 | Read hsync'ed data beyond the length in OM with a positional read | Not append specific. A positional read past the length in OM refreshes the block list instead of returning EOF. | | done (positional read only) |
| AP-50 | Answer clients that predate append with a result code they can parse | `ClientVersion.APPEND_SUPPORT` and a request validator that maps `APPEND_WRITER_CONFLICT` to an older status for older clients. | AP-08 | done (new client version needs a decision) |
| AP-51 | Let the leader decide the soft limit of an append lease | The leader evaluates the soft limit in `preExecute` of RecoverLease and of a recovery commit and replicates the decision, so followers do not use their local clock. | AP-12 | done |
| AP-52 | Close an append session at the published length when recovery would exceed the space quota | A recovery commit over quota closes the file at its published length instead of failing forever. | AP-12 | done |
| AP-53 | Fence an append session in RecoverLease when OM cannot reach SCM | The writer is fenced even without pipelines in the reply. The client fails the recovery so that the caller retries. | AP-12, AP-45 | done |
| AP-54 | Count an appended EC key per block group in multipart overwrite and quota repair | Remaining callers of the size formula that AP-30 replaces: `S3MultipartUploadCompleteRequest` and `QuotaRepairTask`. | AP-30 | done |
| AP-55 | Answer an S3 write to a key that an append session reserved with 409 | Map the append conflict status in `S3ErrorTable`. | AP-08 | done |

## Suggested order

AP-01, AP-02, AP-03 land first and are independent of each other apart from the listed edges. After AP-07 the work splits into four mostly independent tracks: OM lifecycle (AP-09 to AP-15), client and filesystem (AP-16 to AP-25), snapshot and GC (AP-26 to AP-31), and operations (AP-32 to AP-36). AP-26 must merge before any build in which append can be enabled, since without it deleting an appended file can reclaim blocks a snapshot still references.

## POC findings to carry into the Jiras

Found while building and reviewing the POC. Each is open on the branch.

Security and authorization:

- AP-04: admission checks the ACLs in `preExecute` and reserves the file later, when the request is applied. An ACL change between the two is not seen. Closing this needs the checked identity or object ID in the replicated request (a protocol change).
- AP-06, AP-07, AP-09: a session request is authorized against the file the session belongs to, which is found again after an ancestor rename by walking the directory table in `preExecute`. A cheaper lookup is needed for deep trees.
- AP-08, AP-37: only `APPEND_WRITER_CONFLICT` is mapped to an older status for clients older than append. The other new status codes still reach such a client as an RPC failure if it can trigger them.
- AP-37: `ClientVersion.APPEND_SUPPORT` is a new client version. This is a wire compatibility change that needs a decision upstream.
- Not append specific: `DeleteOpenKeys`, `PurgeKeys`, and `PurgeDirectories` are internal request types that any RPC client can submit. The POC only makes `DeleteOpenKeys` and the multipart part commit refuse rows of an append session.
- No integration test runs append with a real authorizer (Ranger or the native authorizer in a secure cluster).

OM lifecycle:

- AP-09, AP-18: only `RenewAppendLeases` advances `lastRenewedAt`. Two missed renewals (for example a long OM failover) make a live writer recoverable. Consider renewing on hsync or deriving the client interval from the OM soft limit.
- AP-12: lease recovery returns pipelines and tokens for the last two open blocks only, so a session with more than two unpublished blocks recovers none of its unpublished data.
- AP-12: when OM cannot reach SCM, `RecoverLease` of an append session still fences the writer and answers without pipelines. The client fails and has to retry. A recovery commit that would exceed the bucket quota closes the file at its published length and drops what was found beyond it.
- AP-12, not append specific: the plain hsync branch of `RecoverLease` decides the soft limit with the local clock of each OM and calls SCM while the request is applied.
- AP-11, AP-28: an appended file under a recursively deleted directory is retained whole while the previous snapshot still has it, and sessions under snapshot retained descendants stay ACTIVE but unreachable until purge.
- AP-14: on LEGACY, a batch rename skips a reserved key, a plain hsync key has no lease recovery, and EC append is covered by unit tests only.
- AP-05: the session index is built by scanning both open tables on first use after an OM start, also when append is disabled.
- AP-32: `list-open-files` with a path prefix below the bucket returns nothing on FSO.

Snapshots and accounting:

- AP-29: the partial reclaim rewrite of a deleted table row has no expected state guard, and the residual row's `dataSize` shrinks to the retained blocks.
- AP-30: `ReclaimableKeyFilter`, `BucketDeletedDataCalculator`, and `KeyLifecycleService` still use the formula on the file size and undercount an appended EC file.
- Not append specific: a block that an hsync published and a later commit dropped is written to the deleted table under a pseudo object ID. The branch carries a stopgap. The fix belongs upstream under its own Jira.

Client:

- AP-23: datanodes keep the tail of a RATIS stream in memory until the stream is closed, so an hsync on a streaming append makes only closed blocks durable. `FileSystem.append` therefore does not use streaming. It needs a datanode change first.
- AP-23: after a block of a streaming append failed, hsync fails and only `close()` publishes the data written after it.
- AP-20: a second `append()` against a live writer waits up to one soft limit before failing, including from the same FileSystem instance. A failed append stream leaves the file reserved until the soft limit passes.
- Not append specific: a positional read beyond the length in OM (the under construction last block of an hsync'ed file) refreshes the block list on every such read. `available()` and `getFileStatus().getLen()` stay at the length in OM, and the streaming read path is not refreshed.
- Not append specific: client settings in a `core-site.xml` on the classpath are overridden by the defaults in `ozone-default.xml`, so `ozone.fs.hsync.enabled` set there is ignored and hsync silently degrades to a flush.

Applications:

- AP-39: Solr reads an open transaction log through a new stream and a positional read. Only the first hsync of a created file updates the length in OM, so realtime get depends on the positional read fix above. Solr does not recover leases on a file system that is not HDFS. After a crash the operator runs `ozone admin om lease recover` on the newest transaction log and removes `write.lock`.
- AP-40: HBase 2.6 writes its WAL with create, hsync, and `recoverLease`, and never calls `append()`.
- AP-39: compose scripts go through Maven resource filtering, so shell variables such as `${id}` are replaced at build time.
- AP-37: no run with mixed versions in one cluster.
