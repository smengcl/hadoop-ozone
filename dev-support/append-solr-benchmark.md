# Solr on Ozone compared with Solr on HDFS: local benchmark plan

Local draft for HDDS-4333. The goal is a relative comparison on one machine, not absolute numbers. Results go in the last section after the runs.

## Question

With Solr storing its index and transaction log (tlog) through `HdfsDirectoryFactory`, how do indexing, per update durability, query, and restart behave on `ofs://` (Ozone with append) compared with `hdfs://`?

## Environment

- One host, Docker, images for the native architecture (arm64 on this machine). One storage stack runs at a time, each run starts from empty volumes.
- Solr: `solr:9.11` with `SOLR_MODULES=hdfs`, one node, one core from the `_default` configset, the same heap (`SOLR_HEAP=2g`) and the same options for both file systems. Only `solr.hdfs.home` and the client configuration directory differ.
- Ozone: the distribution built from this branch, SCM, OM and three datanodes, RATIS/THREE, `ozone.om.append.enabled=true`, `ozone.fs.hsync.enabled=true`, `ozone.hbase.enhancements.allowed=true`, `ozone.client.hbase.enhancements.allowed=true`. S3 Gateway, Recon and HttpFS are not started.
- HDFS: Apache Hadoop 3.4.x, one NameNode and three DataNodes, replication 3, default client settings.
- Recorded with every run: image digests, Ozone commit, Hadoop version, Docker CPU and memory limits, host load average before the run.

## Solr profiles

| Profile | Settings | Purpose |
| --- | --- | --- |
| raw | block cache off, NRT caching directory off | Shows the file system itself |
| default | Solr defaults for the hdfs module (block cache on, NRT caching on) | Closer to how Solr is deployed |

`autoSoftCommit` is off in both. `autoCommit` is off for the update and restart workloads and 15 s with `openSearcher=false` for bulk indexing.

## Workloads

| ID | What it does | What it measures | Metrics |
| --- | --- | --- | --- |
| W1 update | One client thread, one document per request, no commit, 5,000 requests | The tlog path: every update ends in `hflush` on the tlog | Latency p50, p95, p99, requests per second |
| W2 bulk | 8 client threads, batches of 1,000 documents of about 1 KB, 200,000 documents, one commit at the end | Segment writes, merges, tlog rolls | Documents per second, total time, index size on the file system |
| W3 query | After W2: 5,000 term queries with `rows=10`, with 1 and with 8 threads, first right after a Solr restart (cold), then repeated (warm) | Index reads | Latency p50, p95, p99, queries per second |
| W4 restart | 20,000 documents indexed without commit, then (a) graceful stop and start, (b) `kill -9` and start | Tlog reopen through `append()` and replay | Time from container start until the core answers and until all documents are searchable |

Notes on W4: after `kill -9` the newest tlog was created and hsync'ed but never closed. On Ozone the test recovers its lease with `ozone admin om lease recover` before starting Solr, with the lease soft limit set to 10 s. On HDFS Solr recovers the lease itself. The lease wait is reported separately from the replay time. The stale `write.lock` is removed on both.

## Driver

A Python 3 script on the host that uses only the standard library (`http.client`, threads). It generates documents from a fixed seed, talks to the published Solr port, and writes one CSV row per run with all latencies summarized. The same script and the same parameters are used for both file systems.

## Procedure

1. Start the stack, wait for safe mode exit and three healthy datanodes, create the Solr home directory.
2. Start Solr, create the core, run a short warm up (500 updates, discarded).
3. Run W1, W2, W3 and W4 in that order. Recreate the core between W1 and W2.
4. Tear the stack down with its volumes.
5. Repeat three times for each file system and profile, alternating the order (Ozone, HDFS, HDFS, Ozone, ...) so that drift on the host does not favor one side.
6. Report the median of the three runs with the minimum and maximum.

The runs happen while nothing else heavy uses the machine.

## How to read the results

- Single host, Docker Desktop VM, loopback network and one shared disk. Network and disk parallelism of a real cluster are absent, so only ratios between the two file systems are meaningful, and only roughly.
- Solr calls `hflush` on the tlog after every update. On Ozone `hflush` is `hsync`: the data is written through the Ratis pipeline and synced on three datanodes. On HDFS `hflush` sends the data to the datanodes without a disk sync. W1 therefore compares two different durability levels.
- Only the first hsync of a created file contacts OM. An append session publishes the file length to OM on every advancing hsync, which applies to a tlog that Solr reopened after a restart. The OM request count per update is reported for W1.
- Small tlogs waste little space on either system, but each append session on Ozone starts a new block. The block count per tlog after W4 is reported.

## Results

Run on 2026-10-07 with Ozone at 86df5de6c52 of this branch, Hadoop 3.4.3, Solr 9.11.0 (`solr:9.11`), Docker 29.8.2 on arm64 with 14 CPUs and 23 GiB for the Docker VM. Twelve passes (two file systems, two profiles, three repetitions) took 42 minutes. All passed, with no request errors. The host was not idle: another job kept the 1 minute load average between 4.6 and 10.5 during the passes. The order of the file systems alternated, and the spread within a file system is small compared with the differences below.

Values are the median of three passes, with the minimum and maximum in parentheses where the spread matters.

| Workload | Metric | Profile | Ozone | HDFS | Ozone / HDFS |
| --- | --- | --- | --- | --- | --- |
| W1 update | latency p50, ms | raw | 4.48 (4.42 to 5.04) | 0.79 (0.65 to 0.83) | 5.7 |
| W1 update | latency p99, ms | raw | 13.7 | 1.48 | 9.3 |
| W1 update | requests per second | raw | 197 | 1181 | 0.17 |
| W1 update | latency p50, ms | default | 4.51 | 0.74 | 6.1 |
| W1 update | requests per second | default | 201 | 1244 | 0.16 |
| W1 update | OM requests per update | both | 0 | | |
| W2 bulk | documents per second | raw | 31162 (16725 to 31396) | 43509 | 0.72 |
| W2 bulk | documents per second | default | 26494 | 46035 | 0.58 |
| W2 bulk | index size, MB | both | 257 | 257 | 1.00 |
| W3 query, warm, 1 thread | latency p50, ms | raw | 23.2 | 9.56 | 2.4 |
| W3 query, warm, 8 threads | queries per second | raw | 208 | 427 | 0.49 |
| W3 query, cold, 1 thread | latency p50, ms | raw | 24.1 | 8.16 | 3.0 |
| W3 query, warm, 1 thread | latency p50, ms | default | 0.60 | 0.59 | 1.0 |
| W3 query, warm, 8 threads | queries per second | default | 7851 | 7809 | 1.0 |
| W3 query, cold, 1 thread | latency p50 and p99, ms | default | 0.76 and 16.2 | 0.79 and 8.16 | 1.0 and 2.0 |
| W3 query, cold, 8 threads | queries per second | default | 1662 | 2832 | 0.59 |
| W4 graceful restart | start until all documents are searchable, s | raw | 3.46 | 2.27 | 1.5 |
| W4 graceful restart | same | default | 3.51 | 2.18 | 1.6 |
| W4 kill -9 | lease recovery wait, s | both | 10.2 | 4.0 | 2.5 |
| W4 kill -9 | start until the core answers, s | raw | 3.56 | 6.23 | 0.57 |
| W4 kill -9 | start until all documents are searchable, s | raw | 29.7 | 17.8 | 1.7 |
| W4 kill -9 | same | default | 8.32 | 10.0 | 0.83 |
| W4 kill -9 | blocks of the recovered tlog | both | 2 | | |

The full table with every percentile is produced by `summarize.py` from `results/full.csv` of the harness.

Reading:

- Per update durability (W1) is where Ozone costs most: about 4.5 ms against 0.8 ms per update, which limits one indexing thread to about 200 updates per second. Each update is a synced Ratis write on three datanodes on Ozone and an unsynced pipeline write on HDFS, so this is not the same guarantee. No OM request is made per update: the tlog is a created file, and only its first hsync goes to OM.
- Bulk indexing (W2) reaches 60 to 70 percent of the HDFS throughput. The index size is the same.
- Queries (W3) depend on the Solr block cache. With the default caches, warm queries are equal on both, and cold queries have the same median with a tail that is two to three times longer on Ozone. Without the caches every query reads from the file system and Ozone is 2.2 to 3 times slower per read.
- Graceful restart (W4) reopens the tlog through `append()` and takes about 1.3 s longer on Ozone.
- After `kill -9`, HDFS needs no operator: Solr recovers the lease itself in 4 s. On Ozone the operator (here the script) runs `ozone admin om lease recover` and removes `write.lock`, and has to wait for the lease soft limit, which the harness sets to 10 s (the default is 60 s). Once Solr starts, the core answers sooner on Ozone because the lease is already recovered. Replay of the 20,000 uncommitted documents is slower on Ozone without the caches (26 s against 12 s) and closer with them (5 s against 4 s).
- The tlog that went through `kill -9`, recovery and reopen has two blocks: the append session started a new block.

Deviations from the plan above:

- The Ozone block and container sizes are the Ozone defaults (256 MB and 5 GB), not the 1 MB and 1 GB of the compose test configuration.
- HDFS runs from the Apache arm64 binary tarball on `eclipse-temurin:17-jre`, since `apache/hadoop` images are amd64 only.
- W4 runs on a new empty core, so it measures tlog reopen and replay without opening the W2 index.
- Hsync requests to OM are counted from the OM metric `NumKeyHSyncs`, since the audit log has no separate entry for them.
- Cold queries run after a Solr restart. The page cache of the datanodes is not dropped.

Limits: one host, one disk, loopback network, a busy machine, and three passes. The ratios show where the two file systems differ and by roughly how much. They are not capacity numbers for a cluster.
