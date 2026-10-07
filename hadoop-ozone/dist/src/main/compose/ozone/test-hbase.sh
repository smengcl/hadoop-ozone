#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#suite:misc

# HBase keeps hbase.rootdir and its WALs on Ozone (ofs://). Rows are put without a flush, so they only live in the
# memstore and in the hsync'ed WAL. After a kill -9 of the region server, and then of master and region server
# together, HBase must recover the WAL lease (LeaseRecoverable.recoverLease), split and replay the WAL, and serve
# every row again. The run ends with flush, major compaction and a graceful restart.

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=false
export OZONE_REPLICATION_FACTOR=3
export COMPOSE_FILE=docker-compose.yaml:hbase.yaml

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

TABLE=t1
ROWS=200
WAL_DIR=ofs://om/vol1/bucket1/hbase-wal

fail() {
  echo "FAILED: $*" >&2
  exit 1
}

# runs the HBase shell commands given on stdin; the shell exits non zero on an error in non interactive mode
hbase_shell() {
  execute_command_in_container hbase-master hbase shell -n
}

# "list" starts answering while the master is still initializing and create would hit PleaseHoldException, so prove the
# master can actually assign a region by creating and dropping a throwaway table. Idempotent, so it is safe on restarts.
wait_for_hbase() {
  wait_for_execute_command hbase-master 900 "hbase shell -n <<'PROBE' 2>/dev/null | grep -q 'row(s)'
disable 'hbaseozone_probe' rescue nil
drop 'hbaseozone_probe' rescue nil
create 'hbaseozone_probe', 'f'
disable 'hbaseozone_probe'
drop 'hbaseozone_probe'
list 'nonexistent_table_name'
PROBE" || fail "HBase did not come up"
}

# put rows $1..$2 with the default durability (every put is synced to the WAL), no flush
put_rows() {
  hbase_shell <<EOF >/dev/null || fail "putting rows $1..$2"
($1..$2).each { |i| put '${TABLE}', "row#{i}", 'f:c', "v#{i}" }
EOF
}

row_count() {
  echo "count '${TABLE}'" | hbase_shell 2>/dev/null | grep -oE '^=> [0-9]+' | cut -d' ' -f2
}

# regions come back only after WAL splitting and replay, so poll
assert_row_count() {
  local actual
  SECONDS=0
  while [[ ${SECONDS} -lt 900 ]]; do
    # testlib.sh sets -e: a count that fails while regions are in transition must not end the script
    actual=$(row_count) || true
    [[ "${actual}" == "$1" ]] && return
    sleep 10
  done
  fail "expected $1 rows, got '${actual}' ($2)"
}

# count of log lines matching $2 in container $1
log_count() {
  docker-compose logs --no-log-prefix "$1" 2>/dev/null | grep -c -- "$2" || true
}

assert_log_grew() {
  local now
  now=$(log_count "$1" "$2")
  [[ "${now}" -gt "$3" ]] || fail "no new '$2' in $1 log ($4)"
}

# HBase logs every WAL lease recovery with its attempt number and duration
print_lease_recovery() {
  echo "lease recovery ($1):"
  docker-compose logs --no-log-prefix "$2" 2>/dev/null | grep -E 'Recovered lease|Failed to recover lease|Cannot recoverLease' | sed 's/^/  /'
}

# WAL files HBase currently has open: ozone admin om list-open-files shows parent IDs for FSO, so match the file name
open_wal_files() {
  execute_command_in_container om ozone admin om list-open-files --service-host om | grep -F 'hbase-regionserver%2C'
}

# names of the region server WAL files that are open
open_wal_names() {
  open_wal_files | grep -oE 'hbase-regionserver%2C[^[:space:]/]+' | sort -u || true
}

start_docker_env

wait_for_hbase
# the HBase master created the bucket itself through mkdirs; it must be FSO for lease recovery
execute_command_in_container om ozone sh bucket info /vol1/bucket1 | grep -q FILE_SYSTEM_OPTIMIZED || fail "bucket layout"

## 1. rows only in memstore and WAL: the WAL is an open, hsync'ed key, visible with its synced length
hbase_shell <<EOF >/dev/null || fail "creating table"
create '${TABLE}', 'f'
EOF
put_rows 1 ${ROWS}
[[ "$(row_count)" == "${ROWS}" ]] || fail "rows after put"
open_wal_files | grep -q -E "Yes[[:space:]]" || fail "region server WAL is not an open hsync'ed key: $(open_wal_files)"
execute_command_in_container om ozone fs -ls -R "${WAL_DIR}/WALs" | grep -E '^-' | awk '$5 == 0 {exit 1}' || fail "WAL file has no hsync'ed data: $(execute_command_in_container om ozone fs -ls -R "${WAL_DIR}/WALs")"

## 2. kill -9 the region server: the master runs a ServerCrashProcedure, the restarted region server recovers the WAL
##    lease (OM rejects recovery inside ozone.om.lease.soft.limit, HBase retries), splits and replays it
recovered_before=$(log_count hbase-regionserver 'Recovered lease')
split_before=$(log_count hbase-regionserver 'Processed [0-9]* edits')
wals_before_kill=$(open_wal_names)
[[ -n "${wals_before_kill}" ]] || fail "no open region server WAL before the kill"
killed_at=$(date +%s)
docker-compose --ansi never kill -s SIGKILL hbase-regionserver
start_containers hbase-regionserver
assert_row_count ${ROWS} "after region server kill -9"
# "Recovered lease" is logged for a closed file as well, so check in OM that the WALs of the killed server were closed
still_open=$(comm -12 <(echo "${wals_before_kill}") <(open_wal_names))
[[ -z "${still_open}" ]] || fail "WAL of the killed region server is still an open key: ${still_open}"
echo "all ${ROWS} rows readable $(( $(date +%s) - killed_at ))s after region server kill -9"
assert_log_grew hbase-regionserver 'Recovered lease' "${recovered_before}" "WAL lease recovery after region server kill -9"
assert_log_grew hbase-regionserver 'Processed [0-9]* edits' "${split_before}" "WAL split after region server kill -9"
print_lease_recovery "region server kill -9" hbase-regionserver

## 3. kill -9 master and region server: the master also recovers and replays its own MasterData WAL
put_rows $((ROWS + 1)) $((ROWS + 50))
ROWS=$((ROWS + 50))
recovered_before=$(log_count hbase-regionserver 'Recovered lease')
master_recovered_before=$(log_count hbase-master 'Recovered lease')
killed_at=$(date +%s)
docker-compose --ansi never kill -s SIGKILL hbase-master hbase-regionserver
start_containers hbase-master hbase-regionserver
wait_for_hbase
assert_row_count ${ROWS} "after master and region server kill -9"
echo "all ${ROWS} rows readable $(( $(date +%s) - killed_at ))s after master and region server kill -9"
assert_log_grew hbase-regionserver 'Recovered lease' "${recovered_before}" "WAL lease recovery after full kill -9"
assert_log_grew hbase-master 'Recovered lease' "${master_recovered_before}" "master store WAL lease recovery after full kill -9"
print_lease_recovery "full kill -9, master" hbase-master
print_lease_recovery "full kill -9, region server" hbase-regionserver

## 4. flush, major compaction, graceful restart
put_rows $((ROWS + 1)) $((ROWS + 50))
ROWS=$((ROWS + 50))
hbase_shell <<EOF >/dev/null || fail "flush and major compaction"
flush '${TABLE}'
major_compact '${TABLE}'
EOF
wait_for_execute_command hbase-master 300 "echo \"compaction_state '${TABLE}'\" | hbase shell -n | grep -q NONE" || fail "major compaction did not finish"
docker-compose --ansi never stop -t 120 hbase-regionserver hbase-master
start_containers hbase-master hbase-regionserver
wait_for_hbase
assert_row_count ${ROWS} "after graceful restart"

## evidence: Ozone FileSystem calls HBase made (client side counters) and the matching OM audit entries
for c in hbase-master hbase-regionserver; do
  echo "${c} Ozone FileSystem operations:"
  docker-compose logs --no-log-prefix "${c}" 2>/dev/null | grep -oE 'OzoneFSStorageStatistics: op_[a-z_]+ \+= [0-9]+  ->  [0-9]+' \
    | awk '{last[$2] = $6} END {for (op in last) print "  " op " " last[op]}' | sort || true
done
echo "OM audit:"
execute_commands_in_container om 'grep -oE "op=[A-Z_]+ |hsync" /var/log/hadoop/om-audit-*.log | sort | uniq -c | sort -rn' | sed 's/^/  /'

echo "HBase on Ozone WAL recovery test PASSED"
