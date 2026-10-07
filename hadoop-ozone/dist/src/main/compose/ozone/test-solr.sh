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

# Solr keeps index + tlog on Ozone (ofs://). Each Solr restart reopens every existing tlog with
# FileSystem.append(); a kill -9 additionally requires Ozone lease recovery and stale lock removal
# before Solr can replay the tlog. Docs are indexed without commit so they only live in the tlog.

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=false
export OZONE_REPLICATION_FACTOR=3
export COMPOSE_FILE=docker-compose.yaml:solr.yaml

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

SOLR_URL=http://localhost:8983/solr
CORE=test
SOLR_HOME=ofs://om/vol1/bucket1/solr
TLOG_DIR=${SOLR_HOME}/${CORE}/data/tlog
INDEX_DIR=${SOLR_HOME}/${CORE}/data/index

fail() {
  echo "FAILED: $*" >&2
  exit 1
}

solr_curl() {
  execute_command_in_container solr curl -sf "${SOLR_URL}/$1"
}

wait_for_solr_core() {
  wait_for_execute_command solr 120 "curl -sf ${SOLR_URL}/${CORE}/admin/ping | grep -q '\"status\":\"OK\"'"
}

# index docs with ids $1..$2, no commit: they are only in the tlog
index_docs() {
  local json="[" sep=""
  # the loop variable must not be named "id": Maven resource filtering would replace it with the project id
  for doc in $(seq "$1" "$2"); do
    json+="${sep}{\"id\":\"${doc}\",\"title\":\"doc ${doc}\"}"
    sep=","
  done
  json+="]"
  execute_command_in_container solr curl -sf -X POST "${SOLR_URL}/${CORE}/update" -H 'Content-Type: application/json' -d "${json}" >/dev/null \
    || fail "indexing docs $1..$2"
}

num_found() {
  solr_curl "${CORE}/select?q=*:*&rows=0" | grep -o '"numFound":[0-9]*' | cut -d: -f2
}

# tlog replay runs in the background after the core is up, so wait for the expected count
assert_num_found() {
  local actual
  SECONDS=0
  while [[ ${SECONDS} -lt 60 ]]; do
    # testlib.sh sets -e: a request that fails while Solr is busy must not end the script
    actual=$(num_found) || true
    [[ "${actual}" == "$1" ]] && return
    sleep 2
  done
  fail "expected numFound=$1, got ${actual} ($2)"
}

file_size() {
  execute_command_in_container om ozone fs -ls "$1" | awk '{size=$5} END {print size}'
}

# Solr logs this (and then DELETES the tlog) when FileSystem.append() on an existing tlog throws.
tlog_open_failures() {
  docker-compose logs --no-log-prefix solr 2>/dev/null | grep -c "Failure to open existing log file" || true
}

assert_no_new_tlog_open_failure() {
  local now
  now=$(tlog_open_failures)
  [[ "${now}" == "$1" ]] || fail "Solr failed to reopen (append) an existing tlog: $(docker-compose logs --no-log-prefix solr | grep -A3 'Failure to open existing log file' | tail -4)"
}

assert_tlog_count() {
  local actual
  actual=$(execute_command_in_container om ozone fs -ls "${TLOG_DIR}" | grep -c "/tlog\.") || true
  [[ "${actual}" == "$1" ]] || fail "expected $1 tlog file(s) in ${TLOG_DIR}, got ${actual}: $(execute_command_in_container om ozone fs -ls "${TLOG_DIR}")"
}

recover_lease() {
  local path=$1
  SECONDS=0
  while [[ ${SECONDS} -lt 180 ]]; do
    if execute_command_in_container om ozone admin om lease recover --path "${path}" | grep -q SUCCEEDED; then
      return
    fi
    sleep 5
  done
  fail "lease recovery timed out on ${path}"
}

start_docker_env

execute_command_in_container om ozone sh volume create /vol1
execute_command_in_container om ozone sh bucket create --layout FILE_SYSTEM_OPTIMIZED /vol1/bucket1

wait_for_execute_command solr 120 "curl -sf ${SOLR_URL}/admin/info/system >/dev/null"
execute_command_in_container solr solr create -c "${CORE}"
wait_for_solr_core

## 1. index without commit: docs only in the hsync'ed tlog, readable through realtime get
# Two updates: only the first hsync of a new file updates its length in OM, so doc 3 lies beyond that length.
index_docs 1 2
index_docs 3 3
sleep 2
[[ "$(num_found)" == "0" ]] || fail "docs are searchable without a commit"
solr_curl "${CORE}/get?id=1" | grep -q '"id":"1"' || fail "realtime get from open tlog"
solr_curl "${CORE}/get?id=3" | grep -q '"id":"3"' || fail "realtime get from open tlog beyond the length in OM"
assert_tlog_count 1

## 2. graceful restart: Solr commits on close and closes the tlog, then on startup reopens it with append()
baseline=$(tlog_open_failures)
docker-compose --ansi never stop -t 60 solr
assert_tlog_count 1
start_containers solr
wait_for_solr_core
assert_no_new_tlog_open_failure "${baseline}"
assert_tlog_count 1
assert_num_found 3 "after graceful restart"

## 3. kill -9: the current tlog stays open (created and hsync'ed, never closed), so it needs explicit lease
##    recovery before append() can be admitted. Remove the stale index lock, then Solr must append() to the
##    tlogs, replay docs 4..5 and append a commit record to the replayed tlog.
index_docs 4 5
baseline=$(tlog_open_failures)
docker-compose --ansi never kill -s SIGKILL solr
tlog=$(execute_command_in_container om ozone fs -ls "${TLOG_DIR}" | awk '/\/tlog\./{print $NF}' | sort | tail -1)
recover_lease "${tlog}"
recovered_size=$(file_size "${tlog}")
execute_command_in_container om ozone fs -rm "${INDEX_DIR}/write.lock"
start_containers solr
wait_for_solr_core
assert_no_new_tlog_open_failure "${baseline}"
assert_num_found 5 "tlog replayed after kill -9"
# Solr ends the replay of an old tlog by appending a commit record to it
[[ "$(file_size "${tlog}")" -gt "${recovered_size}" ]] || fail "replayed tlog ${tlog} did not grow from ${recovered_size} bytes"

## 4. index again and restart once more: append on a tlog that was itself opened via append
index_docs 6 7
baseline=$(tlog_open_failures)
docker-compose --ansi never stop -t 60 solr
start_containers solr
wait_for_solr_core
assert_no_new_tlog_open_failure "${baseline}"
assert_num_found 7 "after second graceful restart"

echo "Solr on Ozone append test PASSED"
