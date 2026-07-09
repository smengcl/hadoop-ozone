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

#suite:ipv6

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

# This scenario needs an IPv6-enabled Docker daemon. Default GitHub Actions
# runners do not provide one, so probe first and skip cleanly (exit 0) when
# IPv6 networking is unavailable rather than failing the suite.
probe_net="ozone-ipv6-probe-$$"
if ! docker network create --driver bridge --ipv6 \
    --subnet fd00:dead:beef:probe::/64 "${probe_net}" >/dev/null 2>&1; then
  echo "Docker daemon does not support IPv6 networking; skipping IPv6 suite."
  exit 0
fi
docker network rm "${probe_net}" >/dev/null 2>&1 || true

export SECURITY_ENABLED=false
export OZONE_REPLICATION_FACTOR=3
export COMPOSE_FILE=docker-compose.yaml

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

start_docker_env

execute_robot_test scm basic

execute_robot_test scm cli
execute_robot_test scm admincli

execute_robot_test scm freon

execute_robot_test scm -v SCHEME:o3fs -v BUCKET_TYPE:bucket -N ozonefs-o3fs-bucket ozonefs/ozonefs.robot
execute_robot_test scm -v SCHEME:ofs -N ozonefs-obs ozonefs/ozonefs-obs.robot

execute_robot_test s3g s3
