<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to you under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Ozone dual-stack (IPv6 + IPv4) compose environment

This environment runs SCM, OM, datanodes, S3 Gateway and Recon on a
dual-stack Docker network so RPC, filesystem (o3fs/ofs) and S3 paths are
exercised with IPv6 available. The server endpoints bind to the IPv6
wildcard `::`, and the JVM runs dual-stack (IPv4 is no longer forced, see
HDDS-15772).

## Prerequisites

An IPv6-enabled Docker daemon is required. `test.sh` probes for IPv6
networking support and skips (exit 0) when it is not available, so the
`ipv6` acceptance suite passes as a no-op on runners without IPv6.

To enable IPv6 in the Docker daemon, add to `/etc/docker/daemon.json`:

```json
{
  "ipv6": true,
  "fixed-cidr-v6": "fd00:dead:beef::/64"
}
```

## Usage

```
docker-compose up -d
```

Run the acceptance tests with:

```
./test.sh
```
