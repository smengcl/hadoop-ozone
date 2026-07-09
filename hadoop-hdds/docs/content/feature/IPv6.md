---
title: "IPv6 Support"
weight: 14
menu:
   main:
      parent: Features
summary: Deploying Ozone on dual-stack and IPv6-only networks
---
<!---
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

Ozone can run on dual-stack (IPv4 and IPv6) and IPv6-only networks. This page
covers the settings an operator needs to review for those deployments.

## JVM network stack

Ozone daemons and tests run dual-stack by default; `java.net.preferIPv4Stack`
is no longer forced. To pin a host back to IPv4-only, set `OZONE_OPTS` in
`ozone-env.sh`:

```bash
export OZONE_OPTS="-Djava.net.preferIPv4Stack=true"
```

## Bind addresses

The default bind address for every service is the IPv4 wildcard `0.0.0.0`,
which binds IPv4 only even on a dual-stack JVM. To accept IPv6 (and, on a
dual-stack host, IPv4-mapped) connections, set the relevant `*.bind.host` /
`*-bind-host` properties to the IPv6 wildcard `::`. For example:

```xml
<property>
  <name>ozone.scm.client.bind.host</name>
  <value>::</value>
</property>
<property>
  <name>ozone.scm.block.client.bind.host</name>
  <value>::</value>
</property>
<property>
  <name>ozone.scm.datanode.bind.host</name>
  <value>::</value>
</property>
<property>
  <name>hdds.datanode.client.bind.host</name>
  <value>::</value>
</property>
<property>
  <name>ozone.s3g.http-bind-host</name>
  <value>::</value>
</property>
<property>
  <name>ozone.recon.http-bind-host</name>
  <value>::</value>
</property>
```

The OM RPC server binds to the host in `ozone.om.address`; set it to `::` (or
to an OM service id under HA) for IPv6 binding.

## Address formats

IPv6 literals must be bracketed when combined with a port, matching the usual
`[address]:port` convention.

- OzoneFS: `ofs://[2001:db8::1]:9862/vol/bucket/key`. For `o3fs`, a bracketed
  IPv6 literal is not a legal URI authority mid-string
  (`o3fs://bucket.volume.host:port`), so use an OM service id to reach an
  IPv6 OM over `o3fs`.
- S3 Gateway: the HTTP `Host` header uses `[address]:port`, for example
  `[2001:db8::1]:9878`.
- `ozone.scm.names`, `ozone.om.address` and other host:port properties accept
  bracketed IPv6 literals.

## Secure mode

In secure mode the internal CA issues certificates for service hosts. When a
service is reached over an IPv6 literal, that address must be present in the
certificate's Subject Alternative Names, otherwise TLS host verification
fails. Prefer DNS names (with both A and AAAA records) over raw IPv6 literals
so certificates and host matching stay stable.

## Try it out

A dual-stack compose environment is provided under
`compose/ozone-ipv6`. It requires an IPv6-enabled Docker daemon; see that
directory's `README.md` for details.
