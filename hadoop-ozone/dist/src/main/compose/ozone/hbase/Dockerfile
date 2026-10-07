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

# HBase has no official multi-arch Docker image, so build one from the Apache binary tarball.
# The hadoop3 build of HBase 2.6 ships hadoop-common 3.4.x, which has org.apache.hadoop.fs.LeaseRecoverable
# and SafeMode, the interfaces HBase 2.6 uses to recover WAL leases on a non HDFS file system.
FROM eclipse-temurin:17-jre
ARG HBASE_VERSION=2.6.7
# The archive keeps every release; the mirrors drop a release when the next one comes out.
ARG HBASE_URL=https://archive.apache.org/dist/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-hadoop3-bin.tar.gz
# from hbase-$HBASE_VERSION-hadoop3-bin.tar.gz.sha512 of the release
ARG HBASE_SHA512=2e0835a3ed8e5a9114f5d878deafb2f7a6b1c8386ba422d1a307fcc5551d452dc7da468729a43acf75dcfe0b20424cb48f72ed424dfd15f81b1b3624454ed82d
ADD $HBASE_URL /tmp/hbase.tar.gz
RUN echo "$HBASE_SHA512  /tmp/hbase.tar.gz" | sha512sum -c - \
  && mkdir -p /opt/hbase \
  && tar xzf /tmp/hbase.tar.gz -C /opt/hbase --strip-components=1 \
  && rm -rf /tmp/hbase.tar.gz /opt/hbase/docs
ENV HBASE_HOME=/opt/hbase PATH=/opt/hbase/bin:$PATH HBASE_MANAGES_ZK=false
WORKDIR /opt/hbase
