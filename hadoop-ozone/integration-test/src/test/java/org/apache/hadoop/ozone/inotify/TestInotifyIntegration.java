/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.inotify;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.InotifyEvent;
import org.apache.hadoop.ozone.om.helpers.InotifyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InotifyOpType;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InotifyRequest;
import org.apache.ozone.test.ClusterForTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestInotifyIntegration extends ClusterForTests<MiniOzoneCluster> {

  private OzoneClient client;
  private OzoneBucket bucket;
  private String volumeName;
  private String bucketName;

  @Override
  protected MiniOzoneCluster createCluster() throws Exception {
    return newClusterBuilder().build();
  }

  @BeforeAll
  void setup() throws Exception {
    client = getCluster().newClient();
    bucket = TestDataUtil.createVolumeAndBucket(client);
    volumeName = bucket.getVolumeName();
    bucketName = bucket.getName();
  }

  @AfterAll
  void teardown() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  @Test
  void testInotifyReadAndWriteEvents() throws Exception {
    String keyName = "inotify-key";
    try (OzoneOutputStream out = bucket.createKey(keyName, 3)) {
      out.write(new byte[] {1, 2, 3});
    }
    try (OzoneInputStream in = bucket.readKey(keyName)) {
      in.read();
    }

    String prefix = volumeName + "/" + bucketName;
    InotifyRequest request = InotifyRequest.newBuilder()
        .setWriteSequenceNumber(0L)
        .setAccessSequenceNumber(0L)
        .setLimitCount(1000L)
        .setPathPrefix(prefix)
        .setRecursive(true)
        .setOpType(InotifyOpType.READ_AND_WRITE)
        .build();

    InotifyResponse response =
        client.getProxy().getInotifyEvents(request);

    assertThat(response.isDbUpdateSuccess()).isTrue();
    assertThat(response.getEvents()).isNotEmpty();
    assertThat(response.getAccessEvents()).isNotEmpty();
    assertThat(response.getEvents())
        .anyMatch(event -> event.getPath().contains(keyName));
    assertThat(response.getAccessEvents())
        .anyMatch(event -> event.getEventType() == InotifyEvent.EventType.ACCESS);
  }
}
