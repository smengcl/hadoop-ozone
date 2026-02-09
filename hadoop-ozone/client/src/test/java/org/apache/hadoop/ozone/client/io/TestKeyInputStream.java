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

package org.apache.hadoop.ozone.client.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.junit.jupiter.api.Test;

class TestKeyInputStream {

  @Test
  void testGetBlockLocationInfo() throws IOException {
    Pipeline pipeline = MockPipeline.createRatisPipeline();
    BlockID blockId1 = new BlockID(1, 1);
    BlockID blockId2 = new BlockID(1, 2);

    List<OmKeyLocationInfo> locations = new ArrayList<>();
    locations.add(new OmKeyLocationInfo.Builder()
        .setBlockID(blockId1)
        .setLength(128)
        .setOffset(0)
        .setPipeline(pipeline)
        .setPartNumber(0)
        .build());
    locations.add(new OmKeyLocationInfo.Builder()
        .setBlockID(blockId2)
        .setLength(256)
        .setOffset(128)
        .setPipeline(pipeline)
        .setPartNumber(0)
        .build());

    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setBucketName("bucket")
        .setVolumeName("volume")
        .setKeyName("key")
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .addOmKeyLocationInfoGroup(new OmKeyLocationInfoGroup(0, locations))
        .build();

    BlockLocationInfo found1 = KeyInputStream.getBlockLocationInfo(keyInfo, blockId1);
    BlockLocationInfo found2 = KeyInputStream.getBlockLocationInfo(keyInfo, blockId2);
    BlockLocationInfo missing = KeyInputStream.getBlockLocationInfo(
        keyInfo, new BlockID(1, 3));

    assertEquals(blockId1, found1.getBlockID());
    assertEquals(blockId2, found2.getBlockID());
    assertNull(missing);
  }
}
