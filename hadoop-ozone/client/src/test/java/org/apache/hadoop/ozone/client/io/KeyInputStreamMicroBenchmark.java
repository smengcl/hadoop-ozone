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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;

/**
 * Micro benchmark for BlockLocationInfo lookup.
 */
public final class KeyInputStreamMicroBenchmark {

  private static final int LOCATIONS = 10_000;
  private static final int LOOPS = 5_000;

  private KeyInputStreamMicroBenchmark() {
  }

  public static void main(String[] args) throws IOException {
    OmKeyInfo keyInfo = buildKeyInfo();
    BlockID target = new BlockID(1, LOCATIONS / 2);

    warmup(keyInfo, target);

    long baselineNs = timeNs(() -> collectFirst(keyInfo, target));
    long optimizedNs = timeNs(() -> KeyInputStream.getBlockLocationInfo(keyInfo, target));

    System.out.println("baseline ns: " + baselineNs);
    System.out.println("optimized ns: " + optimizedNs);
  }

  private static void warmup(OmKeyInfo keyInfo, BlockID target) {
    for (int i = 0; i < 200; i++) {
      collectFirst(keyInfo, target);
      KeyInputStream.getBlockLocationInfo(keyInfo, target);
    }
  }

  private static long timeNs(Runnable op) {
    long start = System.nanoTime();
    for (int i = 0; i < LOOPS; i++) {
      op.run();
    }
    return System.nanoTime() - start;
  }

  private static OmKeyInfo buildKeyInfo() throws IOException {
    Pipeline pipeline = MockPipeline.createRatisPipeline();
    List<OmKeyLocationInfo> locations = new ArrayList<>(LOCATIONS);
    for (int i = 0; i < LOCATIONS; i++) {
      locations.add(new OmKeyLocationInfo.Builder()
          .setBlockID(new BlockID(1, i))
          .setLength(128)
          .setOffset(i * 128L)
          .setPipeline(pipeline)
          .setPartNumber(0)
          .build());
    }
    return new OmKeyInfo.Builder()
        .setBucketName("bucket")
        .setVolumeName("volume")
        .setKeyName("key")
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .addOmKeyLocationInfoGroup(new OmKeyLocationInfoGroup(0, locations))
        .build();
  }

  private static OmKeyLocationInfo collectFirst(OmKeyInfo keyInfo, BlockID blockID) {
    List<OmKeyLocationInfo> collect =
        keyInfo.getLatestVersionLocations()
            .getLocationList()
            .stream()
            .filter(l -> l.getBlockID().equals(blockID))
            .collect(Collectors.toList());
    return collect.isEmpty() ? null : collect.get(0);
  }
}
