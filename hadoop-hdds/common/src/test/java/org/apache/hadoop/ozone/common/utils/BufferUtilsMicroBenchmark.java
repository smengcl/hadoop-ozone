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

package org.apache.hadoop.ozone.common.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/**
 * Micro benchmark for BufferUtils read-only buffer conversions.
 *
 * Run with:
 *   mvn -pl hadoop-hdds/common -DskipTests=false -Dtest=BufferUtilsMicroBenchmark test
 *   java -cp ... BufferUtilsMicroBenchmark
 */
public final class BufferUtilsMicroBenchmark {

  private static final int BUFFERS = 1024;
  private static final int BYTES_PER_BUFFER = 128;
  private static final int LOOPS = 1_000;

  private BufferUtilsMicroBenchmark() {
  }

  public static void main(String[] args) {
    List<ByteString> input = buildByteStrings();
    warmup(input);

    long baselineNs = timeNs(() -> baselineArrayConversion(input));
    long optimizedNs = timeNs(() -> BufferUtils.getReadOnlyByteBuffersArray(input));

    System.out.println("baseline ns: " + baselineNs);
    System.out.println("optimized ns: " + optimizedNs);
  }

  private static void warmup(List<ByteString> input) {
    for (int i = 0; i < 200; i++) {
      baselineArrayConversion(input);
      BufferUtils.getReadOnlyByteBuffersArray(input);
    }
  }

  private static long timeNs(Runnable op) {
    long start = System.nanoTime();
    for (int i = 0; i < LOOPS; i++) {
      op.run();
    }
    return System.nanoTime() - start;
  }

  private static ByteBuffer[] baselineArrayConversion(List<ByteString> input) {
    return BufferUtils.getReadOnlyByteBuffers(input).toArray(new ByteBuffer[0]);
  }

  private static List<ByteString> buildByteStrings() {
    Random random = new Random(1);
    List<ByteString> byteStrings = new ArrayList<>(BUFFERS);
    for (int i = 0; i < BUFFERS; i++) {
      byte[] data = new byte[BYTES_PER_BUFFER];
      random.nextBytes(data);
      byteStrings.add(ByteString.copyFrom(data));
    }
    return byteStrings;
  }
}
