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

package org.apache.ozone.erasurecode.rawcoder;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that the per-instance direct scratch buffer pool used by the native
 * EC encoder and decoder (byte-array input path) produces bit-identical output
 * across repeated calls on the same coder instance.
 *
 * <p>A pool that returns a dirty (stale data) buffer would cause a
 * subsequent encode to mix old data into its computation, yielding wrong
 * output. This test catches such bugs by comparing repeated runs against a
 * single reference computation.
 */
public class TestNativeRawCoderScratchBufferPool {

  private static final int NUM_DATA = 3;
  private static final int NUM_PARITY = 2;
  private static final int CHUNK_SIZE = 1024;
  private static final int REPETITIONS = 5;

  private final Random random = new Random(0xdeadbeef);

  @BeforeEach
  public void assumeNativeLoaded() {
    Assumptions.assumeTrue(ErasureCodeNative.isNativeCodeLoaded(),
        "Native EC library not loaded, skipping pool reuse test");
  }

  /**
   * Encode the same data REPETITIONS times using the same encoder instance
   * and verify each result matches the very first (reference) encoding.
   * Because the pool re-uses the same direct buffers the output must still
   * be correct: previous call's data must not bleed into the next call.
   */
  @Test
  public void testEncoderPoolReuse() throws IOException {
    ECReplicationConfig cfg =
        new ECReplicationConfig(NUM_DATA, NUM_PARITY);
    RawErasureCoderFactory factory = new NativeRSRawErasureCoderFactory();
    RawErasureEncoder encoder = factory.createEncoder(cfg);

    byte[][] inputs = generateInputs(NUM_DATA, CHUNK_SIZE);
    byte[][] referenceOutputs = allocateOutputs(NUM_PARITY, CHUNK_SIZE);

    // First call produces the reference.
    encoder.encode(inputs, referenceOutputs);

    // Subsequent calls must yield the same parity bytes.
    for (int rep = 0; rep < REPETITIONS; rep++) {
      byte[][] outputs = allocateOutputs(NUM_PARITY, CHUNK_SIZE);
      // Re-encode exactly the same inputs; since encode() does not modify
      // the byte-array inputs there is no need to copy them first.
      encoder.encode(inputs, outputs);
      for (int p = 0; p < NUM_PARITY; p++) {
        assertArrayEquals(referenceOutputs[p], outputs[p],
            "Parity[" + p + "] mismatch on repetition " + rep
                + "; pool may have returned stale data");
      }
    }

    encoder.release();
  }

  /**
   * Decode the same erasure scenario REPETITIONS times using the same
   * decoder instance and verify each result matches the reference decoding.
   */
  @Test
  public void testDecoderPoolReuse() throws IOException {
    ECReplicationConfig cfg =
        new ECReplicationConfig(NUM_DATA, NUM_PARITY);
    RawErasureCoderFactory factory = new NativeRSRawErasureCoderFactory();
    RawErasureEncoder encoder = factory.createEncoder(cfg);
    RawErasureDecoder decoder = factory.createDecoder(cfg);

    byte[][] inputs = generateInputs(NUM_DATA, CHUNK_SIZE);
    byte[][] parities = allocateOutputs(NUM_PARITY, CHUNK_SIZE);
    encoder.encode(inputs, parities);
    encoder.release();

    // Erase data unit 0: decoder must reconstruct it from the remaining units.
    int erasedIndex = 0;
    int[] erasedIndexes = new int[]{erasedIndex};

    // Build the decoder input array: numData + numParity total slots,
    // the erased slot is null.
    byte[][] decoderInputs = buildDecoderInputs(inputs, parities, erasedIndex);

    byte[][] reference = allocateOutputs(1, CHUNK_SIZE);
    decoder.decode(decoderInputs, erasedIndexes, reference);

    assertEquals(CHUNK_SIZE, reference[0].length);
    assertArrayEquals(inputs[erasedIndex], reference[0],
        "Reference decode did not reproduce original data unit");

    for (int rep = 0; rep < REPETITIONS; rep++) {
      byte[][] result = allocateOutputs(1, CHUNK_SIZE);
      decoder.decode(buildDecoderInputs(inputs, parities, erasedIndex),
          erasedIndexes, result);
      assertArrayEquals(reference[0], result[0],
          "Decoded data[" + erasedIndex + "] mismatch on repetition " + rep
              + "; pool may have returned stale data");
    }

    decoder.release();
  }

  /**
   * Verify that re-encoding different data through the same encoder produces
   * different (and correct) parity output.  This guards against a pool
   * implementation that ignores the new input and returns cached output.
   */
  @Test
  public void testEncoderPoolWithVaryingInputs() throws IOException {
    ECReplicationConfig cfg =
        new ECReplicationConfig(NUM_DATA, NUM_PARITY);
    RawErasureCoderFactory factory = new NativeRSRawErasureCoderFactory();
    RawErasureEncoder encoder = factory.createEncoder(cfg);

    for (int rep = 0; rep < REPETITIONS; rep++) {
      byte[][] inputs = generateInputs(NUM_DATA, CHUNK_SIZE);
      byte[][] outputs = allocateOutputs(NUM_PARITY, CHUNK_SIZE);
      encoder.encode(inputs, outputs);

      // Verify by decoding: erase input[0] and recover it.
      RawErasureDecoder decoder =
          new NativeRSRawErasureCoderFactory().createDecoder(cfg);
      byte[][] decoderIn = buildDecoderInputs(inputs, outputs, 0);
      byte[][] recovered = allocateOutputs(1, CHUNK_SIZE);
      decoder.decode(decoderIn, new int[]{0}, recovered);
      assertArrayEquals(inputs[0], recovered[0],
          "Round-trip encode+decode failed on rep " + rep);
      decoder.release();
    }

    encoder.release();
  }

  // -------------------------------------------------------------------------
  // helpers
  // -------------------------------------------------------------------------

  private byte[][] generateInputs(int count, int size) {
    byte[][] result = new byte[count][size];
    for (byte[] buf : result) {
      random.nextBytes(buf);
    }
    return result;
  }

  private byte[][] allocateOutputs(int count, int size) {
    byte[][] result = new byte[count][size];
    return result;
  }

  /**
   * Build the (numData + numParity)-element input array for the decoder,
   * placing null at the erased data index.
   */
  private byte[][] buildDecoderInputs(byte[][] dataInputs,
      byte[][] parityInputs, int erasedDataIndex) {
    int total = dataInputs.length + parityInputs.length;
    byte[][] decoderIn = new byte[total][];
    for (int i = 0; i < dataInputs.length; i++) {
      decoderIn[i] = (i == erasedDataIndex) ? null : dataInputs[i];
    }
    for (int i = 0; i < parityInputs.length; i++) {
      decoderIn[dataInputs.length + i] = parityInputs[i];
    }
    return decoderIn;
  }
}
