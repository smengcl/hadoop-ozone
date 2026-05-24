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

import java.nio.ByteBuffer;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;

/**
 * A simple per-coder-instance pool of direct {@link ByteBuffer} scratch
 * buffers used by the native encode/decode path.
 *
 * <p>Callers that already hold the coder-level lock (encoderLock /
 * decoderLock) may borrow and return slots without additional
 * synchronization. Each slot is identified by its index in the pool array.
 * If the existing buffer at a slot is too small for the requested capacity
 * it is replaced with a fresh allocation; the old buffer is simply
 * abandoned (GC'd via the Cleaner mechanism that backs direct buffers).
 *
 * <p>This class is NOT thread-safe on its own; it relies on the caller's
 * external lock.
 */
@InterfaceAudience.Private
final class DirectBufferPool {

  /** Lazily grown to accommodate the number of slots actually requested. */
  private ByteBuffer[] pool = new ByteBuffer[0];

  /**
   * Return a direct {@link ByteBuffer} for the given slot index that has
   * at least {@code capacity} bytes of space from position 0.
   *
   * <p>The returned buffer has position 0 and limit == capacity.
   *
   * @param slot     zero-based index into the pool
   * @param capacity minimum required capacity in bytes
   * @return a reusable direct {@link ByteBuffer} ready for writing
   */
  ByteBuffer get(int slot, int capacity) {
    ensureSize(slot + 1);
    ByteBuffer buf = pool[slot];
    if (buf == null || buf.capacity() < capacity) {
      buf = ByteBuffer.allocateDirect(capacity);
      pool[slot] = buf;
    }
    buf.clear();
    buf.limit(capacity);
    return buf;
  }

  /**
   * Grow the backing array to at least {@code minSize} entries if needed.
   */
  private void ensureSize(int minSize) {
    if (pool.length < minSize) {
      ByteBuffer[] larger = new ByteBuffer[minSize];
      System.arraycopy(pool, 0, larger, 0, pool.length);
      pool = larger;
    }
  }
}
