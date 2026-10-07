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

package org.apache.hadoop.fs.ozone;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.apache.hadoop.ozone.om.exceptions.AppendConflictException;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendConflictInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionPhase;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendWriterKind;
import org.apache.ratis.util.function.CheckedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Append admission shared by the Ozone file systems. When the file is held by an abandoned append writer it waits
 * until OM reports that writer recoverable, recovers the lease and retries the admission once.
 */
final class AppendWithRecovery {
  private static final Logger LOG = LoggerFactory.getLogger(AppendWithRecovery.class);

  private AppendWithRecovery() {
  }

  /**
   * @param maxWaitMs how long to wait for an append writer to become recoverable before failing with the conflict
   */
  static OzoneFSOutputStream append(OzoneClientAdapter adapter, String key, long maxWaitMs) throws IOException {
    return append(adapter, key, maxWaitMs, System::nanoTime, Thread::sleep);
  }

  @VisibleForTesting
  static OzoneFSOutputStream append(OzoneClientAdapter adapter, String key, long maxWaitMs, LongSupplier nanoClock,
      CheckedConsumer<Long, InterruptedException> sleeper) throws IOException {
    final long start = nanoClock.getAsLong();
    AppendConflictInfo previous = null;
    while (true) {
      final AppendConflictException conflict;
      try {
        return adapter.appendFile(key);
      } catch (AppendConflictException e) {
        conflict = e;
      }
      final AppendConflictInfo info = conflict.getConflict();
      // Only a renewable append session is recovered automatically, and never with force recovery.
      // Ordinary and hsync writers need an explicit recoverLease.
      if (info == null || info.getWriterKind() != AppendWriterKind.APPEND_WRITER
          || !info.hasRemainingMsUntilRecoverable()) {
        throw conflict;
      }
      if (previous != null && (info.getSessionId() != previous.getSessionId()
          || info.getLastRenewedAt() != previous.getLastRenewedAt())) {
        // The writer renewed its lease while we were waiting (or another writer took over): it is alive.
        throw conflict;
      }
      final long remainingMs = info.getRemainingMsUntilRecoverable();
      if (remainingMs == 0 || info.getPhase() == AppendSessionPhase.APPEND_RECOVERING) {
        LOG.info("Recovering the lease of append session {} of {} before appending", info.getSessionId(), key);
        // ponytail: the deadline bounds the wait only, not the datanode and OM calls of the recovery itself.
        try {
          LeaseRecoveryClientDNHandler.recoverLease(adapter, key, false);
        } catch (OMException e) {
          // Another client recovered the session first, or its writer renewed the lease after the admission attempt.
          // The retry below tells which: it succeeds, or fails with the conflict of the current writer.
          if (e.getResult() != ResultCodes.KEY_ALREADY_CLOSED && e.getResult() != ResultCodes.APPEND_SESSION_NOT_FOUND
              && e.getResult() != ResultCodes.KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD) {
            throw e;
          }
          LOG.debug("Recovery of append session {} of {} is not needed", info.getSessionId(), key, e);
        }
        return adapter.appendFile(key);
      }
      if (TimeUnit.NANOSECONDS.toMillis(nanoClock.getAsLong() - start) + remainingMs > maxWaitMs) {
        throw conflict;
      }
      // ponytail: admission (an OM write) is the only status poll, so sleep the whole server reported duration.
      // A live writer is detected after at most one soft limit. Poll a read path once OM exposes lastRenewedAt there.
      try {
        sleeper.accept(remainingMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw (IOException) new InterruptedIOException("Interrupted while waiting to append to " + key).initCause(e);
      }
      previous = info;
    }
  }
}
