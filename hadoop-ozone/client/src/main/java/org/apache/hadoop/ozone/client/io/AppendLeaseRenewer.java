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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.utils.Scheduler;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Renews the OM leases of the append streams opened through one client, so that OM does not hand their files to
 * lease recovery while the writers are alive. A stream that OM reports as no longer active is fenced.
 * <p>
 * Renewal only reads the registry and sets a flag on the stream, it never takes a lock that a blocked write holds.
 */
public class AppendLeaseRenewer implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AppendLeaseRenewer.class);
  static final int BATCH_SIZE = 256;

  private final OzoneManagerProtocol omClient;
  private final Duration interval;
  private final Map<AppendSessionKey, AppendSessionState> sessions = new ConcurrentHashMap<>();
  private Scheduler scheduler;
  private boolean closed;

  public AppendLeaseRenewer(OzoneManagerProtocol omClient, Duration interval) {
    this.omClient = omClient;
    this.interval = interval;
  }

  /** Renews the session of the given append stream until the stream is closed or has failed. */
  public void register(KeyOutputStream stream) {
    register(stream.getBlockOutputStreamEntryPool().getAppendState());
  }

  /** Renews the session of the given RATIS streaming append stream until the stream is closed or has failed. */
  public void register(KeyDataStreamOutput stream) {
    register(stream.getAppendState());
  }

  private void register(AppendSessionState state) {
    final AppendSessionKey session = state.getSessionKey();
    sessions.put(session, state);
    state.setCleanupHook(() -> sessions.remove(session));
    start();
  }

  private synchronized void start() {
    if (scheduler == null && !closed) {
      scheduler = new Scheduler("AppendLeaseRenewer", true, 1);
      scheduler.scheduleWithFixedDelay(this::renew, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }
  }

  @VisibleForTesting
  void renew() {
    for (List<AppendSessionKey> batch : Lists.partition(new ArrayList<>(sessions.keySet()), BATCH_SIZE)) {
      try {
        final List<Boolean> renewed = omClient.renewAppendLeases(batch);
        for (int i = 0; i < batch.size() && i < renewed.size(); i++) {
          if (!renewed.get(i)) {
            // null if the stream was closed while the request was in flight
            final AppendSessionState state = sessions.remove(batch.get(i));
            if (state != null) {
              LOG.warn("Append lease of key {} (session {}) was lost", state.getKeyName(), batch.get(i).getSessionId());
              state.markLeaseLost();
            }
          }
        }
      } catch (Exception e) {
        // Only an answer from OM fences a stream. The next run retries.
        LOG.warn("Failed to renew {} append leases", batch.size(), e);
      }
    }
  }

  @Override
  public synchronized void close() {
    closed = true;
    if (scheduler != null) {
      scheduler.close();
      scheduler = null;
    }
  }
}
