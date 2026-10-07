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

package org.apache.hadoop.ozone.om.helpers;

import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionPhase;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionProto;

/**
 * Lease state of an append session, stored in the session's open key record. The open record's location list holds
 * only the blocks allocated by this session (the suffix). The first {@code prefixBlockCount} blocks of the committed
 * file, {@code prefixLength} bytes in total, are the immutable prefix and are never copied into the open record.
 */
@Immutable
public final class OmAppendSession {
  private final AppendSessionPhase phase;
  private final long prefixLength;
  private final int prefixBlockCount;
  private final long openedAt;
  private final long lastRenewedAt;

  public OmAppendSession(AppendSessionPhase phase, long prefixLength, int prefixBlockCount, long openedAt,
      long lastRenewedAt) {
    this.phase = Objects.requireNonNull(phase, "phase == null");
    this.prefixLength = prefixLength;
    this.prefixBlockCount = prefixBlockCount;
    this.openedAt = openedAt;
    this.lastRenewedAt = lastRenewedAt;
  }

  /** New ACTIVE session admitted at {@code now} on a file with the given committed prefix. */
  public static OmAppendSession newActive(long prefixLength, int prefixBlockCount, long now) {
    return new OmAppendSession(AppendSessionPhase.APPEND_ACTIVE, prefixLength, prefixBlockCount, now, now);
  }

  public AppendSessionPhase getPhase() {
    return phase;
  }

  public boolean isActive() {
    return phase == AppendSessionPhase.APPEND_ACTIVE;
  }

  public long getPrefixLength() {
    return prefixLength;
  }

  public int getPrefixBlockCount() {
    return prefixBlockCount;
  }

  public long getOpenedAt() {
    return openedAt;
  }

  public long getLastRenewedAt() {
    return lastRenewedAt;
  }

  public OmAppendSession withPhase(AppendSessionPhase newPhase) {
    return new OmAppendSession(newPhase, prefixLength, prefixBlockCount, openedAt, lastRenewedAt);
  }

  /** Renewal time only moves forward, so a delayed or replayed renewal cannot shorten the lease. */
  public OmAppendSession withRenewal(long renewedAt) {
    return new OmAppendSession(phase, prefixLength, prefixBlockCount, openedAt, Math.max(lastRenewedAt, renewedAt));
  }

  public AppendSessionProto toProtobuf() {
    return AppendSessionProto.newBuilder()
        .setPhase(phase)
        .setPrefixLength(prefixLength)
        .setPrefixBlockCount(prefixBlockCount)
        .setOpenedAt(openedAt)
        .setLastRenewedAt(lastRenewedAt)
        .build();
  }

  public static OmAppendSession fromProtobuf(AppendSessionProto proto) {
    // An unset or unknown phase would read as the first enum value, APPEND_ACTIVE, and make a fenced session writable.
    if (!proto.hasPhase()) {
      throw new IllegalArgumentException("Append session without a known phase");
    }
    return new OmAppendSession(proto.getPhase(), proto.getPrefixLength(), proto.getPrefixBlockCount(),
        proto.getOpenedAt(), proto.getLastRenewedAt());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OmAppendSession)) {
      return false;
    }
    OmAppendSession that = (OmAppendSession) o;
    return phase == that.phase && prefixLength == that.prefixLength && prefixBlockCount == that.prefixBlockCount
        && openedAt == that.openedAt && lastRenewedAt == that.lastRenewedAt;
  }

  @Override
  public int hashCode() {
    return Objects.hash(phase, prefixLength, prefixBlockCount, openedAt, lastRenewedAt);
  }

  @Override
  public String toString() {
    return "AppendSession{phase=" + phase + ", prefixLength=" + prefixLength + ", prefixBlockCount="
        + prefixBlockCount + ", openedAt=" + openedAt + ", lastRenewedAt=" + lastRenewedAt + "}";
  }
}
