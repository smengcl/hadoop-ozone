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
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionKey;
import org.apache.ratis.util.function.CheckedRunnable;

/**
 * Append session state of a stream entry pool, shared by {@link BlockOutputStreamEntryPool} and
 * {@link BlockDataStreamOutputEntryPool}. The entries of an append stream are the suffix only: OM keeps the prefix
 * blocks and gets prefixLength + suffix bytes as dataSize. For every other stream prefixLength is 0, hsync is not
 * handled here and the lease is never lost.
 */
final class AppendSessionState {
  private final boolean append;
  private final long prefixLength;
  private final String keyName;
  private final AppendSessionKey sessionKey;
  // Suffix bytes last published to OM by an append hsync.
  private long publishedSuffixLength;
  private volatile boolean leaseLost;
  private volatile Runnable cleanupHook = () -> { };

  AppendSessionState(OmKeyInfo info, long openID) {
    this.append = info.getAppendSession() != null;
    this.prefixLength = append ? info.getAppendSession().getPrefixLength() : 0;
    this.keyName = info.getKeyName();
    this.sessionKey = append ? AppendSessionKey.newBuilder().setVolumeName(info.getVolumeName())
        .setBucketName(info.getBucketName()).setSessionId(openID).build() : null;
  }

  boolean isAppend() {
    return append;
  }

  /** @return the file length when this append session was admitted, 0 if this is not an append stream. */
  long getPrefixLength() {
    return prefixLength;
  }

  String getKeyName() {
    return keyName;
  }

  AppendSessionKey getSessionKey() {
    return sessionKey;
  }

  /**
   * Publishes the location list of the key args with an OM hsync. Readers of an appended file only see what OM
   * published, so every advancing hsync goes to OM, also within the same block. OM checks dataSize against the
   * submitted block lengths, so both come from the same list.
   */
  void hsync(OmKeyArgs.Builder keyArgs, CheckedRunnable<IOException> omHsync) throws IOException {
    final long suffixLength = keyArgs.getLocationInfoList().stream().mapToLong(OmKeyLocationInfo::getLength).sum();
    if (suffixLength != publishedSuffixLength) {
      keyArgs.setDataSize(prefixLength + suffixLength);
      omHsync.run();
      publishedSuffixLength = suffixLength;
    }
  }

  /** The hook runs when the stream is closed or has failed. */
  void setCleanupHook(Runnable hook) {
    this.cleanupHook = hook;
  }

  void cleanup() {
    cleanupHook.run();
  }

  /** OM no longer has this stream's append session as active: the next write, hsync or close fails. */
  void markLeaseLost() {
    leaseLost = true;
  }

  void checkLease() throws IOException {
    if (leaseLost) {
      throw new IOException("The append lease was lost for key " + keyName + " (session " + sessionKey.getSessionId()
          + "): its lease expired, or the file was recovered or deleted while this stream was open.");
    }
  }
}
