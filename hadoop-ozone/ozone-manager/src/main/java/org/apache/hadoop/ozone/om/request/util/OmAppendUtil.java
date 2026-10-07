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

package org.apache.hadoop.ozone.om.request.util;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionPhase;

/**
 * Helper methods shared by the OM requests that handle append sessions.
 */
public final class OmAppendUtil {

  private OmAppendUtil() {
  }

  /** Returns true if the committed file is reserved by the given append session. */
  public static boolean isOwnedBy(OmKeyInfo committed, long sessionId) {
    return committed != null && committed.getAppendOwnerSessionId() != null
        && committed.getAppendOwnerSessionId() == sessionId;
  }

  /**
   * Returns the blocks of an append session's open record that the committed file does not reference. Only these may
   * be handed to block deletion when the session ends: the other blocks of the open record were published by hsync
   * and belong to the committed file.
   *
   * @param committed the current committed file, or null if it no longer exists in the live namespace. With null,
   *     every block of the open record is returned, so pass null only when the committed blocks are released by the
   *     caller through another path (as file deletion does).
   */
  public static List<OmKeyLocationInfo> privateAllocations(OmKeyInfo openRecord, OmKeyInfo committed) {
    OmKeyLocationInfoGroup suffix = openRecord.getLatestVersionLocations();
    if (suffix == null) {
      return Collections.emptyList();
    }
    OmKeyLocationInfoGroup published = committed == null ? null : committed.getLatestVersionLocations();
    Set<ContainerBlockID> publishedIds = published == null ? Collections.emptySet()
        : published.createLocationList().stream()
            .map(l -> l.getBlockID().getContainerBlockID()).collect(Collectors.toSet());
    return suffix.createLocationList().stream()
        .filter(l -> !publishedIds.contains(l.getBlockID().getContainerBlockID()))
        .collect(Collectors.toList());
  }

  /**
   * Returns the open record of a deleted file's append session, marked INVALIDATED and reduced to the session's
   * private allocations. The result contains no block that the deleted committed file referenced, so the existing
   * open key cleanup can move the whole record to the deleted table without touching snapshot referenced data. The
   * caller must also remove the session from the append session index.
   */
  public static OmKeyInfo invalidate(OmKeyInfo openRecord, OmKeyInfo committed, long trxnLogIndex) {
    long version = openRecord.getLatestVersionLocations() == null ? 0
        : openRecord.getLatestVersionLocations().getVersion();
    return openRecord.toBuilder()
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(version, privateAllocations(openRecord, committed))))
        .setAppendSession(openRecord.getAppendSession().withPhase(AppendSessionPhase.APPEND_INVALIDATED))
        .setUpdateID(trxnLogIndex)
        .build();
  }
}
