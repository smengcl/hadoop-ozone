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

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionPhase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper methods shared by the OM requests that handle append sessions.
 */
public final class OmAppendUtil {

  private static final Logger LOG = LoggerFactory.getLogger(OmAppendUtil.class);

  private OmAppendUtil() {
  }

  /** Returns true if the committed file is reserved by the given append session. */
  public static boolean isOwnedBy(OmKeyInfo committed, long sessionId) {
    return committed != null && committed.getAppendOwnerSessionId() != null
        && committed.getAppendOwnerSessionId() == sessionId;
  }

  /**
   * Fails with APPEND_WRITER_CONFLICT if the committed file is reserved by an append session. Ordinary writers must
   * not create, overwrite or commit over such a file.
   */
  public static void checkNotReserved(OmKeyInfo committed, String keyName) throws OMException {
    if (committed != null && committed.getAppendOwnerSessionId() != null) {
      throw new OMException("File " + keyName + " is reserved by append session "
          + committed.getAppendOwnerSessionId(), OMException.ResultCodes.APPEND_WRITER_CONFLICT);
    }
  }

  /**
   * Returns the file table DB key of the file that an append session's open record belongs to. It is derived from
   * the open record, which follows renames, and never from the path of a request.
   */
  public static String getDbFileKey(OMMetadataManager omMetadataManager, OmKeyInfo openRecord) throws IOException {
    String volume = openRecord.getVolumeName();
    String bucket = openRecord.getBucketName();
    return omMetadataManager.getOzonePathKey(omMetadataManager.getVolumeId(volume),
        omMetadataManager.getBucketId(volume, bucket), openRecord.getParentObjectID(), openRecord.getFileName());
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

  /**
   * Invalidates the append session that owns an FSO file which is being deleted: replaces the session's open record
   * in the open file table cache by its {@link #invalidate invalidated} form and removes the session from the append
   * session index. Must be called under the bucket write lock, before the deleted record loses its owner. The caller
   * must persist the returned record under the returned open file table DB key.
   *
   * @param committed the deleted file with its file name and parent object ID
   * @return the open file table DB key and the invalidated open record, or null if nothing has to be persisted (no
   *     append owner, or no live open record of that session)
   */
  public static Pair<String, OmKeyInfo> invalidateSessionOfDeletedFile(OMMetadataManager omMetadataManager,
      OmKeyInfo committed, long trxnLogIndex) throws IOException {
    Long sessionId = committed.getAppendOwnerSessionId();
    if (sessionId == null) {
      return null;
    }
    String volume = committed.getVolumeName();
    String bucket = committed.getBucketName();
    String dbOpenKey = omMetadataManager.getAppendSessionOpenKey(volume, bucket, sessionId);
    if (dbOpenKey == null) {
      dbOpenKey = omMetadataManager.getOpenFileName(omMetadataManager.getVolumeId(volume),
          omMetadataManager.getBucketId(volume, bucket), committed.getParentObjectID(), committed.getFileName(),
          sessionId);
    }
    Table<String, OmKeyInfo> openFileTable = omMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    OmKeyInfo openRecord = openFileTable.get(dbOpenKey);
    omMetadataManager.removeAppendSession(volume, bucket, sessionId);
    if (openRecord == null || openRecord.getAppendSession() == null) {
      LOG.warn("Potentially inconsistent DB state: append open record not found with dbOpenKey '{}'", dbOpenKey);
      return null;
    }
    if (openRecord.getAppendSession().getPhase() == AppendSessionPhase.APPEND_INVALIDATED) {
      return null;
    }
    OmKeyInfo invalidated = invalidate(openRecord, committed, trxnLogIndex);
    openFileTable.addCacheEntry(dbOpenKey, invalidated, trxnLogIndex);
    return Pair.of(dbOpenKey, invalidated);
  }

  /**
   * Fails with KEY_NOT_FOUND unless the file of an append session is still {@link #isReachable reachable}. Must be
   * called under the bucket lock by allocate, hsync, close and recovery completion.
   */
  public static void checkReachable(OMMetadataManager omMetadataManager, OmKeyInfo openRecord) throws IOException {
    String volume = openRecord.getVolumeName();
    String bucket = openRecord.getBucketName();
    if (!isReachable(omMetadataManager, omMetadataManager.getVolumeId(volume),
        omMetadataManager.getBucketId(volume, bucket), openRecord)) {
      throw new OMException("File of append session was deleted with its directory: " + openRecord.getKeyName(),
          OMException.ResultCodes.KEY_NOT_FOUND);
    }
  }

  /**
   * Returns true if every ancestor directory of an FSO file still exists in the live namespace, that is the file was
   * not removed by a recursive directory delete whose cleanup has not reached it yet. A renamed ancestor keeps its
   * object ID and stays reachable; a deleted ancestor does not come back when its path is created again. Allocate,
   * hsync, close and recovery of an append session must fail when this returns false, even though the session's rows
   * still exist. Must be called under the bucket lock.
   *
   * @param openOrCommittedRecord a record of the file with its parent object ID. Its key name is only a hint: the
   *     full path of an open record makes the check cheap as long as no ancestor was renamed.
   */
  public static boolean isReachable(OMMetadataManager omMetadataManager, long volumeId, long bucketId,
      OmKeyInfo openOrCommittedRecord) throws IOException {
    long parentId = openOrCommittedRecord.getParentObjectID();
    try {
      // The path the record remembers still leads to the same parent directory: all ancestors are alive.
      if (OMFileRequest.getParentID(volumeId, bucketId, openOrCommittedRecord.getKeyName(), omMetadataManager)
          == parentId) {
        return true;
      }
    } catch (OMException e) {
      LOG.debug("Path {} does not resolve, an ancestor was renamed or deleted: {}",
          openOrCommittedRecord.getKeyName(), e.getMessage());
    }
    // ponytail: directory rows are keyed by (parent ID, name), so there is no lookup by object ID. Once the remembered
    // path is stale (ancestor renamed or deleted) every check scans all directory rows of the bucket. Refresh the open
    // record's key name on directory rename or keep an object ID to directory key index if this shows up in hsync
    // latency.
    Table<String, OmDirectoryInfo> dirTable = omMetadataManager.getDirectoryTable();
    String bucketPrefix = OM_KEY_PREFIX + volumeId + OM_KEY_PREFIX + bucketId + OM_KEY_PREFIX;
    // Copy the cache first, tombstones included: the double buffer evicts flushed entries without the bucket lock, so
    // an entry checked during the DB scan could be gone before a later cache pass and its directory would be missed.
    Map<String, OmDirectoryInfo> cached = new HashMap<>();
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmDirectoryInfo>>> cacheIterator = dirTable.cacheIterator();
    while (cacheIterator.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmDirectoryInfo>> entry = cacheIterator.next();
      if (entry.getKey().getCacheKey().startsWith(bucketPrefix)) {
        cached.put(entry.getKey().getCacheKey(), entry.getValue().getCacheValue());
      }
    }
    Map<Long, Long> parentOfDir = new HashMap<>();
    for (OmDirectoryInfo dir : cached.values()) {
      if (dir != null) {
        parentOfDir.put(dir.getObjectID(), dir.getParentObjectID());
      }
    }
    try (Table.KeyValueIterator<String, OmDirectoryInfo> iterator = dirTable.iterator(bucketPrefix)) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmDirectoryInfo> row = iterator.next();
        if (!cached.containsKey(row.getKey())) {
          parentOfDir.put(row.getValue().getObjectID(), row.getValue().getParentObjectID());
        }
      }
    }
    while (parentId != bucketId) {
      // remove() instead of get() so that corrupt metadata with a cycle ends the walk.
      Long grandParentId = parentOfDir.remove(parentId);
      if (grandParentId == null) {
        return false;
      }
      parentId = grandParentId;
    }
    return true;
  }
}
