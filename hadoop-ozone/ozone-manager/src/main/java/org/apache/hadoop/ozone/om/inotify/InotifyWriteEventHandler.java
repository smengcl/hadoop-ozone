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

package org.apache.hadoop.ozone.om.inotify;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.helpers.InotifyEvent;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.snapshot.FSODirectoryPathResolver;

/**
 * Decodes OM RocksDB write batches into inotify write events.
 */
public class InotifyWriteEventHandler extends ManagedWriteBatch.Handler {

  private final OMDBDefinition dbDefinition = OMDBDefinition.get();
  private static final Set<String> SUPPORTED_TABLES = initSupportedTables();
  private static final Set<String> FSO_TABLES = initFsoTables();
  private final Map<Integer, String> tableNames;
  private final List<InotifyEvent> events = new ArrayList<>();
  private final OMMetadataManager metadataManager;
  private final boolean resolveFsoPaths;
  private Map<Long, String> volumeIdNameMap;
  private Map<Long, OmBucketInfo> bucketIdInfoMap;
  private final Map<Long, Path> dirPathCache = new HashMap<>();
  private long sequenceNumber;

  public InotifyWriteEventHandler(OMMetadataManager metadataManager,
      boolean resolveFsoPaths) {
    this.tableNames = metadataManager.getStore().getTableNames();
    this.metadataManager = metadataManager;
    this.resolveFsoPaths = resolveFsoPaths;
  }

  public void setSequenceNumber(long sequenceNumber) {
    this.sequenceNumber = sequenceNumber;
  }

  public List<InotifyEvent> getEvents() {
    return events;
  }

  @Override
  public void put(int cfIndex, byte[] keyBytes, byte[] valueBytes) {
    processEvent(cfIndex, keyBytes, valueBytes, InotifyEvent.EventType.CREATE);
  }

  @Override
  public void delete(int cfIndex, byte[] keyBytes) {
    processEvent(cfIndex, keyBytes, null, InotifyEvent.EventType.DELETE);
  }

  @Override
  public void delete(byte[] keyBytes) {
    // Not used for OM write batches.
  }

  @Override
  public void put(byte[] keyBytes, byte[] valueBytes) {
    // Not used for OM write batches.
  }

  @Override
  public void merge(int cfIndex, byte[] keyBytes, byte[] valueBytes) {
    // Not used for OM write batches.
  }

  @Override
  public void merge(byte[] keyBytes, byte[] valueBytes) {
    // Not used for OM write batches.
  }

  @Override
  public void singleDelete(int cfIndex, byte[] keyBytes) {
    // Not used for OM write batches.
  }

  @Override
  public void singleDelete(byte[] keyBytes) {
    // Not used for OM write batches.
  }

  @Override
  public void deleteRange(int cfIndex, byte[] beginKey, byte[] endKey) {
    // Not used for OM write batches.
  }

  @Override
  public void deleteRange(byte[] beginKey, byte[] endKey) {
    // Not used for OM write batches.
  }

  @Override
  public void logData(byte[] blob) {
    // Not used for OM write batches.
  }

  @Override
  public void putBlobIndex(int cfIndex, byte[] key, byte[] value) {
    // Not used for OM write batches.
  }

  @Override
  public void markBeginPrepare() {
    // Not used for OM write batches.
  }

  @Override
  public void markEndPrepare(byte[] xid) {
    // Not used for OM write batches.
  }

  @Override
  public void markCommitWithTimestamp(byte[] commitTimestamp,
      byte[] commitTimestampValue) {
    // Not used for OM write batches.
  }

  @Override
  public void markCommit(byte[] xid) {
    // Not used for OM write batches.
  }

  @Override
  public void markRollback(byte[] xid) {
    // Not used for OM write batches.
  }

  @Override
  public void markNoop(boolean emptyBatch) {
    // Not used for OM write batches.
  }

  private void processEvent(int cfIndex, byte[] keyBytes, byte[] valueBytes,
      InotifyEvent.EventType eventType) {
    String tableName = tableNames.get(cfIndex);
    if (!SUPPORTED_TABLES.contains(tableName)) {
      return;
    }
    DBColumnFamilyDefinition<?, ?> cf = dbDefinition.getColumnFamily(tableName);
    if (cf == null) {
      return;
    }
    Object key;
    Object value;
    try {
      key = cf.getKeyCodec().fromPersistedFormat(keyBytes);
      value = valueBytes == null ? null
          : cf.getValueCodec().fromPersistedFormat(valueBytes);
      InotifyEvent event = toInotifyEvent(tableName, key, value, eventType);
      if (event != null) {
        events.add(event);
      }
    } catch (IOException ignored) {
      // Best-effort decoding; drop events we cannot resolve.
    }
  }

  private InotifyEvent toInotifyEvent(String tableName, Object key,
      Object value, InotifyEvent.EventType eventType) throws IOException {
    if (value instanceof OmKeyInfo) {
      OmKeyInfo keyInfo = (OmKeyInfo) value;
      if (FSO_TABLES.contains(tableName)) {
        if (key instanceof String) {
          String path = normalizeRawKey(tableName, (String) key, value);
          if (path == null) {
            return null;
          }
          return new InotifyEvent(eventType, path, null, !keyInfo.isFile(),
              keyInfo.getModificationTime(), sequenceNumber);
        }
        return null;
      }
      String path = normalizePath(
          keyInfo.getVolumeName(), keyInfo.getBucketName(),
          keyInfo.getKeyName());
      return new InotifyEvent(eventType, path, null, !keyInfo.isFile(),
          keyInfo.getModificationTime(), sequenceNumber);
    }
    if (value instanceof OmDirectoryInfo) {
      if (!resolveFsoPaths) {
        if (key instanceof String) {
          String path = trimLeadingSlash((String) key);
          return new InotifyEvent(eventType, path, null, true,
              ((OmDirectoryInfo) value).getModificationTime(), sequenceNumber);
        }
        return null;
      }
      OmDirectoryInfo dirInfo = (OmDirectoryInfo) value;
      FsoKeyParts parts = parseFsoKey(key);
      if (parts == null) {
        return null;
      }
      String path = buildFsoPath(parts, dirInfo.getName(),
          dirInfo.getParentObjectID());
      if (path == null) {
        return null;
      }
      return new InotifyEvent(eventType, path, null, true,
          dirInfo.getModificationTime(), sequenceNumber);
    }
    if (key instanceof String) {
      String path = normalizeRawKey(tableName, (String) key, value);
      if (path == null) {
        return null;
      }
      return new InotifyEvent(eventType, path, null, false,
          System.currentTimeMillis(), sequenceNumber);
    }
    return null;
  }

  private static String normalizePath(String volumeName, String bucketName,
      String keyName) {
    String normalizedKey = trimLeadingSlash(keyName);
    if (normalizedKey.isEmpty()) {
      return volumeName + "/" + bucketName;
    }
    return volumeName + "/" + bucketName + "/" + normalizedKey;
  }

  private static String normalizeRawKey(String rawKey) {
    if (rawKey == null) {
      return null;
    }
    return trimLeadingSlash(rawKey);
  }

  private String normalizeRawKey(String tableName, String rawKey, Object value)
      throws IOException {
    if (rawKey == null) {
      return null;
    }
    if (!FSO_TABLES.contains(tableName)) {
      return trimLeadingSlash(rawKey);
    }
    if (!resolveFsoPaths) {
      return trimLeadingSlash(rawKey);
    }
    FsoKeyParts parts = parseFsoKey(rawKey);
    if (parts == null) {
      return null;
    }
    String leafName = parts.name;
    if (value instanceof OmKeyInfo) {
      OmKeyInfo keyInfo = (OmKeyInfo) value;
      if (keyInfo.getFileName() != null && !keyInfo.getFileName().isEmpty()) {
        leafName = keyInfo.getFileName();
      }
    }
    return buildFsoPath(parts, leafName, parts.parentId);
  }

  private static String trimLeadingSlash(String value) {
    if (value == null) {
      return "";
    }
    if (value.startsWith("/")) {
      return value.substring(1);
    }
    return value;
  }

  private FsoKeyParts parseFsoKey(Object key) {
    if (!(key instanceof String)) {
      return null;
    }
    String rawKey = trimLeadingSlash((String) key);
    if (rawKey.isEmpty()) {
      return null;
    }
    String[] parts = rawKey.split(OzoneConsts.OM_KEY_PREFIX);
    if (parts.length < 4) {
      return null;
    }
    try {
      long volumeId = Long.parseLong(parts[0]);
      long bucketId = Long.parseLong(parts[1]);
      long parentId = Long.parseLong(parts[2]);
      String name = parts[3];
      return new FsoKeyParts(volumeId, bucketId, parentId, name);
    } catch (NumberFormatException ex) {
      return null;
    }
  }

  private String buildFsoPath(FsoKeyParts parts, String name, long parentId)
      throws IOException {
    ensureVolumeBucketMaps();
    String volumeName = trimLeadingSlash(volumeIdNameMap.get(parts.volumeId));
    OmBucketInfo bucketInfo = bucketIdInfoMap.get(parts.bucketId);
    if (volumeName == null || bucketInfo == null) {
      return null;
    }
    String bucketName = trimLeadingSlash(bucketInfo.getBucketName());
    Path parentPath = resolveDirPath(parts.volumeId, parts.bucketId, parentId);
    if (parentPath == null) {
      return null;
    }
    String bucketPrefix = volumeName + "/" + bucketName;
    boolean isRoot = parentPath.equals(OzoneConsts.ROOT_PATH);
    String parentSuffix = parentPath.toString();
    StringBuilder builder = new StringBuilder(bucketPrefix);
    if (!isRoot) {
      builder.append(parentSuffix);
    }
    if (builder.charAt(builder.length() - 1) != '/') {
      builder.append('/');
    }
    builder.append(name);
    return trimLeadingSlash(builder.toString());
  }

  private Path resolveDirPath(long volumeId, long bucketId, long objectId)
      throws IOException {
    if (objectId == bucketId) {
      return OzoneConsts.ROOT_PATH;
    }
    Path cached = dirPathCache.get(objectId);
    if (cached != null) {
      return cached;
    }
    String prefix = OzoneConsts.OM_KEY_PREFIX + volumeId
        + OzoneConsts.OM_KEY_PREFIX + bucketId + OzoneConsts.OM_KEY_PREFIX;
    FSODirectoryPathResolver resolver = new FSODirectoryPathResolver(
        prefix, bucketId, metadataManager.getDirectoryTable());
    Map<Long, Path> pathMap = resolver.getAbsolutePathForObjectIDs(
        Optional.of(Collections.singleton(objectId)), true);
    Path resolved = pathMap.get(objectId);
    if (resolved != null) {
      dirPathCache.put(objectId, resolved);
    }
    return resolved;
  }

  private void ensureVolumeBucketMaps() throws IOException {
    if (volumeIdNameMap != null && bucketIdInfoMap != null) {
      return;
    }
    volumeIdNameMap = new HashMap<>();
    bucketIdInfoMap = new HashMap<>();
    try (org.apache.hadoop.hdds.utils.db.TableIterator<String,
        ? extends org.apache.hadoop.hdds.utils.db.Table.KeyValue<String,
            org.apache.hadoop.ozone.om.helpers.OmVolumeArgs>> iter =
             metadataManager.getVolumeTable().iterator()) {
      while (iter.hasNext()) {
        org.apache.hadoop.hdds.utils.db.Table.KeyValue<String,
            org.apache.hadoop.ozone.om.helpers.OmVolumeArgs> entry = iter.next();
        volumeIdNameMap.put(entry.getValue().getObjectID(), entry.getKey());
      }
    }
    try (org.apache.hadoop.hdds.utils.db.TableIterator<String,
        ? extends org.apache.hadoop.hdds.utils.db.Table.KeyValue<String,
            OmBucketInfo>> iter = metadataManager.getBucketTable().iterator()) {
      while (iter.hasNext()) {
        org.apache.hadoop.hdds.utils.db.Table.KeyValue<String,
            OmBucketInfo> entry = iter.next();
        OmBucketInfo info = entry.getValue();
        bucketIdInfoMap.put(info.getObjectID(), info);
      }
    }
  }

  private static Set<String> initSupportedTables() {
    Set<String> tables = new HashSet<>();
    tables.add(OMDBDefinition.KEY_TABLE);
    tables.add(OMDBDefinition.FILE_TABLE);
    tables.add(OMDBDefinition.DIRECTORY_TABLE);
    return Collections.unmodifiableSet(tables);
  }

  private static Set<String> initFsoTables() {
    Set<String> tables = new HashSet<>();
    tables.add(OMDBDefinition.FILE_TABLE);
    tables.add(OMDBDefinition.DIRECTORY_TABLE);
    return Collections.unmodifiableSet(tables);
  }

  private static final class FsoKeyParts {
    private final long volumeId;
    private final long bucketId;
    private final long parentId;
    private final String name;

    private FsoKeyParts(long volumeId, long bucketId, long parentId,
        String name) {
      this.volumeId = volumeId;
      this.bucketId = bucketId;
      this.parentId = parentId;
      this.name = name;
    }
  }
}
