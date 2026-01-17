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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.helpers.InotifyEvent;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;

/**
 * Decodes OM RocksDB write batches into inotify write events.
 */
public class InotifyWriteEventHandler extends ManagedWriteBatch.Handler {

  private final OMDBDefinition dbDefinition = OMDBDefinition.get();
  private final Map<Integer, String> tableNames;
  private final List<InotifyEvent> events = new ArrayList<>();
  private long sequenceNumber;

  public InotifyWriteEventHandler(OMMetadataManager metadataManager) {
    this.tableNames = metadataManager.getStore().getTableNames();
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
      String path = normalizePath(
          keyInfo.getVolumeName(), keyInfo.getBucketName(),
          keyInfo.getKeyName());
      return new InotifyEvent(eventType, path, null, !keyInfo.isFile(),
          keyInfo.getModificationTime(), sequenceNumber);
    }
    if (key instanceof String) {
      String path = normalizeRawKey((String) key);
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

  private static String trimLeadingSlash(String value) {
    if (value == null) {
      return "";
    }
    if (value.startsWith("/")) {
      return value.substring(1);
    }
    return value;
  }
}
