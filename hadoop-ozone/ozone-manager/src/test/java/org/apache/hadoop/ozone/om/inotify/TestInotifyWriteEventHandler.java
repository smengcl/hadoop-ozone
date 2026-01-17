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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.InotifyEvent;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestInotifyWriteEventHandler {

  @TempDir
  private File folder;

  private OMMetadataManager metadataManager;

  @BeforeEach
  void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, folder.getAbsolutePath());
    metadataManager = new OmMetadataManagerImpl(conf, null);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (metadataManager != null) {
      metadataManager.stop();
      metadataManager = null;
    }
  }

  @Test
  void testFiltersUnsupportedTables() throws Exception {
    InotifyWriteEventHandler handler =
        new InotifyWriteEventHandler(metadataManager, false);
    handler.setSequenceNumber(1L);

    int txnCf = getCfIndex(OMDBDefinition.TRANSACTION_INFO_TABLE);
    byte[] txnKey = OMDBDefinition.TRANSACTION_INFO_TABLE_DEF.getKeyCodec()
        .toPersistedFormat(OzoneConsts.TRANSACTION_INFO_KEY);
    byte[] txnValue = OMDBDefinition.TRANSACTION_INFO_TABLE_DEF.getValueCodec()
        .toPersistedFormat(TransactionInfo.valueOf(1, 1));
    handler.put(txnCf, txnKey, txnValue);

    int volumeCf = getCfIndex(OMDBDefinition.VOLUME_TABLE);
    OmVolumeArgs volume = OmVolumeArgs.newBuilder()
        .setVolume("hadoop")
        .setAdminName("admin")
        .setOwnerName("owner")
        .setObjectID(1L)
        .setCreationTime(1L)
        .setModificationTime(1L)
        .build();
    byte[] volumeKey = OMDBDefinition.VOLUME_TABLE_DEF.getKeyCodec()
        .toPersistedFormat(metadataManager.getVolumeKey("hadoop"));
    byte[] volumeValue = OMDBDefinition.VOLUME_TABLE_DEF.getValueCodec()
        .toPersistedFormat(volume);
    handler.put(volumeCf, volumeKey, volumeValue);

    int bucketCf = getCfIndex(OMDBDefinition.BUCKET_TABLE);
    OmBucketInfo bucket = OmBucketInfo.newBuilder()
        .setVolumeName("hadoop")
        .setBucketName("buck1")
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .setObjectID(2L)
        .setCreationTime(1L)
        .setModificationTime(1L)
        .build();
    byte[] bucketKey = OMDBDefinition.BUCKET_TABLE_DEF.getKeyCodec()
        .toPersistedFormat(metadataManager.getBucketKey("hadoop", "buck1"));
    byte[] bucketValue = OMDBDefinition.BUCKET_TABLE_DEF.getValueCodec()
        .toPersistedFormat(bucket);
    handler.put(bucketCf, bucketKey, bucketValue);

    assertTrue(handler.getEvents().isEmpty());
  }

  @Test
  void testRawFsoPathWhenResolveDisabled() throws Exception {
    long volumeId = 11L;
    long bucketId = 22L;
    long parentId = bucketId;
    long dirId = 33L;
    String dirName = "dir1";

    String rawKey = metadataManager.getOzonePathKey(
        volumeId, bucketId, parentId, dirName);
    OmDirectoryInfo dirInfo = OmDirectoryInfo.newBuilder()
        .setName(dirName)
        .setObjectID(dirId)
        .setParentObjectID(parentId)
        .setCreationTime(1L)
        .setModificationTime(1L)
        .build();

    InotifyWriteEventHandler handler =
        new InotifyWriteEventHandler(metadataManager, false);
    handler.setSequenceNumber(2L);
    int dirCf = getCfIndex(OMDBDefinition.DIRECTORY_TABLE);
    byte[] keyBytes = OMDBDefinition.DIRECTORY_TABLE_DEF.getKeyCodec()
        .toPersistedFormat(rawKey);
    byte[] valueBytes = OMDBDefinition.DIRECTORY_TABLE_DEF.getValueCodec()
        .toPersistedFormat(dirInfo);
    handler.put(dirCf, keyBytes, valueBytes);

    assertEquals(1, handler.getEvents().size());
    InotifyEvent event = handler.getEvents().get(0);
    assertEquals(InotifyEvent.EventType.CREATE, event.getEventType());
    assertEquals(rawKey.substring(1), event.getPath());
    assertFalse(event.getPath().startsWith("/"));
  }

  @Test
  void testTranslatedFsoPathWhenResolveEnabled() throws Exception {
    long volumeId = 101L;
    long bucketId = 202L;
    long parentId = bucketId;
    long dirId = 303L;
    String volumeName = "hadoop";
    String bucketName = "buck1";
    String dirName = "dir1";

    addVolume(volumeName, volumeId);
    addBucket(volumeName, bucketName, bucketId);

    String rawKey = metadataManager.getOzonePathKey(
        volumeId, bucketId, parentId, dirName);
    OmDirectoryInfo dirInfo = OmDirectoryInfo.newBuilder()
        .setName(dirName)
        .setObjectID(dirId)
        .setParentObjectID(parentId)
        .setCreationTime(1L)
        .setModificationTime(1L)
        .build();

    InotifyWriteEventHandler handler =
        new InotifyWriteEventHandler(metadataManager, true);
    handler.setSequenceNumber(3L);
    int dirCf = getCfIndex(OMDBDefinition.DIRECTORY_TABLE);
    byte[] keyBytes = OMDBDefinition.DIRECTORY_TABLE_DEF.getKeyCodec()
        .toPersistedFormat(rawKey);
    byte[] valueBytes = OMDBDefinition.DIRECTORY_TABLE_DEF.getValueCodec()
        .toPersistedFormat(dirInfo);
    handler.put(dirCf, keyBytes, valueBytes);

    assertEquals(1, handler.getEvents().size());
    InotifyEvent event = handler.getEvents().get(0);
    assertEquals("hadoop/buck1/dir1", event.getPath());
    assertFalse(event.getPath().startsWith("/"));
    assertFalse(event.getPath().contains("//"));
  }

  @Test
  void testRawFsoFilePathWhenResolveDisabled() throws Exception {
    long volumeId = 11L;
    long bucketId = 22L;
    long parentId = 33L;
    long fileId = 44L;
    String rawKey = metadataManager.getOzonePathKey(
        volumeId, bucketId, parentId, "key3");

    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setVolumeName("hadoop")
        .setBucketName("buck1")
        .setKeyName("key3")
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.ONE))
        .setFile(true)
        .setObjectID(fileId)
        .setParentObjectID(parentId)
        .setCreationTime(1L)
        .setModificationTime(1L)
        .build();

    InotifyWriteEventHandler handler =
        new InotifyWriteEventHandler(metadataManager, false);
    handler.setSequenceNumber(4L);
    int fileCf = getCfIndex(OMDBDefinition.FILE_TABLE);
    byte[] keyBytes = OMDBDefinition.FILE_TABLE_DEF.getKeyCodec()
        .toPersistedFormat(rawKey);
    byte[] valueBytes = OMDBDefinition.FILE_TABLE_DEF.getValueCodec()
        .toPersistedFormat(keyInfo);
    handler.put(fileCf, keyBytes, valueBytes);

    assertEquals(1, handler.getEvents().size());
    InotifyEvent event = handler.getEvents().get(0);
    assertEquals(rawKey.substring(1), event.getPath());
    assertFalse(event.getPath().startsWith("/"));
  }

  private int getCfIndex(String tableName) {
    for (Map.Entry<Integer, String> entry :
        metadataManager.getStore().getTableNames().entrySet()) {
      if (tableName.equals(entry.getValue())) {
        return entry.getKey();
      }
    }
    throw new IllegalArgumentException("Missing column family: " + tableName);
  }

  private void addVolume(String volumeName, long objectId) throws IOException {
    OmVolumeArgs volume = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName("admin")
        .setOwnerName("owner")
        .setObjectID(objectId)
        .setCreationTime(1L)
        .setModificationTime(1L)
        .build();
    metadataManager.getVolumeTable().put(
        metadataManager.getVolumeKey(volumeName), volume);
  }

  private void addBucket(String volumeName, String bucketName, long objectId)
      throws IOException {
    OmBucketInfo bucket = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .setObjectID(objectId)
        .setCreationTime(1L)
        .setModificationTime(1L)
        .build();
    metadataManager.getBucketTable().put(
        metadataManager.getBucketKey(volumeName, bucketName), bucket);
  }
}
