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

package org.apache.hadoop.ozone.om.request.key;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmAppendUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests RenameKeyWithFSO request.
 */
public class TestOMKeyRenameRequestWithFSO extends TestOMKeyRenameRequest {
  private OmKeyInfo fromKeyParentInfo;
  private OmKeyInfo toKeyParentInfo;

  @Override
  @BeforeEach
  public void createParentKey() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    long volumeId = omMetadataManager.getVolumeId(volumeName);
    long bucketId = omMetadataManager.getBucketId(volumeName,
        bucketName);
    String fromKeyParentName = UUID.randomUUID().toString();
    String toKeyParentName = UUID.randomUUID().toString();
    fromKeyName = new Path(fromKeyParentName, "fromKey").toString();
    toKeyName = new Path(toKeyParentName, "toKey").toString();
    fromKeyParentInfo = getOmKeyInfo(fromKeyParentName)
        .setParentObjectID(bucketId)
        .build();
    toKeyParentInfo = getOmKeyInfo(toKeyParentName)
        .setParentObjectID(bucketId)
        .build();
    fromKeyInfo = getOmKeyInfo(fromKeyName)
        .setParentObjectID(fromKeyParentInfo.getObjectID())
        .build();
    OMRequestTestUtils.addDirKeyToDirTable(false,
        OMFileRequest.getDirectoryInfo(fromKeyParentInfo), volumeName,
        bucketName, txnLogId, omMetadataManager);
    OMRequestTestUtils.addDirKeyToDirTable(false,
        OMFileRequest.getDirectoryInfo(toKeyParentInfo), volumeName,
        bucketName, txnLogId, omMetadataManager);
    dbToKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
        toKeyParentInfo.getObjectID(), "toKey");
  }

  @Test
  public void testRenameOpenFile() throws Exception {
    fromKeyInfo = fromKeyInfo.withMetadataMutations(metadata ->
        metadata.put(OzoneConsts.HSYNC_CLIENT_ID, String.valueOf(1234)));
    addKeyToTable(fromKeyInfo);
    OMRequest modifiedOmRequest =
        doPreExecute(createRenameKeyRequest(
            volumeName, bucketName, fromKeyName, toKeyName));
    OMKeyRenameRequest omKeyRenameRequest =
        getOMKeyRenameRequest(modifiedOmRequest);
    OMClientResponse response =
        omKeyRenameRequest.validateAndUpdateCache(ozoneManager, 100L);
    assertEquals(OzoneManagerProtocolProtos.Status.RENAME_OPEN_FILE,
        response.getOMResponse().getStatus());
  }

  @Test
  public void testRenameFileWithAppendSession() throws Exception {
    long sessionId = 4321L;
    long volumeId = omMetadataManager.getVolumeId(volumeName);
    long bucketId = omMetadataManager.getBucketId(volumeName, bucketName);
    Table<String, OmKeyInfo> openFileTable = omMetadataManager.getOpenKeyTable(getBucketLayout());
    String dbFromOpenKey = OMRequestTestUtils.addFileWithAppendSession(fromKeyInfo, sessionId, omMetadataManager);
    OmKeyInfo fromOpenKeyInfo = openFileTable.get(dbFromOpenKey);
    // An ordinary writer's open record of the same file is not part of the append session and must stay.
    String dbOrdinaryOpenKey = OMRequestTestUtils.addFileToKeyTable(true, false, fromKeyInfo.getFileName(),
        fromKeyInfo, sessionId + 1, txnLogId, omMetadataManager);

    OMClientResponse response = renameAndApplyToDB(fromKeyName, toKeyName);

    String dbToOpenKey = omMetadataManager.getOpenFileName(volumeId, bucketId, toKeyParentInfo.getObjectID(), "toKey",
        sessionId);
    assertEquals(OzoneManagerProtocolProtos.Status.OK, response.getOMResponse().getStatus());
    assertEquals(dbToOpenKey, omMetadataManager.getAppendSessionOpenKey(volumeName, bucketName, sessionId));
    // The old open row is gone from cache and DB, the new one is in both.
    assertNull(openFileTable.get(dbFromOpenKey));
    assertNull(openFileTable.getSkipCache(dbFromOpenKey));
    assertEquals(openFileTable.get(dbToOpenKey), openFileTable.getSkipCache(dbToOpenKey));
    OmKeyInfo toOpenKeyInfo = openFileTable.getSkipCache(dbToOpenKey);
    assertEquals(toKeyName, toOpenKeyInfo.getKeyName());
    assertEquals("toKey", toOpenKeyInfo.getFileName());
    assertEquals(toKeyParentInfo.getObjectID(), toOpenKeyInfo.getParentObjectID());
    // Phase, prefix boundaries and lease renewal time are untouched, and so are the suffix blocks.
    assertEquals(fromOpenKeyInfo.getAppendSession(), toOpenKeyInfo.getAppendSession());
    assertThat(OMRequestTestUtils.getBlockLocalIds(toOpenKeyInfo)).containsExactly(2L, 3L);
    assertNull(toOpenKeyInfo.getAppendOwnerSessionId());
    assertEquals(fromOpenKeyInfo.getObjectID(), toOpenKeyInfo.getObjectID());

    OmKeyInfo toKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).getSkipCache(dbToKey);
    assertEquals(sessionId, toKeyInfo.getAppendOwnerSessionId());
    assertThat(OMRequestTestUtils.getBlockLocalIds(toKeyInfo)).containsExactly(1L, 2L);
    assertTrue(OmAppendUtil.isReachable(omMetadataManager, volumeId, bucketId, toOpenKeyInfo));

    assertThat(openFileTable.getSkipCache(dbOrdinaryOpenKey)).isNotNull();
  }

  @Test
  public void testRenameFileWithoutAppendSessionKeepsOpenFileTable() throws Exception {
    addKeyToTable(fromKeyInfo);
    String dbOpenKey = OMRequestTestUtils.addFileToKeyTable(true, false, fromKeyInfo.getFileName(), fromKeyInfo,
        clientID, txnLogId, omMetadataManager);

    OMClientResponse response = renameAndApplyToDB(fromKeyName, toKeyName);

    assertEquals(OzoneManagerProtocolProtos.Status.OK, response.getOMResponse().getStatus());
    Table<String, OmKeyInfo> openFileTable = omMetadataManager.getOpenKeyTable(getBucketLayout());
    assertThat(openFileTable.getSkipCache(dbOpenKey)).isNotNull();
    assertEquals(1, omMetadataManager.countRowsInTable(openFileTable));
    assertNull(omMetadataManager.getKeyTable(getBucketLayout()).getSkipCache(dbToKey).getAppendOwnerSessionId());
  }

  @Test
  public void testRenameParentDirOfFileWithAppendSession() throws Exception {
    long sessionId = 4321L;
    long volumeId = omMetadataManager.getVolumeId(volumeName);
    long bucketId = omMetadataManager.getBucketId(volumeName, bucketName);
    Table<String, OmKeyInfo> openFileTable = omMetadataManager.getOpenKeyTable(getBucketLayout());
    String dbOpenKey = OMRequestTestUtils.addFileWithAppendSession(fromKeyInfo, sessionId, omMetadataManager);
    OmKeyInfo openKeyInfo = openFileTable.get(dbOpenKey);
    assertTrue(OmAppendUtil.isReachable(omMetadataManager, volumeId, bucketId, openKeyInfo));

    OMClientResponse response = renameAndApplyToDB(fromKeyParentInfo.getKeyName(), "renamedDir");

    assertEquals(OzoneManagerProtocolProtos.Status.OK, response.getOMResponse().getStatus());
    assertNull(omMetadataManager.getDirectoryTable().get(getDBKeyName(fromKeyParentInfo)));
    // FSO rows are keyed by the parent object ID, so the session's rows and index entry did not move.
    assertEquals(dbOpenKey, omMetadataManager.getAppendSessionOpenKey(volumeName, bucketName, sessionId));
    assertEquals(openKeyInfo, openFileTable.get(dbOpenKey));
    assertEquals(sessionId, omMetadataManager.getKeyTable(getBucketLayout()).get(omMetadataManager.getOzonePathKey(
        volumeId, bucketId, openKeyInfo.getParentObjectID(), openKeyInfo.getFileName())).getAppendOwnerSessionId());
    // The open record still has the old path, the file is reachable through its parent object ID nevertheless.
    assertEquals(fromKeyName, openKeyInfo.getKeyName());
    assertTrue(OmAppendUtil.isReachable(omMetadataManager, volumeId, bucketId, openKeyInfo));
    omMetadataManager.getDirectoryTable().cleanupCache(Collections.singletonList(100L));
    assertTrue(OmAppendUtil.isReachable(omMetadataManager, volumeId, bucketId, openKeyInfo));
  }

  private OMClientResponse renameAndApplyToDB(String fromKey, String toKey) throws Exception {
    OMRequest modifiedOmRequest = doPreExecute(createRenameKeyRequest(volumeName, bucketName, fromKey, toKey));
    OMClientResponse response = getOMKeyRenameRequest(modifiedOmRequest).validateAndUpdateCache(ozoneManager, 100L);
    try (BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation()) {
      response.checkAndUpdateDB(omMetadataManager, batchOperation);
      omMetadataManager.getStore().commitBatchOperation(batchOperation);
    }
    return response;
  }

  @Override
  @Test
  public void testValidateAndUpdateCacheWithToKeyInvalid() throws Exception {
    String invalidToKeyName = "invalid:";
    assertThrows(
        OMException.class, () -> doPreExecute(createRenameKeyRequest(
            volumeName, bucketName, fromKeyName, invalidToKeyName)));  }

  @Test
  public void testValidateAndUpdateCacheWithEmptyToKey() throws Exception {
    String emptyToKeyName = "";
    OMRequest omRequest = createRenameKeyRequest(volumeName,
        bucketName, fromKeyName, emptyToKeyName);
    assertEquals(omRequest.getRenameKeyRequest().getToKeyName(), "");
  }

  @Override
  @Test
  public void testValidateAndUpdateCacheWithFromKeyInvalid() throws Exception {
    String invalidFromKeyName = "";
    assertThrows(
        OMException.class, () -> doPreExecute(createRenameKeyRequest(
            volumeName, bucketName, invalidFromKeyName, toKeyName)));
  }

  @Test
  public void testPreExecuteWithUnNormalizedPath() throws Exception {
    addKeyToTable(fromKeyInfo);
    String toKeyName =
        "///root" + OzoneConsts.OZONE_URI_DELIMITER +
            OzoneConsts.OZONE_URI_DELIMITER +
            UUID.randomUUID();
    String fromKeyName =
        "///" + fromKeyInfo.getKeyName();
    OMRequest modifiedOmRequest =
        doPreExecute(createRenameKeyRequest(toKeyName, fromKeyName));
    String normalizedSrcName =
        modifiedOmRequest.getRenameKeyRequest().getToKeyName();
    String normalizedDstName =
        modifiedOmRequest.getRenameKeyRequest().getKeyArgs().getKeyName();
    String expectedSrcKeyName = OmUtils.normalizeKey(toKeyName, false);
    String expectedDstKeyName = OmUtils.normalizeKey(fromKeyName, false);
    assertEquals(expectedSrcKeyName, normalizedSrcName);
    assertEquals(expectedDstKeyName, normalizedDstName);
  }

  /**
   * Create OMRequest which encapsulates RenameKeyRequest.
   *
   * @return OMRequest
   */
  private OMRequest createRenameKeyRequest(String toKeyName,
      String fromKeyName) {
    KeyArgs keyArgs = KeyArgs.newBuilder().setKeyName(fromKeyName)
        .setVolumeName(volumeName).setBucketName(bucketName).build();

    RenameKeyRequest renameKeyRequest = RenameKeyRequest.newBuilder()
        .setKeyArgs(keyArgs).setToKeyName(toKeyName).build();

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setRenameKeyRequest(renameKeyRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.RenameKey).build();
  }

  private OMRequest doPreExecute(OMRequest originalOmRequest) throws Exception {
    OMKeyRenameRequestWithFSO omKeyRenameRequestWithFSO =
        new OMKeyRenameRequestWithFSO(originalOmRequest, getBucketLayout());

    OMRequest modifiedOmRequest
        = omKeyRenameRequestWithFSO.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set and modification time is
    // set in KeyArgs.
    assertNotEquals(originalOmRequest, modifiedOmRequest);

    assertThat(modifiedOmRequest.getRenameKeyRequest()
        .getKeyArgs().getModificationTime()).isGreaterThan(0);

    return modifiedOmRequest;
  }

  @Override
  protected OmKeyInfo.Builder getOmKeyInfo(String keyName) {
    long bucketId = random.nextLong();
    return OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName, RatisReplicationConfig.getInstance(ONE))
        .setObjectID(bucketId + 100L)
        .setParentObjectID(bucketId + 101L);
  }

  @Override
  protected String addKeyToTable(OmKeyInfo keyInfo) throws Exception {
    OMRequestTestUtils.addFileToKeyTable(false, false,
        keyInfo.getFileName(), keyInfo, clientID, txnLogId, omMetadataManager);
    return getDBKeyName(keyInfo);
  }

  @Override
  protected OMKeyRenameRequest getOMKeyRenameRequest(OMRequest omRequest) {
    return new OMKeyRenameRequestWithFSO(omRequest, getBucketLayout());
  }

  @Override
  protected String getDBKeyName(OmKeyInfo keyInfo) throws IOException {
    return omMetadataManager.getOzonePathKey(
        omMetadataManager.getVolumeId(volumeName),
        omMetadataManager.getBucketId(volumeName, bucketName),
        keyInfo.getParentObjectID(), keyInfo.getKeyName());
  }

  @Override
  protected void assertModificationTime(long except)
      throws IOException {
    // For filesystem should change the modification time for
    // both the parents directory
    OmDirectoryInfo updatedFromKeyParentInfo = omMetadataManager
        .getDirectoryTable().get(getDBKeyName(fromKeyParentInfo));
    OmDirectoryInfo updatedToKeyParentInfo = omMetadataManager
        .getDirectoryTable().get(getDBKeyName(toKeyParentInfo));
    assertEquals(except, updatedFromKeyParentInfo.getModificationTime());
    assertEquals(except, updatedToKeyParentInfo.getModificationTime());
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
