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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmAppendSession;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AbortOpenKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests {@link OMOpenKeyAbortRequest}.
 */
public class TestOMOpenKeyAbortRequest extends OMKeyRequestTests {

  private static final long OTHER_CLIENT_ID = 2L;

  private BucketLayout bucketLayout;
  private long parentId;

  @Override
  public BucketLayout getBucketLayout() {
    return bucketLayout;
  }

  @ParameterizedTest
  @EnumSource(BucketLayout.class)
  public void testAbortOpenKey(BucketLayout layout) throws Exception {
    init(layout);
    OmKeyInfo aborted = newOpenKeyInfo().build();
    String dbOpenKey = addToTable(true, aborted, clientID);
    String otherDbOpenKey = addToTable(true, newOpenKeyInfo().build(), OTHER_CLIENT_ID);

    OMOpenKeyAbortRequest request = preExecute(createAbortRequest(clientID));
    OMClientResponse response = request.validateAndUpdateCache(ozoneManager, ++txnLogId);
    applyToDB(response);

    assertEquals(Status.OK, response.getOMResponse().getStatus());
    assertThat(request.getAuditBuilder().getAuditMap())
        .containsEntry(OzoneConsts.VOLUME, volumeName)
        .containsEntry(OzoneConsts.BUCKET, bucketName)
        .containsEntry(OzoneConsts.KEY, keyName)
        .containsEntry(OzoneConsts.CLIENT_ID, String.valueOf(clientID));

    // Only the aborted writer's open key is removed, and its blocks are handed to the deletion path.
    assertThat(omMetadataManager.getOpenKeyTable(layout).get(dbOpenKey)).isNull();
    assertThat(omMetadataManager.getOpenKeyTable(layout).get(otherDbOpenKey)).isNotNull();
    RepeatedOmKeyInfo deleted = omMetadataManager.getDeletedTable().get(
        omMetadataManager.getOzoneDeletePathKey(aborted.getObjectID(), dbOpenKey));
    assertThat(deleted.getOmKeyInfoList()).hasSize(1);
    assertThat(deleted.getOmKeyInfoList().get(0).getLatestVersionLocations().createLocationList())
        .extracting(OmKeyLocationInfo::getBlockID)
        .containsExactly(aborted.getLatestVersionLocations().createLocationList().get(0).getBlockID());
    assertEquals(0, omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName)).getUsedBytes());

    // The aborted writer is fenced: its late requests, and a repeated abort, find no open key.
    assertEquals(Status.KEY_NOT_FOUND, execute(createAllocateBlockRequest(clientID)).getOMResponse().getStatus());
    assertEquals(Status.KEY_NOT_FOUND, execute(createCommitKeyRequest(clientID)).getOMResponse().getStatus());
    assertEquals(Status.KEY_NOT_FOUND, execute(createAbortRequest(clientID)).getOMResponse().getStatus());
    // The other writer on the same path is not affected.
    assertEquals(Status.OK, execute(createAllocateBlockRequest(OTHER_CLIENT_ID)).getOMResponse().getStatus());
    assertEquals(Status.OK, execute(createCommitKeyRequest(OTHER_CLIENT_ID)).getOMResponse().getStatus());
  }

  @ParameterizedTest
  @EnumSource(BucketLayout.class)
  public void testAbortOpenKeyNotFound(BucketLayout layout) throws Exception {
    init(layout);
    // The writer has already committed: only the committed key and another writer's open key are left.
    addToTable(false, newOpenKeyInfo().build(), clientID);
    String otherDbOpenKey = addToTable(true, newOpenKeyInfo().build(), OTHER_CLIENT_ID);

    OMOpenKeyAbortRequest request = preExecute(createAbortRequest(clientID));
    OMClientResponse response = request.validateAndUpdateCache(ozoneManager, ++txnLogId);
    applyToDB(response);

    assertEquals(Status.KEY_NOT_FOUND, response.getOMResponse().getStatus());
    assertThat(request.getAuditBuilder().getAuditMap()).containsEntry(OzoneConsts.CLIENT_ID, String.valueOf(clientID));
    assertThat(omMetadataManager.getOpenKeyTable(layout).get(otherDbOpenKey)).isNotNull();
    assertThat(omMetadataManager.getDeletedTable().isEmpty()).isTrue();
  }

  @ParameterizedTest
  @EnumSource(BucketLayout.class)
  public void testAbortRejectsNonOrdinaryOpenKeys(BucketLayout layout) throws Exception {
    init(layout);
    final long appendId = 11L;
    final long hsyncId = 12L;
    final long hsyncCommittedOnlyId = 13L;
    final long multipartId = 14L;
    String[] dbOpenKeys = {
        addToTable(true, newOpenKeyInfo().setAppendSession(OmAppendSession.newActive(100, 1, Time.now())).build(),
            appendId),
        addToTable(true, newOpenKeyInfo().addMetadata(OzoneConsts.HSYNC_CLIENT_ID, String.valueOf(hsyncId)).build(),
            hsyncId),
        addToTable(true, newOpenKeyInfo().build(), hsyncCommittedOnlyId),
        addToTable(true, OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName, replicationConfig,
            new OmKeyLocationInfoGroup(0, new ArrayList<>(), true)).setParentObjectID(parentId).build(), multipartId)
    };
    addToTable(false, newOpenKeyInfo()
        .addMetadata(OzoneConsts.HSYNC_CLIENT_ID, String.valueOf(hsyncCommittedOnlyId)).build(), hsyncCommittedOnlyId);

    for (long id : new long[] {appendId, hsyncId, hsyncCommittedOnlyId, multipartId}) {
      assertEquals(Status.NOT_SUPPORTED_OPERATION, execute(createAbortRequest(id)).getOMResponse().getStatus());
    }
    for (String dbOpenKey : dbOpenKeys) {
      assertThat(omMetadataManager.getOpenKeyTable(layout).get(dbOpenKey)).isNotNull();
    }
    assertThat(omMetadataManager.getDeletedTable().isEmpty()).isTrue();
  }

  @ParameterizedTest
  @EnumSource(value = BucketLayout.class, names = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
  public void testPreExecute(BucketLayout layout) throws Exception {
    init(layout);
    String keyToNormalize = keyName.replace("/", "//");
    OMRequest omRequest = createAbortRequest(clientID).toBuilder()
        .setAbortOpenKeyRequest(createAbortRequest(clientID).getAbortOpenKeyRequest().toBuilder()
            .setKeyName(keyToNormalize))
        .build();

    when(ozoneManager.isAdminAuthorizationEnabled()).thenReturn(true);
    when(ozoneManager.isAdmin(any(UserGroupInformation.class))).thenReturn(false);
    OMClientRequest denied = OzoneManagerRatisUtils.createClientRequest(omRequest, ozoneManager);
    OMException e = assertThrows(OMException.class, () -> denied.preExecute(ozoneManager));
    assertEquals(OMException.ResultCodes.ACCESS_DENIED, e.getResult());
    assertThat(denied.getAuditBuilder().getAuditMap())
        .containsEntry(OzoneConsts.CLIENT_ID, String.valueOf(clientID));

    when(ozoneManager.isAdmin(any(UserGroupInformation.class))).thenReturn(true);
    // Only FSO normalizes the key name, the same way the commit request does.
    assertEquals(layout.isFileSystemOptimized() ? keyName : keyToNormalize,
        preExecute(omRequest).getOmRequest().getAbortOpenKeyRequest().getKeyName());

    // A bucket link is resolved, because open keys are stored under the real volume and bucket.
    when(ozoneManager.resolveBucketLink(any(Pair.class), any(OMClientRequest.class)))
        .thenReturn(new ResolvedBucket("linkVolume", "linkBucket", volumeName, bucketName, "owner", layout));
    AbortOpenKeyRequest resolved = new OMOpenKeyAbortRequest(omRequest.toBuilder()
        .setAbortOpenKeyRequest(omRequest.getAbortOpenKeyRequest().toBuilder()
            .setVolumeName("linkVolume")
            .setBucketName("linkBucket"))
        .build(), layout).preExecute(ozoneManager).getAbortOpenKeyRequest();
    assertEquals(volumeName, resolved.getVolumeName());
    assertEquals(bucketName, resolved.getBucketName());
  }

  private void init(BucketLayout layout) throws Exception {
    bucketLayout = layout;
    keyName = "dir1/dir2/" + keyName;
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager, layout);
    if (layout.isFileSystemOptimized()) {
      parentId = OMRequestTestUtils.addParentsToDirTable(volumeName, bucketName,
          OzoneFSUtils.getParentDir(keyName), omMetadataManager);
    }
  }

  /** A key at the tested path that holds one block. */
  private OmKeyInfo.Builder newOpenKeyInfo() throws Exception {
    OmKeyInfo keyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName, replicationConfig)
        .setObjectID(random.nextLong())
        .setParentObjectID(parentId)
        .build();
    OMRequestTestUtils.addKeyLocationInfo(keyInfo, 0, 100);
    return keyInfo.toBuilder();
  }

  /** Adds the key to the open key table (for the given client) or to the key table, and returns its DB key. */
  private String addToTable(boolean openKeyTable, OmKeyInfo keyInfo, long id) throws Exception {
    if (bucketLayout.isFileSystemOptimized()) {
      return OMRequestTestUtils.addFileToKeyTable(openKeyTable, false, OzoneFSUtils.getFileName(keyName), keyInfo, id,
          txnLogId, omMetadataManager);
    }
    OMRequestTestUtils.addKeyToTable(openKeyTable, false, keyInfo, id, txnLogId, omMetadataManager);
    return openKeyTable ? omMetadataManager.getOpenKey(volumeName, bucketName, keyName, id)
        : omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
  }

  private OMOpenKeyAbortRequest preExecute(OMRequest omRequest) throws Exception {
    OMRequest preExecuted = OzoneManagerRatisUtils.createClientRequest(omRequest, ozoneManager)
        .preExecute(ozoneManager);
    return (OMOpenKeyAbortRequest) OzoneManagerRatisUtils.createClientRequest(preExecuted, ozoneManager);
  }

  /** Runs the request the way OM does and applies its response to the DB. */
  private OMClientResponse execute(OMRequest omRequest) throws Exception {
    OMRequest preExecuted = OzoneManagerRatisUtils.createClientRequest(omRequest, ozoneManager)
        .preExecute(ozoneManager);
    OMClientResponse response = OzoneManagerRatisUtils.createClientRequest(preExecuted, ozoneManager)
        .validateAndUpdateCache(ozoneManager, ++txnLogId);
    applyToDB(response);
    return response;
  }

  private void applyToDB(OMClientResponse response) throws Exception {
    try (BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation()) {
      response.checkAndUpdateDB(omMetadataManager, batchOperation);
      omMetadataManager.getStore().commitBatchOperation(batchOperation);
    }
  }

  private OMRequest createAbortRequest(long id) {
    return OMRequest.newBuilder()
        .setCmdType(Type.AbortOpenKey)
        .setClientId(UUID.randomUUID().toString())
        .setUserInfo(UserInfo.newBuilder().setUserName("user1"))
        .setAbortOpenKeyRequest(AbortOpenKeyRequest.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setClientID(id))
        .build();
  }

  private KeyArgs.Builder createKeyArgs() {
    return KeyArgs.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setType(replicationConfig.getReplicationType())
        .setFactor(((RatisReplicationConfig) replicationConfig).getReplicationFactor());
  }

  private OMRequest createAllocateBlockRequest(long id) {
    return OMRequest.newBuilder()
        .setCmdType(Type.AllocateBlock)
        .setClientId(UUID.randomUUID().toString())
        .setAllocateBlockRequest(AllocateBlockRequest.newBuilder().setClientID(id).setKeyArgs(createKeyArgs()))
        .build();
  }

  private OMRequest createCommitKeyRequest(long id) {
    KeyLocation keyLocation = KeyLocation.newBuilder()
        .setBlockID(HddsProtos.BlockID.newBuilder()
            .setContainerBlockID(HddsProtos.ContainerBlockID.newBuilder().setContainerID(100L).setLocalID(1000L)))
        .setOffset(0)
        .setLength(100)
        .build();
    return OMRequest.newBuilder()
        .setCmdType(Type.CommitKey)
        .setClientId(UUID.randomUUID().toString())
        .setCommitKeyRequest(CommitKeyRequest.newBuilder()
            .setClientID(id)
            .setKeyArgs(createKeyArgs().setDataSize(100).addKeyLocations(keyLocation)))
        .build();
  }
}
