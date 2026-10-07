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

package org.apache.hadoop.ozone.om.request.file;

import static org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB.setReplicationConfig;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionPhase.APPEND_ACTIVE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionPhase.APPEND_RECOVERING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmAppendSession;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.key.OMAllocateBlockRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.OMKeyCommitRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequestTests;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionPhase;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RecoverLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RecoverLeaseResponse;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests OMRecoverLeaseRequest.
 */
public class TestOMRecoverLeaseRequest extends OMKeyRequestTests {

  private long parentId;
  private boolean forceRecovery = false;

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  /**
   * Verify that RecoverLease request closes properly for an open file where
   * hsync was called .
   * @throws Exception
   */
  @Test
  public void testRecoverHsyncFile() throws Exception {
    populateNamespace(true, true, true, true);

    OMClientResponse omClientResponse = validateAndUpdateCache();
    OMResponse omResponse = omClientResponse.getOMResponse();
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());
    RecoverLeaseResponse recoverLeaseResponse = omResponse.getRecoverLeaseResponse();
    KeyInfo keyInfo = recoverLeaseResponse.getKeyInfo();
    assertNotNull(keyInfo);
    OmKeyInfo omKeyInfo = OmKeyInfo.getFromProtobuf(keyInfo);

    omClientResponse = validateAndUpdateCacheForCommit(getNewKeyArgs(omKeyInfo, 0));
    omResponse = omClientResponse.getOMResponse();
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());

    verifyTables(true, false);
  }

  /**
   * Verify that RecoverLease request is idempotent.
   * @throws Exception
   */
  @Test
  public void testInitStageIdempotent() throws Exception {
    populateNamespace(true, true, true, true);

    // call recovery first time
    OMClientResponse omClientResponse = validateAndUpdateCache();
    OMResponse omResponse = omClientResponse.getOMResponse();
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());
    RecoverLeaseResponse recoverLeaseResponse = omResponse.getRecoverLeaseResponse();
    KeyInfo keyInfo1 = recoverLeaseResponse.getKeyInfo();
    assertNotNull(keyInfo1);

    // call recovery second time
    omClientResponse = validateAndUpdateCache();
    omResponse = omClientResponse.getOMResponse();
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());
    recoverLeaseResponse = omResponse.getRecoverLeaseResponse();
    KeyInfo keyInfo2 = recoverLeaseResponse.getKeyInfo();
    assertNotNull(keyInfo2);
    assertEquals(keyInfo1.getKeyName(), keyInfo2.getKeyName());
  }

  /**
   * Verify that COMMIT request for recovery is not idempotent.
   * @throws Exception
   */
  @Test
  public void testCommitStageNotIdempotent() throws Exception {
    populateNamespace(true, true, true, true);

    // call recovery
    OMClientResponse omClientResponse = validateAndUpdateCache();
    OMResponse omResponse = omClientResponse.getOMResponse();
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());
    RecoverLeaseResponse recoverLeaseResponse = omResponse.getRecoverLeaseResponse();
    KeyInfo keyInfo = recoverLeaseResponse.getKeyInfo();
    assertNotNull(keyInfo);
    OmKeyInfo omKeyInfo = OmKeyInfo.getFromProtobuf(keyInfo);

    KeyArgs newKeyArgs = getNewKeyArgs(omKeyInfo, 0);

    // call commit first time
    omClientResponse = validateAndUpdateCacheForCommit(newKeyArgs);
    omResponse = omClientResponse.getOMResponse();
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());

    // call commit second time
    omClientResponse = validateAndUpdateCacheForCommit(newKeyArgs);
    omResponse = omClientResponse.getOMResponse();
    assertEquals(OzoneManagerProtocolProtos.Status.KEY_ALREADY_CLOSED, omResponse.getStatus());
  }

  /**
   * Verify that RecoverLease COMMIT request has a new file length.
   * @throws Exception
   */
  @Test
  public void testRecoverWithNewFileLength() throws Exception {
    populateNamespace(true, true, true, true);

    // call recovery
    OMClientResponse omClientResponse = validateAndUpdateCache();
    OMResponse omResponse = omClientResponse.getOMResponse();
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());
    RecoverLeaseResponse recoverLeaseResponse = omResponse.getRecoverLeaseResponse();
    KeyInfo keyInfo = recoverLeaseResponse.getKeyInfo();
    assertNotNull(keyInfo);
    OmKeyInfo omKeyInfo = OmKeyInfo.getFromProtobuf(keyInfo);

    // call commit
    long deltaLength = 100;
    KeyArgs newKeyArgs = getNewKeyArgs(omKeyInfo, deltaLength);
    omClientResponse = validateAndUpdateCacheForCommit(newKeyArgs);
    omResponse = omClientResponse.getOMResponse();
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());

    // get file length and check the length is as expected
    String ozoneKey = getFileName();
    OmKeyInfo omKeyInfoFetched = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertEquals(omKeyInfo.getDataSize(), omKeyInfoFetched.getDataSize());

    // check the final block length is as expected
    List<OmKeyLocationInfo> locationInfoListFetched =
        omKeyInfoFetched.getLatestVersionLocations().getBlocksLatestVersionOnly();
    List<OmKeyLocationInfo> omKeyLocationInfos = omKeyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly();
    assertEquals(omKeyLocationInfos.get(omKeyLocationInfos.size() - 1).getLength(),
        locationInfoListFetched.get(locationInfoListFetched.size() - 1).getLength());

    // check the committed file doesn't have HSYNC_CLIENT_ID and LEASE_RECOVERY metadata
    assertNull(omKeyInfoFetched.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID));
    assertNull(omKeyInfoFetched.getMetadata().get(OzoneConsts.LEASE_RECOVERY));
  }

  /**
   * Verify that RecoverLease COMMIT request has a new client ID.
   * @throws Exception
   */
  @Test
  public void testRecoverWithNewClientID() throws Exception {
    populateNamespace(true, true, true, true);

    // call recovery
    OMClientResponse omClientResponse = validateAndUpdateCache();
    OMResponse omResponse = omClientResponse.getOMResponse();
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());
    RecoverLeaseResponse recoverLeaseResponse = omResponse.getRecoverLeaseResponse();
    KeyInfo keyInfo = recoverLeaseResponse.getKeyInfo();
    assertNotNull(keyInfo);
    OmKeyInfo omKeyInfo = OmKeyInfo.getFromProtobuf(keyInfo);

    // call commit
    KeyArgs newKeyArgs = getNewKeyArgs(omKeyInfo, 0);
    omClientResponse = validateAndUpdateCacheForCommit(newKeyArgs, true, true);
    omResponse = omClientResponse.getOMResponse();
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());
  }

  /**
   * Verify that an under recovery file will reject allocate block and further hsync call(commit).
   * @throws Exception
   */
  @Test
  public void testRejectAllocateBlockAndHsync() throws Exception {
    populateNamespace(true, true, true, true);

    // call recovery
    OMClientResponse omClientResponse = validateAndUpdateCache();
    OMResponse omResponse = omClientResponse.getOMResponse();
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());
    RecoverLeaseResponse recoverLeaseResponse = omResponse.getRecoverLeaseResponse();
    KeyInfo keyInfo = recoverLeaseResponse.getKeyInfo();
    assertNotNull(keyInfo);
    OmKeyInfo omKeyInfo = OmKeyInfo.getFromProtobuf(keyInfo);

    // call allocate block
    OMRequest request = createAllocateBlockRequest(volumeName, bucketName, keyName);
    OMAllocateBlockRequestWithFSO omAllocateBlockRequest =
        new OMAllocateBlockRequestWithFSO(request, getBucketLayout());
    request = omAllocateBlockRequest.preExecute(ozoneManager);
    assertNotNull(request.getUserInfo());
    omAllocateBlockRequest = new OMAllocateBlockRequestWithFSO(request, getBucketLayout());
    omClientResponse = omAllocateBlockRequest.validateAndUpdateCache(
        ozoneManager, 100L);
    assertEquals(OzoneManagerProtocolProtos.Status.KEY_UNDER_LEASE_RECOVERY,
        omClientResponse.getOMResponse().getStatus());

    // call commit(hsync calls commit)
    KeyArgs newKeyArgs = getNewKeyArgs(omKeyInfo, 0);
    omClientResponse = validateAndUpdateCacheForCommit(newKeyArgs, false, false);
    assertEquals(OzoneManagerProtocolProtos.Status.KEY_UNDER_LEASE_RECOVERY,
        omClientResponse.getOMResponse().getStatus());
  }

  /**
   * verify that recover a closed file.
   **/
  @Test
  public void testRecoverClosedFile() throws Exception {
    populateNamespace(true, false, false, false);

    OMClientResponse omClientResponse = validateAndUpdateCache();

    assertEquals(OzoneManagerProtocolProtos.Status.KEY_ALREADY_CLOSED,
        omClientResponse.getOMResponse().getStatus());

    verifyTables(true, false);
  }

  /**
   * verify that recover an open (not yet hsync'ed) file doesn't work.
    */
  @Test
  public void testRecoverOpenFile() throws Exception {
    populateNamespace(false, false, true, false);

    OMClientResponse omClientResponse = validateAndUpdateCache();

    assertEquals(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    verifyTables(false, true);
  }

  /**
   * Verify that recovering a file that doesn't exist throws an exception.
   * @throws Exception
   */
  @Test
  public void testRecoverAbsentFile() throws Exception {
    populateNamespace(false, false, false, false);

    OMClientResponse omClientResponse = validateAndUpdateCache();

    assertEquals(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    verifyTables(false, false);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testLeaseSoftLimitForHsyncRecoverFile(boolean force) throws Exception {
    forceRecovery = force;
    populateNamespace(true, true, true, true);

    // update soft limit to high value
    ozoneManager.getConfiguration().set(OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT, "2s");
    OMClientResponse omClientResponse = validateAndUpdateCache();
    OMResponse omResponse = omClientResponse.getOMResponse();
    RecoverLeaseResponse recoverLeaseResponse;
    if (force) {
      // In case of force it should always succeed irrespective of soft limit value.
      assertEquals(OzoneManagerProtocolProtos.Status.OK,
          omResponse.getStatus());
      recoverLeaseResponse = omResponse.getRecoverLeaseResponse();
      KeyInfo keyInfo = recoverLeaseResponse.getKeyInfo();
      assertNotNull(keyInfo);
    } else {
      // Call recovery inside soft limit period it should fail
      assertEquals(OzoneManagerProtocolProtos.Status.KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD,
          omResponse.getStatus());
    }
    omClientResponse = validateAndUpdateCache();
    omResponse = omClientResponse.getOMResponse();
    if (force) {
      assertEquals(OzoneManagerProtocolProtos.Status.OK,
          omResponse.getStatus());
      recoverLeaseResponse = omResponse.getRecoverLeaseResponse();
      KeyInfo keyInfo = recoverLeaseResponse.getKeyInfo();
      assertNotNull(keyInfo);
    } else {
      // Call second time inside soft limit period also should fail
      assertEquals(OzoneManagerProtocolProtos.Status.KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD,
          omResponse.getStatus());
    }
    Thread.sleep(2000);
    // Call recovery after soft limit period it should succeed
    omClientResponse = validateAndUpdateCache();
    omResponse = omClientResponse.getOMResponse();
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());
    recoverLeaseResponse = omResponse.getRecoverLeaseResponse();
    KeyInfo keyInfo = recoverLeaseResponse.getKeyInfo();
    assertNotNull(keyInfo);

    // Call recovery again it should succeed
    omClientResponse = validateAndUpdateCache();
    omResponse = omClientResponse.getOMResponse();
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());
    recoverLeaseResponse = omResponse.getRecoverLeaseResponse();
    keyInfo = recoverLeaseResponse.getKeyInfo();
    assertNotNull(keyInfo);
  }

  /**
   * Recovery fences an append session before any allocation (empty suffix), with unpublished suffix blocks, and with
   * a published suffix. Only blocks of the session get their pipeline refreshed, never a prefix block.
   */
  @ParameterizedTest
  @CsvSource({"2,0,0", "1,0,1", "1,0,3", "1,2,1", "0,1,0"})
  public void testRecoverAppendSession(int prefixBlocks, int publishedBlocks, int unpublishedBlocks) throws Exception {
    long oldTime = Time.now() - 10_000;
    addAppendSession(prefixBlocks, publishedBlocks, unpublishedBlocks, oldTime, oldTime, APPEND_ACTIVE);
    ozoneManager.getConfiguration().set(OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT, "1s");

    OMResponse omResponse = validateAndUpdateCache().getOMResponse();
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());

    OmKeyInfo committed = OmKeyInfo.getFromProtobuf(omResponse.getRecoverLeaseResponse().getKeyInfo());
    assertEquals(clientID, committed.getAppendOwnerSessionId());
    List<OmKeyLocationInfo> committedBlocks = committed.getLatestVersionLocations().createLocationList();
    assertThat(committedBlocks).hasSize(prefixBlocks + publishedBlocks);
    assertThat(committedBlocks.subList(0, prefixBlocks)).allMatch(b -> b.getPipeline() == null);
    if (publishedBlocks > 0) {
      assertNotNull(committedBlocks.get(committedBlocks.size() - 1).getPipeline());
    }

    OmKeyInfo open = OmKeyInfo.getFromProtobuf(omResponse.getRecoverLeaseResponse().getOpenKeyInfo());
    assertEquals(APPEND_RECOVERING, open.getAppendSession().getPhase());
    assertEquals(prefixBlocks, open.getAppendSession().getPrefixBlockCount());
    List<OmKeyLocationInfo> suffix = open.getLatestVersionLocations().createLocationList();
    int suffixBlocks = publishedBlocks + unpublishedBlocks;
    assertThat(suffix).hasSize(suffixBlocks);
    int refreshed = Math.min(2, suffixBlocks);
    assertThat(suffix.subList(suffixBlocks - refreshed, suffixBlocks)).allMatch(b -> b.getPipeline() != null);
    for (int i = 0; i < prefixBlocks; i++) {
      verify(scmContainerLocationProtocol, never()).getContainerWithPipeline(1000L + i);
    }
    if (suffixBlocks == 0) {
      verify(scmContainerLocationProtocol, never()).getContainerWithPipeline(anyLong());
    }

    // The open record is fenced in place, without the hsync recovery markers, and stays in the session index.
    String openKey = getOpenFileName();
    OmKeyInfo fenced = omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKey);
    assertEquals(APPEND_RECOVERING, fenced.getAppendSession().getPhase());
    assertEquals(oldTime, fenced.getAppendSession().getLastRenewedAt());
    assertEquals(100L, fenced.getUpdateID());
    assertEquals(oldTime, fenced.getModificationTime());
    assertThat(fenced.getMetadata()).doesNotContainKeys(OzoneConsts.LEASE_RECOVERY, OzoneConsts.HSYNC_CLIENT_ID);
    assertEquals(openKey, omMetadataManager.getAppendSessionOpenKey(volumeName, bucketName, clientID));
    OmKeyInfo owner = omMetadataManager.getKeyTable(getBucketLayout()).get(getFileName());
    assertEquals(clientID, owner.getAppendOwnerSessionId());
    assertEquals(oldTime, owner.getModificationTime());
  }

  /**
   * The append soft limit is measured from the last lease renewal, not from the modification time.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testLeaseSoftLimitForAppendSession(boolean force) throws Exception {
    forceRecovery = force;
    // old modification time, fresh renewal
    addAppendSession(1, 1, 0, Time.now() - TimeUnit.DAYS.toMillis(1), Time.now(), APPEND_ACTIVE);
    ozoneManager.getConfiguration().set(OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT, "1h");

    OMResponse omResponse = validateAndUpdateCache().getOMResponse();
    AppendSessionPhase phase = omMetadataManager.getOpenKeyTable(getBucketLayout()).get(getOpenFileName())
        .getAppendSession().getPhase();
    if (force) {
      assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());
      assertEquals(APPEND_RECOVERING, phase);
      // A later call without force joins the recovery although the lease is inside the soft limit.
      forceRecovery = false;
      omResponse = validateAndUpdateCache().getOMResponse();
      assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());
      OmKeyInfo joined = OmKeyInfo.getFromProtobuf(omResponse.getRecoverLeaseResponse().getOpenKeyInfo());
      assertEquals(APPEND_RECOVERING, joined.getAppendSession().getPhase());
    } else {
      assertEquals(OzoneManagerProtocolProtos.Status.KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD, omResponse.getStatus());
      assertEquals(APPEND_ACTIVE, phase);
    }
  }

  /**
   * The leader decides whether the lease is past the soft limit. An OM that applies the request later, when its own
   * clock or soft limit says otherwise, does not decide again.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAppendSoftLimitDecidedByLeader(boolean expiredOnLeader) throws Exception {
    addAppendSession(1, 1, 0, Time.now(), Time.now(), APPEND_ACTIVE);
    ozoneManager.getConfiguration().set(OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT, expiredOnLeader ? "0s" : "1h");
    OMRequest request = doPreExecute(createRecoverLeaseRequest(volumeName, bucketName, keyName, false));
    ozoneManager.getConfiguration().set(OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT, expiredOnLeader ? "1h" : "0s");

    assertEquals(expiredOnLeader ? OzoneManagerProtocolProtos.Status.OK
            : OzoneManagerProtocolProtos.Status.KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD,
        getOmRecoverLeaseRequest(request).validateAndUpdateCache(ozoneManager, 100L).getOMResponse().getStatus());
    assertEquals(expiredOnLeader ? APPEND_RECOVERING : APPEND_ACTIVE,
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(getOpenFileName()).getAppendSession().getPhase());

    if (!expiredOnLeader) {
      // A request that was logged before the leader supplied the cutoff is still decided locally.
      OMRequest logged = request.toBuilder()
          .setRecoverLeaseRequest(request.getRecoverLeaseRequest().toBuilder().clearLeaseSoftLimitCutoff()).build();
      assertEquals(OzoneManagerProtocolProtos.Status.OK,
          getOmRecoverLeaseRequest(logged).validateAndUpdateCache(ozoneManager, 101L).getOMResponse().getStatus());
    }
  }

  @Test
  public void testRecoverAppendSessionIgnoresModificationTime() throws Exception {
    // fresh modification time, old renewal
    addAppendSession(1, 1, 0, Time.now(), Time.now() - TimeUnit.HOURS.toMillis(2), APPEND_ACTIVE);
    ozoneManager.getConfiguration().set(OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT, "1h");

    assertEquals(OzoneManagerProtocolProtos.Status.OK, validateAndUpdateCache().getOMResponse().getStatus());
    assertEquals(APPEND_RECOVERING, omMetadataManager.getOpenKeyTable(getBucketLayout()).get(getOpenFileName())
        .getAppendSession().getPhase());
  }

  @Test
  public void testRecoverAppendSessionWithoutIndexEntry() throws Exception {
    addAppendSession(1, 0, 1, 0, 0, APPEND_ACTIVE);
    omMetadataManager.removeAppendSession(volumeName, bucketName, clientID);

    assertEquals(OzoneManagerProtocolProtos.Status.OK, validateAndUpdateCache().getOMResponse().getStatus());
    assertEquals(APPEND_RECOVERING, omMetadataManager.getOpenKeyTable(getBucketLayout()).get(getOpenFileName())
        .getAppendSession().getPhase());
  }

  @Test
  public void testRecoverAppendSessionWithoutOpenRecord() throws Exception {
    addAppendSession(1, 0, 0, 0, 0, APPEND_ACTIVE);
    omMetadataManager.getOpenKeyTable(getBucketLayout()).delete(getOpenFileName());

    assertEquals(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND, validateAndUpdateCache().getOMResponse().getStatus());
    assertEquals(clientID,
        omMetadataManager.getKeyTable(getBucketLayout()).get(getFileName()).getAppendOwnerSessionId());
  }

  /**
   * Adds a file owned by append session {@code clientID} the way admission, allocation and hsync leave it: the
   * committed record holds the prefix followed by the published suffix, the open record holds only the suffix.
   */
  private void addAppendSession(int prefixBlocks, int publishedBlocks, int unpublishedBlocks, long modificationTime,
      long lastRenewedAt, AppendSessionPhase phase) throws Exception {
    String parentDir = "c/d/e";
    keyName = parentDir + "/f";
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager, getBucketLayout());
    parentId = OMRequestTestUtils.addParentsToDirTable(volumeName, bucketName, parentDir, omMetadataManager);
    List<OmKeyLocationInfo> blocks = getKeyLocation(prefixBlocks + publishedBlocks + unpublishedBlocks);

    OmKeyInfo committed = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName, replicationConfig,
            new OmKeyLocationInfoGroup(version, new ArrayList<>(), false))
        .setParentObjectID(parentId).setModificationTime(modificationTime).setAppendOwnerSessionId(clientID).build();
    committed.appendNewBlocks(blocks.subList(0, prefixBlocks + publishedBlocks), false);
    OMRequestTestUtils.addFileToKeyTable(false, false, committed.getFileName(), committed, clientID,
        committed.getUpdateID(), omMetadataManager);

    OmKeyInfo open = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName, replicationConfig,
            new OmKeyLocationInfoGroup(version, new ArrayList<>(), false))
        .setParentObjectID(parentId).setModificationTime(modificationTime)
        .setAppendSession(new OmAppendSession(phase, 200L * prefixBlocks, prefixBlocks, lastRenewedAt, lastRenewedAt))
        .build();
    open.appendNewBlocks(blocks.subList(prefixBlocks, blocks.size()), false);
    String openKey = OMRequestTestUtils.addFileToKeyTable(true, false, open.getFileName(), open, clientID,
        open.getUpdateID(), omMetadataManager);
    omMetadataManager.putAppendSession(volumeName, bucketName, clientID, openKey);
  }

  private KeyArgs getNewKeyArgs(OmKeyInfo omKeyInfo, long deltaLength) throws IOException {
    OmKeyLocationInfoGroup omKeyLocationInfoGroup = omKeyInfo.getLatestVersionLocations();
    List<OmKeyLocationInfo> omKeyLocationInfoList = omKeyLocationInfoGroup.getBlocksLatestVersionOnly();
    long lastBlockLength = omKeyLocationInfoList.get(omKeyLocationInfoList.size() - 1).getLength();
    omKeyLocationInfoList.get(omKeyLocationInfoList.size() - 1).setLength(lastBlockLength + deltaLength);

    long fileLength = omKeyLocationInfoList.stream().mapToLong(OmKeyLocationInfo::getLength).sum();
    omKeyInfo.setDataSize(fileLength);
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(omKeyInfo.getVolumeName())
        .setBucketName(omKeyInfo.getBucketName()).setKeyName(omKeyInfo.getKeyName())
        .setReplicationConfig(omKeyInfo.getReplicationConfig()).setDataSize(fileLength)
        .setLocationInfoList(omKeyLocationInfoList).setLatestVersionLocation(true)
        .build();

    List<OmKeyLocationInfo> locationInfoList = keyArgs.getLocationInfoList();
    Objects.requireNonNull(locationInfoList, "locationInfoList == null");
    KeyArgs.Builder keyArgsBuilder = KeyArgs.newBuilder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setDataSize(keyArgs.getDataSize())
        .addAllMetadata(KeyValueUtil.toProtobuf(keyArgs.getMetadata()))
        .addAllKeyLocations(locationInfoList.stream()
            .map(info -> info.getProtobuf(ClientVersion.CURRENT_VERSION))
            .collect(Collectors.toList()));
    setReplicationConfig(keyArgs.getReplicationConfig(), keyArgsBuilder);
    return keyArgsBuilder.build();
  }

  private void populateNamespace(boolean addKeyTable, boolean keyInfoWithHsyncFlag,
      boolean addOpenKeyTable, boolean openKeyInfoWithHsyncFlag) throws Exception {
    String parentDir = "c/d/e";
    String fileName = "f";
    keyName = parentDir + "/" + fileName;
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    // Create parent dirs for the path
    parentId = OMRequestTestUtils.addParentsToDirTable(volumeName,
        bucketName, parentDir, omMetadataManager);

    // add to both key table and open key table
    List<OmKeyLocationInfo> allocatedLocationList = getKeyLocation(3);

    OmKeyInfo omKeyInfo;
    if (addKeyTable) {
      String ozoneKey = addToFileTable(allocatedLocationList, keyInfoWithHsyncFlag);
      omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout())
          .get(ozoneKey);
      assertNotNull(omKeyInfo);
    }

    if (addOpenKeyTable) {
      String openKey = addToOpenFileTable(allocatedLocationList, openKeyInfoWithHsyncFlag);

      omKeyInfo = omMetadataManager.getOpenKeyTable(getBucketLayout())
          .get(openKey);
      assertNotNull(omKeyInfo);
    }

    // Set lease soft limit to 0
    ozoneManager.getConfiguration().set(OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT, "0s");
  }

  protected OMRequest createAllocateBlockRequest(String volumeName, String bucketName, String keyName) {
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName)
        .setFactor(((RatisReplicationConfig) replicationConfig).getReplicationFactor())
        .setType(replicationConfig.getReplicationType())
        .build();

    AllocateBlockRequest allocateBlockRequest =
        AllocateBlockRequest.newBuilder().setClientID(clientID).setKeyArgs(keyArgs).build();

    return OMRequest.newBuilder()
          .setCmdType(OzoneManagerProtocolProtos.Type.AllocateBlock)
          .setClientId(UUID.randomUUID().toString())
          .setAllocateBlockRequest(allocateBlockRequest).build();
  }

  @Nonnull
  protected OMRequest createRecoverLeaseRequest(
      String volumeName, String bucketName, String keyName, boolean force) {
    RecoverLeaseRequest.Builder rb = RecoverLeaseRequest.newBuilder();
    rb.setVolumeName(volumeName).setBucketName(bucketName).setKeyName(keyName).setForce(force);
    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.RecoverLease)
        .setClientId(UUID.randomUUID().toString())
        .setRecoverLeaseRequest(rb.build()).build();
  }

  private OMClientResponse validateAndUpdateCache() throws Exception {
    OMRequest modifiedOmRequest = doPreExecute(createRecoverLeaseRequest(
        volumeName, bucketName, keyName, forceRecovery));
    assertNotNull(modifiedOmRequest.getUserInfo());

    OMRecoverLeaseRequest omRecoverLeaseRequest = getOmRecoverLeaseRequest(
        modifiedOmRequest);

    OMClientResponse omClientResponse =
        omRecoverLeaseRequest.validateAndUpdateCache(ozoneManager, 100L);
    return omClientResponse;
  }

  @Nonnull
  protected OMRequest createKeyCommitRequest(KeyArgs keyArgs, boolean newClientID, boolean recovery) {
    CommitKeyRequest.Builder rb =
        CommitKeyRequest.newBuilder().setKeyArgs(keyArgs).setRecovery(recovery);
    rb.setClientID(newClientID ? clientID + 1 : clientID);
    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CommitKey)
        .setClientId(UUID.randomUUID().toString())
        .setCommitKeyRequest(rb.build()).build();
  }

  private OMClientResponse validateAndUpdateCacheForCommit(KeyArgs keyArgs) throws Exception {
    return validateAndUpdateCacheForCommit(keyArgs, false, true);
  }

  private OMClientResponse validateAndUpdateCacheForCommit(KeyArgs keyArgs, boolean newClientID,
      boolean recovery) throws Exception {
    OMRequest omRequest = createKeyCommitRequest(keyArgs, newClientID, recovery);
    OMKeyCommitRequestWithFSO omKeyCommitRequest = new OMKeyCommitRequestWithFSO(omRequest, getBucketLayout());
    OMRequest modifiedOmRequest = omKeyCommitRequest.preExecute(ozoneManager);
    assertNotNull(modifiedOmRequest.getUserInfo());

    omKeyCommitRequest = new OMKeyCommitRequestWithFSO(modifiedOmRequest, getBucketLayout());
    OMClientResponse omClientResponse =
        omKeyCommitRequest.validateAndUpdateCache(ozoneManager, 100L);
    return omClientResponse;
  }

  private void verifyTables(boolean hasKey, boolean hasOpenKey)
      throws IOException {
    // Now entry should be created in key Table.
    String ozoneKey = getFileName();
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout())
        .get(ozoneKey);
    if (hasKey) {
      assertNotNull(omKeyInfo);
    } else {
      assertNull(omKeyInfo);
    }
    // Entry should be deleted from openKey Table.
    String openKey = getOpenFileName();
    omKeyInfo = omMetadataManager.getOpenKeyTable(getBucketLayout())
        .get(openKey);
    if (hasOpenKey) {
      assertNotNull(omKeyInfo);
    } else {
      assertNull(omKeyInfo);
    }
  }

  String getOpenFileName() throws IOException {
    final long volumeId = omMetadataManager.getVolumeId(
        volumeName);
    final long bucketId = omMetadataManager.getBucketId(
        volumeName, bucketName);
    final String fileName = OzoneFSUtils.getFileName(keyName);
    return omMetadataManager.getOpenFileName(volumeId, bucketId,
        parentId, fileName, clientID);
  }

  String getFileName() throws IOException {
    final long volumeId = omMetadataManager.getVolumeId(
        volumeName);
    final long bucketId = omMetadataManager.getBucketId(
        volumeName, bucketName);
    final String fileName = OzoneFSUtils.getFileName(keyName);
    return omMetadataManager.getOzonePathKey(volumeId, bucketId, parentId,
        fileName);
  }

  protected OMRecoverLeaseRequest getOmRecoverLeaseRequest(OMRequest omRequest) {
    return new OMRecoverLeaseRequest(omRequest, getBucketLayout());
  }

  private List<OmKeyLocationInfo> getKeyLocation(int count) {
    List<KeyLocation> keyLocations = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      KeyLocation keyLocation =
          KeyLocation.newBuilder()
              .setBlockID(HddsProtos.BlockID.newBuilder()
                  .setContainerBlockID(HddsProtos.ContainerBlockID.newBuilder()
                      .setContainerID(i + 1000).setLocalID(i + 100).build()))
              .setOffset(0).setLength(200).setCreateVersion(version).build();
      keyLocations.add(keyLocation);
    }
    return keyLocations.stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());
  }

  private OMRequest doPreExecute(OMRequest originalOMRequest) throws Exception {
    OMRecoverLeaseRequest omRecoverLeaseRequest =
        getOmRecoverLeaseRequest(originalOMRequest);

    OMRequest modifiedOmRequest = omRecoverLeaseRequest.preExecute(
        ozoneManager);

    return modifiedOmRequest;
  }

  String addToOpenFileTable(List<OmKeyLocationInfo> locationList, boolean hsyncFlag)
      throws Exception {
    OmKeyInfo.Builder keyInfoBuilder = OMRequestTestUtils.createOmKeyInfo(volumeName,
            bucketName, keyName, replicationConfig, new OmKeyLocationInfoGroup(version, new ArrayList<>(), false))
        .setParentObjectID(parentId);
    if (hsyncFlag) {
      keyInfoBuilder.addMetadata(OzoneConsts.HSYNC_CLIENT_ID,
          String.valueOf(clientID));
    }
    OmKeyInfo omKeyInfo = keyInfoBuilder.build();
    omKeyInfo.appendNewBlocks(locationList, false);

    OMRequestTestUtils.addFileToKeyTable(
        true, false, omKeyInfo.getFileName(),
        omKeyInfo, clientID, omKeyInfo.getUpdateID(), omMetadataManager);

    final long volumeId = omMetadataManager.getVolumeId(
        omKeyInfo.getVolumeName());
    final long bucketId = omMetadataManager.getBucketId(
        omKeyInfo.getVolumeName(), omKeyInfo.getBucketName());

    return omMetadataManager.getOpenFileName(volumeId, bucketId,
        omKeyInfo.getParentObjectID(), omKeyInfo.getFileName(), clientID);
  }

  String addToFileTable(List<OmKeyLocationInfo> locationList, boolean hsyncFlag)
      throws Exception {
    OmKeyInfo.Builder keyInfoBuilder = OMRequestTestUtils.createOmKeyInfo(volumeName,
            bucketName, keyName, replicationConfig, new OmKeyLocationInfoGroup(version, new ArrayList<>(), false))
        .setParentObjectID(parentId);
    if (hsyncFlag) {
      keyInfoBuilder.addMetadata(OzoneConsts.HSYNC_CLIENT_ID,
          String.valueOf(clientID));
    }
    OmKeyInfo omKeyInfo = keyInfoBuilder.build();
    omKeyInfo.appendNewBlocks(locationList, false);

    OMRequestTestUtils.addFileToKeyTable(
        false, false, omKeyInfo.getFileName(),
        omKeyInfo, clientID, omKeyInfo.getUpdateID(), omMetadataManager);

    final long volumeId = omMetadataManager.getVolumeId(
        omKeyInfo.getVolumeName());
    final long bucketId = omMetadataManager.getBucketId(
        omKeyInfo.getVolumeName(), omKeyInfo.getBucketName());

    return omMetadataManager.getOzonePathKey(volumeId, bucketId, parentId,
        omKeyInfo.getFileName());
  }

}
