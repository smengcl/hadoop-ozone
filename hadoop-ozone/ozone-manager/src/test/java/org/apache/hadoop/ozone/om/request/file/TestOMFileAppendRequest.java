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

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.APPEND_NOT_SUPPORTED;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.APPEND_SESSION_NOT_FOUND;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.APPEND_WRITER_CONFLICT;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.INVALID_REQUEST;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.NOT_A_FILE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmMetadataReader;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmAppendSession;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.key.OMAllocateBlockRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.OMKeyCommitRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.OMKeyCreateRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.OMKeyRenameRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequestTests;
import org.apache.hadoop.ozone.om.request.key.OMOpenKeysDeleteRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3InitiateMultipartUploadRequestWithFSO;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadCommitPartRequestWithFSO;
import org.apache.hadoop.ozone.om.request.util.OmAppendUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCommitResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendConflictInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendFileResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionKey;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionPhase;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendWriterKind;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteOpenKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKey;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKeyBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RecoverLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenewAppendLeasesRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests append sessions in OM: admission by {@link OMFileAppendRequest} and the session's later requests.
 */
public class TestOMFileAppendRequest extends OMKeyRequestTests {

  private static final String PARENT_DIR = "c/d/e";
  private static final long BLOCK_LENGTH = 200;
  private static final long PREFIX_CONTAINER_ID = 5000;
  private static final long SUFFIX_CONTAINER_ID = 6000;
  private static final long ORDINARY_CONTAINER_ID = 7000;
  private static final OzoneAcl ACL = OzoneAcl.parseAcl("user:alice:r");
  private static final UserInfo CALLER = UserInfo.newBuilder().setUserName("writer").build();

  private long parentId;
  private String dbFileKey;
  private long txnId = 1000;
  private int allocatedSuffixBlocks;
  private final List<String> deniedPaths = new ArrayList<>();

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  @BeforeEach
  public void init() throws Exception {
    ozoneManager.getConfiguration().setBoolean(OMConfigKeys.OZONE_OM_APPEND_ENABLED, true);
    keyName = PARENT_DIR + "/f";
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager, getBucketLayout());
    parentId = OMRequestTestUtils.addParentsToDirTable(volumeName, bucketName, PARENT_DIR, omMetadataManager);
    dbFileKey = omMetadataManager.getOzonePathKey(omMetadataManager.getVolumeId(volumeName),
        omMetadataManager.getBucketId(volumeName, bucketName), parentId, "f");
  }

  @Test
  public void testAdmission() throws Exception {
    OmKeyInfo before = addCommittedFile(2);

    OMClientResponse response = append();

    assertThat(response.getOMResponse().getStatus()).isEqualTo(OK);
    AppendFileResponse appendResponse = response.getOMResponse().getAppendFileResponse();
    long sessionId = appendResponse.getID();
    assertThat(sessionId).isNotZero();
    assertThat(appendResponse.getOpenVersion()).isEqualTo(version);
    OmKeyInfo returned = OmKeyInfo.getFromProtobuf(appendResponse.getKeyInfo());
    assertThat(returned.getKeyName()).isEqualTo(keyName);
    assertThat(returned.getLatestVersionLocations().getLocationListCount()).isZero();
    assertThat(returned.getAppendSession().getPrefixLength()).isEqualTo(2 * BLOCK_LENGTH);

    String dbOpenKey = dbOpenKey(sessionId);
    assertThat(omMetadataManager.getAppendSessionOpenKey(volumeName, bucketName, sessionId)).isEqualTo(dbOpenKey);

    flush(response);
    OmKeyInfo committed = omMetadataManager.getKeyTable(getBucketLayout()).getSkipCache(dbFileKey);
    assertThat(committed.getAppendOwnerSessionId()).isEqualTo(sessionId);
    assertThat(committed.getAppendSession()).isNull();
    assertThat(committed.getUpdateID()).isEqualTo(txnId);
    assertThat(committed.getDataSize()).isEqualTo(before.getDataSize());
    assertThat(committed.getModificationTime()).isEqualTo(before.getModificationTime());
    assertThat(blockIds(committed)).isEqualTo(blockIds(before));
    assertThat(committed.getMetadata()).doesNotContainKey(OzoneConsts.HSYNC_CLIENT_ID);

    OmKeyInfo open = omMetadataManager.getOpenKeyTable(getBucketLayout()).getSkipCache(dbOpenKey);
    OmAppendSession session = open.getAppendSession();
    assertThat(session.getPhase()).isEqualTo(AppendSessionPhase.APPEND_ACTIVE);
    assertThat(session.getPrefixLength()).isEqualTo(2 * BLOCK_LENGTH);
    assertThat(session.getPrefixBlockCount()).isEqualTo(2);
    assertThat(session.getLastRenewedAt()).isEqualTo(session.getOpenedAt()).isPositive();
    assertThat(open.getAppendOwnerSessionId()).isNull();
    assertThat(open.getKeyName()).isEqualTo(keyName);
    assertThat(open.getParentObjectID()).isEqualTo(parentId);
    assertThat(open.getObjectID()).isEqualTo(before.getObjectID());
    assertThat(open.getCreationTime()).isEqualTo(before.getCreationTime());
    assertThat(open.getDataSize()).isEqualTo(2 * BLOCK_LENGTH);
    assertThat(open.getKeyLocationVersions()).hasSize(1);
    assertThat(open.getLatestVersionLocations().getLocationListCount()).isZero();
  }

  @Test
  public void testConflictWithAppendWriter() throws Exception {
    addCommittedFile(1);
    long owner = append().getOMResponse().getAppendFileResponse().getID();

    AppendConflictInfo conflict = assertConflict(append(), AppendWriterKind.APPEND_WRITER);

    assertThat(conflict.getSessionId()).isEqualTo(owner);
    assertThat(conflict.getPhase()).isEqualTo(AppendSessionPhase.APPEND_ACTIVE);
    assertThat(conflict.getLastRenewedAt()).isEqualTo(openRecord(owner).getAppendSession().getLastRenewedAt());
    assertThat(conflict.getRemainingMsUntilRecoverable()).isPositive().isLessThanOrEqualTo(60_000);
    assertThat(committedFile().getAppendOwnerSessionId()).isEqualTo(owner);
  }

  @Test
  public void testConflictWithHsyncWriter() throws Exception {
    addCommittedFile(1, OzoneConsts.HSYNC_CLIENT_ID, "1");

    assertConflict(append(), AppendWriterKind.HSYNC_WRITER);

    assertThat(committedFile().getAppendOwnerSessionId()).isNull();
  }

  @Test
  public void testConflictWithOrdinaryWriter() throws Exception {
    addCommittedFile(1);
    // A flushed create or overwrite.
    String flushedWriter = addOrdinaryOpenRecord(11, true);
    assertConflict(append(), AppendWriterKind.ORDINARY_WRITER);

    // Committed and flushed in the meantime, but a new writer was applied and is not flushed yet.
    omMetadataManager.getOpenKeyTable(getBucketLayout()).delete(flushedWriter);
    String unflushedWriter = addOrdinaryOpenRecord(12, false);
    assertConflict(append(), AppendWriterKind.ORDINARY_WRITER);

    // The removal of a writer was applied and is not flushed yet: the tombstone overrides the DB row.
    omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(new CacheKey<>(unflushedWriter),
        CacheValue.get(++txnId));
    String removedWriter = addOrdinaryOpenRecord(13, true);
    omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(new CacheKey<>(removedWriter),
        CacheValue.get(++txnId));
    assertThat(append().getOMResponse().getStatus()).isEqualTo(OK);
  }

  @Test
  public void testFeatureDisabled() throws Exception {
    addCommittedFile(1);
    ozoneManager.getConfiguration().setBoolean(OMConfigKeys.OZONE_OM_APPEND_ENABLED, false);

    assertThatThrownBy(this::append).isInstanceOfSatisfying(OMException.class,
        e -> assertThat(e.getResult()).isEqualTo(OMException.ResultCodes.APPEND_NOT_SUPPORTED));

    assertThat(committedFile().getAppendOwnerSessionId()).isNull();
  }

  @Test
  public void testGdprFileIsRejected() throws Exception {
    addCommittedFile(1, OzoneConsts.GDPR_FLAG, "true");

    assertThat(append().getOMResponse().getStatus()).isEqualTo(APPEND_NOT_SUPPORTED);

    assertThat(committedFile().getAppendOwnerSessionId()).isNull();
  }

  @Test
  public void testMissingFileAndDirectory() throws Exception {
    assertThat(append().getOMResponse().getStatus()).isEqualTo(KEY_NOT_FOUND);

    keyName = "missing/dir/f";
    assertThat(append().getOMResponse().getStatus()).isEqualTo(KEY_NOT_FOUND);

    keyName = PARENT_DIR;
    assertThat(append().getOMResponse().getStatus()).isEqualTo(NOT_A_FILE);
  }

  @Test
  public void testAllocateBlockAddsToSuffixOnly() throws Exception {
    OmKeyInfo before = addCommittedFile(2);
    long sessionId = admit();

    OMClientResponse response = allocate(sessionId);

    assertThat(response.getOMResponse().getStatus()).isEqualTo(OK);
    flush(response);
    OmKeyInfo open = omMetadataManager.getOpenKeyTable(getBucketLayout()).getSkipCache(dbOpenKey(sessionId));
    assertThat(blockIds(open)).containsExactly(suffixBlockId(0));
    assertThat(open.getKeyName()).isEqualTo(keyName);
    assertThat(open.getAppendSession().isActive()).isTrue();
    assertThat(blockIds(committedFile())).isEqualTo(blockIds(before));
    assertThat(committedFile().getDataSize()).isEqualTo(before.getDataSize());
  }

  @Test
  public void testAllocateBlockRejectedForFencedOrForeignSession() throws Exception {
    addCommittedFile(1);
    long sessionId = admit();
    OmKeyInfo reserved = committedFile();

    // Fenced by lease recovery.
    setPhase(sessionId, AppendSessionPhase.APPEND_RECOVERING);
    assertThat(allocate(sessionId).getOMResponse().getStatus()).isEqualTo(APPEND_SESSION_NOT_FOUND);
    setPhase(sessionId, AppendSessionPhase.APPEND_ACTIVE);

    // The file is reserved by another session.
    omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(new CacheKey<>(dbFileKey),
        CacheValue.get(++txnId, reserved.toBuilder().setAppendOwnerSessionId(sessionId + 1).build()));
    assertThat(allocate(sessionId).getOMResponse().getStatus()).isEqualTo(APPEND_SESSION_NOT_FOUND);
    omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(new CacheKey<>(dbFileKey),
        CacheValue.get(++txnId, reserved));

    // No longer in the session index, but the open record is still found through the path.
    omMetadataManager.removeAppendSession(volumeName, bucketName, sessionId);
    assertThat(allocate(sessionId).getOMResponse().getStatus()).isEqualTo(APPEND_SESSION_NOT_FOUND);

    assertThat(openRecord(sessionId).getLatestVersionLocations().getLocationListCount()).isZero();
  }

  @Test
  public void testSessionOfFileUnderDeletedDirectoryIsRejected() throws Exception {
    addCommittedFile(1);
    long sessionId = admit();
    assertThat(allocate(sessionId).getOMResponse().getStatus()).isEqualTo(OK);

    // A recursive delete removes only the top directory row. The session's rows wait for the directory cleanup.
    long bucketId = omMetadataManager.getBucketId(volumeName, bucketName);
    omMetadataManager.getDirectoryTable().addCacheEntry(new CacheKey<>(omMetadataManager.getOzonePathKey(
        omMetadataManager.getVolumeId(volumeName), bucketId, bucketId, "c")), CacheValue.get(++txnId));

    assertThat(allocate(sessionId).getOMResponse().getStatus()).isEqualTo(KEY_NOT_FOUND);
    assertThat(hsync(sessionId, 2 * BLOCK_LENGTH, BLOCK_LENGTH).getOMResponse().getStatus()).isEqualTo(KEY_NOT_FOUND);
    assertThat(close(sessionId, 2 * BLOCK_LENGTH, BLOCK_LENGTH).getOMResponse().getStatus()).isEqualTo(KEY_NOT_FOUND);
    assertThat(blockIds(committedFile())).containsExactly(prefixBlockId(0));
    assertThat(openRecord(sessionId).getAppendSession().isActive()).isTrue();
  }

  @Test
  public void testHsyncPublishesPrefixAndSuffix() throws Exception {
    // A location version other than 0 shows that the suffix blocks join the committed version.
    version = 2;
    OmKeyInfo before = addCommittedFile(2);
    long sessionId = admit();
    allocate(sessionId);
    allocate(sessionId);
    long usedBytes = bucketUsedBytes();

    OMClientResponse response = hsync(sessionId, 2 * BLOCK_LENGTH + 150, 150);

    assertThat(response.getOMResponse().getStatus()).isEqualTo(OK);
    flush(response);
    // Read back from the DB: the block order must survive the codec.
    OmKeyInfo committed = omMetadataManager.getKeyTable(getBucketLayout()).getSkipCache(dbFileKey);
    assertThat(blockIds(committed)).containsExactly(prefixBlockId(0), prefixBlockId(1), suffixBlockId(0));
    assertThat(committed.getDataSize()).isEqualTo(2 * BLOCK_LENGTH + 150);
    assertThat(committed.getModificationTime()).isGreaterThan(before.getModificationTime());
    assertThat(committed.getAppendOwnerSessionId()).isEqualTo(sessionId);
    assertThat(committed.getMetadata()).doesNotContainKey(OzoneConsts.HSYNC_CLIENT_ID);
    assertThat(blockIds(openRecord(sessionId))).containsExactly(suffixBlockId(0), suffixBlockId(1));
    assertThat(omMetadataManager.getAppendSessionOpenKey(volumeName, bucketName, sessionId))
        .isEqualTo(dbOpenKey(sessionId));
    assertThat(bucketUsedBytes()).isEqualTo(usedBytes + 150);

    // The same publication again changes nothing.
    assertThat(hsync(sessionId, 2 * BLOCK_LENGTH + 150, 150).getOMResponse().getStatus()).isEqualTo(OK);
    assertThat(bucketUsedBytes()).isEqualTo(usedBytes + 150);
    assertThat(committedFile().getModificationTime()).isEqualTo(committed.getModificationTime());

    // Published bytes cannot regress, and the length must match the blocks.
    assertThat(hsync(sessionId, 2 * BLOCK_LENGTH + 100, 100).getOMResponse().getStatus()).isEqualTo(INVALID_REQUEST);
    assertThat(hsync(sessionId, 2 * BLOCK_LENGTH + 150, 160).getOMResponse().getStatus()).isEqualTo(INVALID_REQUEST);
    assertThat(committedFile().getDataSize()).isEqualTo(2 * BLOCK_LENGTH + 150);

    // Only the delta of a later publication is charged.
    assertThat(hsync(sessionId, 2 * BLOCK_LENGTH + 1050, 1000, 50).getOMResponse().getStatus()).isEqualTo(OK);
    assertThat(blockIds(committedFile()))
        .containsExactly(prefixBlockId(0), prefixBlockId(1), suffixBlockId(0), suffixBlockId(1));
    assertThat(bucketUsedBytes()).isEqualTo(usedBytes + 1050);
  }

  @Test
  public void testCloseEndsSessionAndReleasesPrivateBlocks() throws Exception {
    addCommittedFile(2);
    long sessionId = admit();
    // An attribute update after admission must survive the publication.
    OmKeyInfo reserved = committedFile();
    omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(new CacheKey<>(dbFileKey),
        CacheValue.get(++txnId, reserved.toBuilder().addMetadata("updated", "yes").addAcl(ACL).build()));
    allocate(sessionId);
    allocate(sessionId);
    allocate(sessionId);
    long usedBytes = bucketUsedBytes();
    assertThat(hsync(sessionId, 2 * BLOCK_LENGTH + 150, 150).getOMResponse().getStatus()).isEqualTo(OK);

    OMClientResponse response = close(sessionId, 2 * BLOCK_LENGTH + 1030, 1000, 30);

    assertThat(response.getOMResponse().getStatus()).isEqualTo(OK);
    flush(response);
    OmKeyInfo committed = omMetadataManager.getKeyTable(getBucketLayout()).getSkipCache(dbFileKey);
    assertThat(committed.getAppendOwnerSessionId()).isNull();
    assertThat(committed.getAppendSession()).isNull();
    assertThat(blockIds(committed))
        .containsExactly(prefixBlockId(0), prefixBlockId(1), suffixBlockId(0), suffixBlockId(1));
    assertThat(committed.getDataSize()).isEqualTo(2 * BLOCK_LENGTH + 1030);
    assertThat(committed.getMetadata()).containsEntry("updated", "yes");
    assertThat(committed.getAcls()).contains(ACL);
    assertThat(bucketUsedBytes()).isEqualTo(usedBytes + 1030);
    assertThat(omMetadataManager.getOpenKeyTable(getBucketLayout()).getSkipCache(dbOpenKey(sessionId))).isNull();
    assertThat(omMetadataManager.getAppendSessionOpenKey(volumeName, bucketName, sessionId)).isNull();
    // Only the block that was never published is released.
    assertThat(blocksToDelete(response)).containsExactly(suffixBlockId(2));
    assertThat(omMetadataManager.getDeletedTable().isEmpty()).isFalse();

    // The session is gone.
    assertThat(close(sessionId, 2 * BLOCK_LENGTH + 1030, 1000, 30).getOMResponse().getStatus())
        .isEqualTo(KEY_NOT_FOUND);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1})
  public void testEmptyAppendCloseLeavesFileUnchanged(int prefixBlocks) throws Exception {
    OmKeyInfo before = addCommittedFile(prefixBlocks);
    long sessionId = admit();
    allocate(sessionId);
    long usedBytes = bucketUsedBytes();

    OMClientResponse response = close(sessionId, prefixBlocks * BLOCK_LENGTH);

    assertThat(response.getOMResponse().getStatus()).isEqualTo(OK);
    OmKeyInfo committed = committedFile();
    assertThat(committed.getAppendOwnerSessionId()).isNull();
    assertThat(blockIds(committed)).isEqualTo(blockIds(before));
    assertThat(committed.getDataSize()).isEqualTo(before.getDataSize());
    assertThat(committed.getModificationTime()).isEqualTo(before.getModificationTime());
    assertThat(bucketUsedBytes()).isEqualTo(usedBytes);
    assertThat(openRecord(sessionId)).isNull();
    assertThat(blocksToDelete(response)).containsExactly(suffixBlockId(0));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRecoveryCommitKeepsPublishedSuffix(boolean bySessionId) throws Exception {
    addCommittedFile(1);
    long sessionId = admit();
    allocate(sessionId);
    allocate(sessionId);
    assertThat(hsync(sessionId, BLOCK_LENGTH + 150, 150).getOMResponse().getStatus()).isEqualTo(OK);
    setPhase(sessionId, AppendSessionPhase.APPEND_RECOVERING);

    // The fenced writer can no longer publish.
    assertThat(hsync(sessionId, BLOCK_LENGTH + 160, 160).getOMResponse().getStatus())
        .isEqualTo(APPEND_SESSION_NOT_FOUND);
    assertThat(close(sessionId, BLOCK_LENGTH + 160, 160).getOMResponse().getStatus())
        .isEqualTo(APPEND_SESSION_NOT_FOUND);

    // An empty suffix keeps what was published and drops the rest. The existing client sends client ID 0.
    OMClientResponse response = commit(bySessionId ? sessionId : 0, false, true, BLOCK_LENGTH + 150);

    assertThat(response.getOMResponse().getStatus()).isEqualTo(OK);
    OmKeyInfo committed = committedFile();
    assertThat(committed.getAppendOwnerSessionId()).isNull();
    assertThat(blockIds(committed)).containsExactly(prefixBlockId(0), suffixBlockId(0));
    assertThat(committed.getDataSize()).isEqualTo(BLOCK_LENGTH + 150);
    assertThat(openRecord(sessionId)).isNull();
    assertThat(omMetadataManager.getAppendSessionOpenKey(volumeName, bucketName, sessionId)).isNull();
    assertThat(blocksToDelete(response)).containsExactly(suffixBlockId(1));
  }

  @Test
  public void testRecoveryCommitOfActiveSessionNeedsExpiredLease() throws Exception {
    addCommittedFile(1);
    long sessionId = admit();
    allocate(sessionId);

    assertThat(commit(sessionId, false, true, BLOCK_LENGTH + 120, 120).getOMResponse().getStatus())
        .isEqualTo(KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD);
    assertThat(committedFile().getAppendOwnerSessionId()).isEqualTo(sessionId);

    ozoneManager.getConfiguration().set(OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT, "0s");
    assertThat(commit(sessionId, false, true, BLOCK_LENGTH + 120, 120).getOMResponse().getStatus()).isEqualTo(OK);
    OmKeyInfo committed = committedFile();
    assertThat(committed.getAppendOwnerSessionId()).isNull();
    assertThat(blockIds(committed)).containsExactly(prefixBlockId(0), suffixBlockId(0));
    assertThat(committed.getDataSize()).isEqualTo(BLOCK_LENGTH + 120);
  }

  @Test
  public void testCommitOfInvalidatedSession() throws Exception {
    addCommittedFile(1);
    long sessionId = admit();
    allocate(sessionId);
    // What deleting the file does to its append session.
    OmKeyInfo open = openRecord(sessionId);
    omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(new CacheKey<>(dbOpenKey(sessionId)),
        CacheValue.get(++txnId, OmAppendUtil.invalidate(open, committedFile(), txnId)));
    omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(new CacheKey<>(dbFileKey),
        CacheValue.get(++txnId));
    omMetadataManager.removeAppendSession(volumeName, bucketName, sessionId);

    assertThat(close(sessionId, BLOCK_LENGTH + 120, 120).getOMResponse().getStatus())
        .isEqualTo(APPEND_SESSION_NOT_FOUND);
    assertThat(committedFile()).isNull();
  }

  @Test
  public void testOrdinaryCreateRejectedDuringReservation() throws Exception {
    addCommittedFile(1);
    long sessionId = admit();

    OMRequest createFile = OMRequest.newBuilder()
        .setCmdType(Type.CreateFile)
        .setClientId(UUID.randomUUID().toString())
        .setCreateFileRequest(CreateFileRequest.newBuilder()
            .setKeyArgs(keyArgs().setDataSize(100)).setIsOverwrite(true).setIsRecursive(true))
        .build();
    OMFileCreateRequestWithFSO fileCreate = new OMFileCreateRequestWithFSO(createFile, getBucketLayout());
    fileCreate.setUGI(UserGroupInformation.getCurrentUser());
    fileCreate = new OMFileCreateRequestWithFSO(fileCreate.preExecute(ozoneManager), getBucketLayout());
    assertThat(fileCreate.validateAndUpdateCache(ozoneManager, ++txnId).getOMResponse().getStatus())
        .isEqualTo(APPEND_WRITER_CONFLICT);

    OMRequest createKey = OMRequest.newBuilder()
        .setCmdType(Type.CreateKey)
        .setClientId(UUID.randomUUID().toString())
        .setCreateKeyRequest(CreateKeyRequest.newBuilder().setKeyArgs(keyArgs().setDataSize(100)))
        .build();
    OMKeyCreateRequestWithFSO keyCreate = new OMKeyCreateRequestWithFSO(createKey, getBucketLayout());
    keyCreate.setUGI(UserGroupInformation.getCurrentUser());
    keyCreate = new OMKeyCreateRequestWithFSO(keyCreate.preExecute(ozoneManager), getBucketLayout());
    assertThat(keyCreate.validateAndUpdateCache(ozoneManager, ++txnId).getOMResponse().getStatus())
        .isEqualTo(APPEND_WRITER_CONFLICT);

    assertThat(committedFile().getAppendOwnerSessionId()).isEqualTo(sessionId);
  }

  @Test
  public void testCommitRejectsDuplicateBlocksAndHsyncWithRecovery() throws Exception {
    addCommittedFile(1, OzoneConsts.ETAG, "etag");
    long sessionId = admit();
    allocate(sessionId);

    KeyArgs.Builder sameBlockTwice = keyArgs().setDataSize(3 * BLOCK_LENGTH);
    for (int i = 0; i < 2; i++) {
      sameBlockTwice.addKeyLocations(KeyLocation.newBuilder()
          .setBlockID(suffixBlockId(0).getProtobuf()).setOffset(0).setLength(BLOCK_LENGTH));
    }
    assertThat(commit(sessionId, true, false, sameBlockTwice).getOMResponse().getStatus()).isEqualTo(INVALID_REQUEST);
    assertThat(commit(sessionId, true, true, 2 * BLOCK_LENGTH, BLOCK_LENGTH).getOMResponse().getStatus())
        .isEqualTo(INVALID_REQUEST);
    assertThat(blockIds(committedFile())).containsExactly(prefixBlockId(0));
    assertThat(committedFile().getMetadata()).containsKey(OzoneConsts.ETAG);

    // Growth drops the ETag, which described the old contents.
    assertThat(hsync(sessionId, 2 * BLOCK_LENGTH, BLOCK_LENGTH).getOMResponse().getStatus()).isEqualTo(OK);
    assertThat(blockIds(committedFile())).containsExactly(prefixBlockId(0), suffixBlockId(0));
    assertThat(committedFile().getMetadata()).doesNotContainKey(OzoneConsts.ETAG);
  }

  @Test
  public void testOrdinaryCommitRejectedDuringReservation() throws Exception {
    OmKeyInfo before = addCommittedFile(1);
    long sessionId = admit();
    // An ordinary writer of this path, as left behind when the reserved file is renamed onto the path.
    long ordinaryClientId = 11;
    addOrdinaryOpenRecord(ordinaryClientId, true);

    KeyArgs.Builder keyArgs = keyArgs().setDataSize(BLOCK_LENGTH).addKeyLocations(KeyLocation.newBuilder()
        .setBlockID(new BlockID(ORDINARY_CONTAINER_ID, LOCAL_ID).getProtobuf()).setOffset(0).setLength(BLOCK_LENGTH));
    assertThat(commit(ordinaryClientId, false, false, keyArgs).getOMResponse().getStatus())
        .isEqualTo(APPEND_WRITER_CONFLICT);
    // A recovery commit that names a session other than the owner.
    assertThat(commit(sessionId + 1, false, true, BLOCK_LENGTH).getOMResponse().getStatus())
        .isEqualTo(APPEND_SESSION_NOT_FOUND);

    OmKeyInfo committed = committedFile();
    assertThat(committed.getAppendOwnerSessionId()).isEqualTo(sessionId);
    assertThat(blockIds(committed)).isEqualTo(blockIds(before));
  }

  @Test
  public void testRenewAppendLeases() throws Exception {
    OmKeyInfo before = addCommittedFile(1);
    long active = admit();
    long admittedAt = openRecord(active).getAppendSession().getLastRenewedAt();
    // A session of another file in another bucket, and one that lease recovery has fenced.
    String otherBucket = UUID.randomUUID().toString();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, otherBucket, omMetadataManager, getBucketLayout());
    OmKeyInfo otherOpen = openRecord(active).toBuilder().setBucketName(otherBucket).build();
    String dbOtherOpenKey = "/other/open/key/7";
    omMetadataManager.getOpenKeyTable(getBucketLayout()).put(dbOtherOpenKey, otherOpen);
    omMetadataManager.putAppendSession(volumeName, otherBucket, 7, dbOtherOpenKey);
    OmKeyInfo fencedOpen = otherOpen.toBuilder()
        .setAppendSession(otherOpen.getAppendSession().withPhase(AppendSessionPhase.APPEND_RECOVERING)).build();
    String dbFencedOpenKey = "/other/open/key/8";
    omMetadataManager.getOpenKeyTable(getBucketLayout()).put(dbFencedOpenKey, fencedOpen);
    omMetadataManager.putAppendSession(volumeName, otherBucket, 8, dbFencedOpenKey);
    when(ozoneManager.resolveBucketLink(eq(Pair.of(volumeName, "nobucket")), any(OMClientRequest.class)))
        .thenThrow(new OMException("Bucket not found", OMException.ResultCodes.BUCKET_NOT_FOUND));

    OMClientResponse response = renew(admittedAt + 1000, sessionKey(bucketName, active), sessionKey(bucketName, 404),
        sessionKey(otherBucket, 7), sessionKey(otherBucket, 8), sessionKey("nobucket", active));

    assertThat(response.getOMResponse().getStatus()).isEqualTo(OK);
    assertThat(response.getOMResponse().getRenewAppendLeasesResponse().getRenewedList())
        .containsExactly(true, false, true, false, false);
    flush(response);
    Table<String, OmKeyInfo> openKeyTable = omMetadataManager.getOpenKeyTable(getBucketLayout());
    OmAppendSession renewed = openKeyTable.getSkipCache(dbOpenKey(active)).getAppendSession();
    assertThat(renewed.getLastRenewedAt()).isEqualTo(admittedAt + 1000);
    assertThat(renewed.getOpenedAt()).isEqualTo(admittedAt);
    assertThat(openKeyTable.getSkipCache(dbOtherOpenKey).getAppendSession().getLastRenewedAt())
        .isEqualTo(admittedAt + 1000);
    assertThat(openKeyTable.getSkipCache(dbFencedOpenKey).getAppendSession().getLastRenewedAt())
        .isEqualTo(admittedAt);
    // The file itself is not touched.
    assertThat(committedFile().getModificationTime()).isEqualTo(before.getModificationTime());

    // A delayed renewal does not shorten the lease.
    assertThat(renew(admittedAt + 500, sessionKey(bucketName, active)).getOMResponse()
        .getRenewAppendLeasesResponse().getRenewedList()).containsExactly(true);
    assertThat(openRecord(active).getAppendSession().getLastRenewedAt()).isEqualTo(admittedAt + 1000);
  }

  @Test
  public void testSessionRequestsAreAuthorizedAgainstSessionFile() throws Exception {
    addCommittedFile(1);
    long sessionId = admit();
    long admittedAt = openRecord(sessionId).getAppendSession().getLastRenewedAt();
    denyWrite(keyName);
    // The caller names a path that it may write, and the session of a file that it may not write.
    keyName = PARENT_DIR + "/own";

    assertDenied(() -> allocate(sessionId));
    assertDenied(() -> hsync(sessionId, BLOCK_LENGTH));
    assertDenied(() -> close(sessionId, BLOCK_LENGTH));
    assertDenied(() -> commit(sessionId, false, true, BLOCK_LENGTH));
    assertThat(renew(admittedAt + 1000, sessionKey(bucketName, sessionId)).getOMResponse()
        .getRenewAppendLeasesResponse().getRenewedList()).containsExactly(false);

    assertThat(committedFile().getAppendOwnerSessionId()).isEqualTo(sessionId);
    assertThat(openRecord(sessionId).getAppendSession().getLastRenewedAt()).isEqualTo(admittedAt);
    assertThat(openRecord(sessionId).getLatestVersionLocations().getLocationListCount()).isZero();
  }

  @Test
  public void testSessionIsAuthorizedAgainstCurrentPathAfterRename() throws Exception {
    addCommittedFile(1);
    long sessionId = admit();
    long admittedAt = openRecord(sessionId).getAppendSession().getLastRenewedAt();
    rename(keyName, "c/d/g");
    // The open record keeps the path c/d/g when an ancestor directory is renamed.
    rename("c/d", "c/x");

    // The writer still sends the path it opened. Neither that path nor the one in the open record is the file's.
    denyWrite(keyName, keyName + "/" + sessionId, "c/d/g", "c/d/g/" + sessionId);
    assertThat(allocate(sessionId).getOMResponse().getStatus()).isEqualTo(OK);
    assertThat(hsync(sessionId, 2 * BLOCK_LENGTH, BLOCK_LENGTH).getOMResponse().getStatus()).isEqualTo(OK);
    assertThat(renew(admittedAt + 1000, sessionKey(bucketName, sessionId)).getOMResponse()
        .getRenewAppendLeasesResponse().getRenewedList()).containsExactly(true);

    denyWrite("c/x/g");
    assertDenied(() -> allocate(sessionId));
    assertDenied(() -> close(sessionId, 2 * BLOCK_LENGTH, BLOCK_LENGTH));
    assertThat(renew(admittedAt + 2000, sessionKey(bucketName, sessionId)).getOMResponse()
        .getRenewAppendLeasesResponse().getRenewedList()).containsExactly(false);

    denyWrite();
    assertThat(close(sessionId, 2 * BLOCK_LENGTH, BLOCK_LENGTH).getOMResponse().getStatus()).isEqualTo(OK);
  }

  @Test
  public void testOrdinarySessionIsAuthorizedByRequestPath() throws Exception {
    addCommittedFile(1);
    long writerClientId = 4711;
    addOrdinaryOpenRecord(writerClientId, false);

    // Not an append session: the open key of the request path is checked, in the form of the native authorizer.
    denyWrite(keyName);
    assertThat(allocate(writerClientId).getOMResponse().getStatus()).isEqualTo(OK);
    denyWrite(keyName + "/" + writerClientId);
    assertDenied(() -> allocate(writerClientId));
    assertDenied(() -> close(writerClientId, BLOCK_LENGTH));
  }

  @Test
  public void testNativeAuthorizerChecksFileAclsForAppend() throws Exception {
    addCommittedFile(1);
    // As the native authorizer does for a committed file, the authorizer grants WRITE on every key.
    denyWrite();
    when(ozoneManager.isAdmin(any(UserGroupInformation.class))).thenReturn(false);

    assertDenied(this::append);
    setFileAcls(OzoneAcl.parseAcl("user:" + CALLER.getUserName() + ":w"));
    long sessionId = admit();
    long admittedAt = openRecord(sessionId).getAppendSession().getLastRenewedAt();
    assertThat(allocate(sessionId).getOMResponse().getStatus()).isEqualTo(OK);

    // The ACLs of the committed file decide, not the copy that the open record took at admission.
    setFileAcls(ACL);
    assertDenied(() -> allocate(sessionId));
    assertDenied(() -> hsync(sessionId, 2 * BLOCK_LENGTH, BLOCK_LENGTH));
    assertDenied(() -> close(sessionId, 2 * BLOCK_LENGTH, BLOCK_LENGTH));
    assertThat(renew(admittedAt + 1000, sessionKey(bucketName, sessionId)).getOMResponse()
        .getRenewAppendLeasesResponse().getRenewedList()).containsExactly(false);

    when(ozoneManager.getBucketOwner(any(), any(), any(), any())).thenReturn(CALLER.getUserName());
    assertThat(close(sessionId, 2 * BLOCK_LENGTH, BLOCK_LENGTH).getOMResponse().getStatus()).isEqualTo(OK);
  }

  @Test
  public void testMultipartPartCommitCannotConsumeSession() throws Exception {
    addCommittedFile(1);
    long sessionId = admit();
    assertThat(allocate(sessionId).getOMResponse().getStatus()).isEqualTo(OK);
    assertThat(hsync(sessionId, 2 * BLOCK_LENGTH, BLOCK_LENGTH).getOMResponse().getStatus()).isEqualTo(OK);

    // The open record of the session has the DB key of a part's open key with the session ID as client ID. A part
    // commit for a missing upload would move its blocks to the deleted table, one for an upload would take the row.
    OMRequest initiate = OMRequestTestUtils.createInitiateMPURequest(volumeName, bucketName, keyName).toBuilder()
        .setUserInfo(CALLER).build();
    initiate = new S3InitiateMultipartUploadRequestWithFSO(initiate, getBucketLayout()).preExecute(ozoneManager);
    OMResponse initiated = new S3InitiateMultipartUploadRequestWithFSO(initiate, getBucketLayout())
        .validateAndUpdateCache(ozoneManager, ++txnId).getOMResponse();
    assertThat(initiated.getStatus()).isEqualTo(OK);
    for (String uploadId : Arrays.asList("no-such-upload",
        initiated.getInitiateMultiPartUploadResponse().getMultipartUploadID())) {
      OMRequest request = OMRequestTestUtils.createCommitPartMPURequest(volumeName, bucketName, keyName, sessionId,
          BLOCK_LENGTH, uploadId, 1, Collections.emptyList()).toBuilder().setUserInfo(CALLER).build();
      request = new S3MultipartUploadCommitPartRequestWithFSO(request, getBucketLayout()).preExecute(ozoneManager);
      OMClientResponse response = new S3MultipartUploadCommitPartRequestWithFSO(request, getBucketLayout())
          .validateAndUpdateCache(ozoneManager, ++txnId);
      assertThat(response.getOMResponse().getStatus()).isEqualTo(KEY_NOT_FOUND);
      flush(response);
    }

    assertThat(omMetadataManager.countRowsInTable(omMetadataManager.getDeletedTable())).isZero();
    assertThat(omMetadataManager.getAppendSessionOpenKey(volumeName, bucketName, sessionId))
        .isEqualTo(dbOpenKey(sessionId));
    assertThat(blockIds(openRecord(sessionId))).containsExactly(suffixBlockId(0));
    assertThat(close(sessionId, 2 * BLOCK_LENGTH, BLOCK_LENGTH).getOMResponse().getStatus()).isEqualTo(OK);
    assertThat(blockIds(committedFile())).containsExactly(prefixBlockId(0), suffixBlockId(0));
  }

  @Test
  public void testOpenKeyDeletionSkipsLiveSession() throws Exception {
    addCommittedFile(1);
    long sessionId = admit();
    OMRequest request = OMRequest.newBuilder()
        .setCmdType(Type.DeleteOpenKeys)
        .setClientId(UUID.randomUUID().toString())
        .setDeleteOpenKeysRequest(DeleteOpenKeysRequest.newBuilder()
            .setBucketLayout(getBucketLayout().toProto())
            .addOpenKeysPerBucket(OpenKeyBucket.newBuilder().setVolumeName(volumeName).setBucketName(bucketName)
                .addKeys(OpenKey.newBuilder().setName(dbOpenKey(sessionId)))))
        .build();

    for (AppendSessionPhase phase : Arrays.asList(AppendSessionPhase.APPEND_ACTIVE,
        AppendSessionPhase.APPEND_RECOVERING)) {
      setPhase(sessionId, phase);
      assertThat(new OMOpenKeysDeleteRequest(request, getBucketLayout()).validateAndUpdateCache(ozoneManager, ++txnId)
          .getOMResponse().getStatus()).isEqualTo(OK);
      assertThat(openRecord(sessionId)).isNotNull();
    }

    // What open key cleanup is meant to remove: the leftover of a session whose file was deleted.
    setPhase(sessionId, AppendSessionPhase.APPEND_INVALIDATED);
    assertThat(new OMOpenKeysDeleteRequest(request, getBucketLayout()).validateAndUpdateCache(ozoneManager, ++txnId)
        .getOMResponse().getStatus()).isEqualTo(OK);
    assertThat(openRecord(sessionId)).isNull();
  }

  @Test
  public void testNativeAuthorizerChecksFileAclsForRecoveryByPath() throws Exception {
    addCommittedFile(1);
    long sessionId = admit();
    // As the native authorizer does for a committed file, the authorizer grants WRITE on every key.
    denyWrite();
    when(ozoneManager.isAdmin(any(UserGroupInformation.class))).thenReturn(false);

    // Recovery by path names no session, so it is not covered by the check of the session requests.
    assertDenied(() -> recoverLease(true));
    assertDenied(() -> commit(0, false, true, BLOCK_LENGTH));
    assertThat(openRecord(sessionId).getAppendSession().isActive()).isTrue();
    assertThat(committedFile().getAppendOwnerSessionId()).isEqualTo(sessionId);

    setFileAcls(OzoneAcl.parseAcl("user:" + CALLER.getUserName() + ":w"));
    assertThat(recoverLease(true).getOMResponse().getStatus()).isEqualTo(OK);
    assertThat(openRecord(sessionId).getAppendSession().getPhase()).isEqualTo(AppendSessionPhase.APPEND_RECOVERING);
    assertThat(commit(0, false, true, BLOCK_LENGTH).getOMResponse().getStatus()).isEqualTo(OK);
    assertThat(committedFile().getAppendOwnerSessionId()).isNull();
  }

  @Test
  public void testRenewAuthorizesEachSessionOnceInRequestOrder() throws Exception {
    addCommittedFile(1);
    long allowed = admit();
    String allowedFile = keyName;
    keyName = PARENT_DIR + "/g";
    addCommittedFile(1);
    long denied = admit();
    // An authorizer that decides by path only, like Ranger.
    when(ozoneManager.getAccessAuthorizer()).thenReturn(mock(IAccessAuthorizer.class));
    denyWrite(keyName);

    AppendSessionKey allowedKey = sessionKey(bucketName, allowed);
    AppendSessionKey deniedKey = sessionKey(bucketName, denied);
    assertThat(renew(Time.now() + 1000, allowedKey, deniedKey, sessionKey(bucketName, 404), allowedKey, deniedKey)
        .getOMResponse().getRenewAppendLeasesResponse().getRenewedList())
        .containsExactly(true, false, false, true, false);
    OmMetadataReader reader = (OmMetadataReader) ozoneManager.getOmMetadataReader().get();
    for (String file : Arrays.asList(allowedFile, keyName)) {
      verify(reader).checkAcls(eq(OzoneObj.ResourceType.KEY), any(), eq(IAccessAuthorizer.ACLType.WRITE), any(),
          any(), eq(file), any(), any(), any(), anyBoolean(), any());
    }

    assertDenied(() -> allocate(denied));
    keyName = allowedFile;
    assertThat(allocate(allowed).getOMResponse().getStatus()).isEqualTo(OK);
  }

  @Test
  public void testRenewRejectsOversizedBatch() throws Exception {
    AppendSessionKey unknown = sessionKey(bucketName, 404);
    int max = OMAppendLeaseRenewRequest.MAX_SESSIONS_PER_REQUEST;

    assertThat(renew(1, Collections.nCopies(max, unknown).toArray(new AppendSessionKey[0])).getOMResponse()
        .getRenewAppendLeasesResponse().getRenewedList()).hasSize(max).containsOnly(false);
    assertThatThrownBy(() -> renew(1, Collections.nCopies(max + 1, unknown).toArray(new AppendSessionKey[0])))
        .isInstanceOfSatisfying(OMException.class,
            e -> assertThat(e.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST));
  }

  /** Enables ACLs with an authorizer that denies WRITE on exactly the given key paths and grants everything else. */
  private void denyWrite(String... paths) throws Exception {
    deniedPaths.clear();
    deniedPaths.addAll(Arrays.asList(paths));
    when(ozoneManager.getAclsEnabled()).thenReturn(true);
    OmMetadataReader reader = (OmMetadataReader) ozoneManager.getOmMetadataReader().get();
    when(reader.checkAcls(eq(OzoneObj.ResourceType.KEY), any(), eq(IAccessAuthorizer.ACLType.WRITE), any(), any(),
        any(), any(), any(), any(), anyBoolean(), any())).thenAnswer(invocation -> {
          if (deniedPaths.contains(invocation.<String>getArgument(5))) {
            throw new OMException("No WRITE on " + invocation.getArgument(5),
                OMException.ResultCodes.PERMISSION_DENIED);
          }
          return true;
        });
  }

  private static void assertDenied(ThrowableAssert.ThrowingCallable request) {
    assertThatThrownBy(request).isInstanceOfSatisfying(OMException.class,
        e -> assertThat(e.getResult()).isEqualTo(OMException.ResultCodes.PERMISSION_DENIED));
  }

  private void setFileAcls(OzoneAcl... acls) throws Exception {
    omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(new CacheKey<>(dbFileKey),
        CacheValue.get(++txnId, committedFile().toBuilder().setAcls(Arrays.asList(acls)).build()));
  }

  private OMClientResponse recoverLease(boolean force) throws Exception {
    OMRequest request = OMRequest.newBuilder()
        .setCmdType(Type.RecoverLease)
        .setClientId(UUID.randomUUID().toString())
        .setUserInfo(CALLER)
        .setRecoverLeaseRequest(RecoverLeaseRequest.newBuilder()
            .setVolumeName(volumeName).setBucketName(bucketName).setKeyName(keyName).setForce(force))
        .build();
    request = new OMRecoverLeaseRequest(request).preExecute(ozoneManager);
    return new OMRecoverLeaseRequest(request).validateAndUpdateCache(ozoneManager, ++txnId);
  }

  private void rename(String fromKeyName, String toKeyName) throws Exception {
    OMRequest request = OMRequest.newBuilder()
        .setCmdType(Type.RenameKey)
        .setClientId(UUID.randomUUID().toString())
        .setRenameKeyRequest(RenameKeyRequest.newBuilder()
            .setKeyArgs(keyArgs().setKeyName(fromKeyName)).setToKeyName(toKeyName))
        .build();
    request = new OMKeyRenameRequestWithFSO(request, getBucketLayout()).preExecute(ozoneManager);
    assertThat(new OMKeyRenameRequestWithFSO(request, getBucketLayout()).validateAndUpdateCache(ozoneManager, ++txnId)
        .getOMResponse().getStatus()).isEqualTo(OK);
  }

  private static AppendConflictInfo assertConflict(OMClientResponse response, AppendWriterKind kind) {
    OMResponse omResponse = response.getOMResponse();
    assertThat(omResponse.getStatus()).isEqualTo(APPEND_WRITER_CONFLICT);
    assertThat(omResponse.getAppendFileResponse().hasConflict()).isTrue();
    AppendConflictInfo conflict = omResponse.getAppendFileResponse().getConflict();
    assertThat(conflict.getWriterKind()).isEqualTo(kind);
    return conflict;
  }

  private OMClientResponse append() throws Exception {
    OMRequest request = OMRequest.newBuilder()
        .setCmdType(Type.AppendFile)
        .setClientId(UUID.randomUUID().toString())
        .setUserInfo(CALLER)
        .setAppendFileRequest(AppendFileRequest.newBuilder().setKeyArgs(keyArgs()))
        .build();
    request = new OMFileAppendRequest(request).preExecute(ozoneManager);
    return new OMFileAppendRequest(request).validateAndUpdateCache(ozoneManager, ++txnId);
  }

  private AppendSessionKey sessionKey(String bucket, long sessionId) {
    return AppendSessionKey.newBuilder().setVolumeName(volumeName).setBucketName(bucket).setSessionId(sessionId)
        .build();
  }

  /** Applies a renewal that the leader stamped with the given time. */
  private OMClientResponse renew(long renewalTime, AppendSessionKey... sessions) throws Exception {
    OMRequest request = OMRequest.newBuilder()
        .setCmdType(Type.RenewAppendLeases)
        .setClientId(UUID.randomUUID().toString())
        .setUserInfo(CALLER)
        .setRenewAppendLeasesRequest(RenewAppendLeasesRequest.newBuilder().addAllSessions(Arrays.asList(sessions)))
        .build();
    request = new OMAppendLeaseRenewRequest(request).preExecute(ozoneManager);
    assertThat(request.getRenewAppendLeasesRequest().getRenewalTime()).isPositive();
    request = request.toBuilder()
        .setRenewAppendLeasesRequest(request.getRenewAppendLeasesRequest().toBuilder().setRenewalTime(renewalTime))
        .build();
    return new OMAppendLeaseRenewRequest(request).validateAndUpdateCache(ozoneManager, ++txnId);
  }

  private long admit() throws Exception {
    OMResponse omResponse = append().getOMResponse();
    assertThat(omResponse.getStatus()).isEqualTo(OK);
    return omResponse.getAppendFileResponse().getID();
  }

  /** Allocates the next suffix block of the session, see {@link #suffixBlockId(int)}. */
  private OMClientResponse allocate(long sessionId) throws Exception {
    OMRequest request = OMRequest.newBuilder()
        .setCmdType(Type.AllocateBlock)
        .setClientId(UUID.randomUUID().toString())
        .setUserInfo(CALLER)
        .setAllocateBlockRequest(AllocateBlockRequest.newBuilder().setClientID(sessionId).setKeyArgs(keyArgs()))
        .build();
    request = new OMAllocateBlockRequestWithFSO(request, getBucketLayout()).preExecute(ozoneManager);
    // The SCM mock returns the same block for every call.
    AllocateBlockRequest allocateBlockRequest = request.getAllocateBlockRequest();
    request = request.toBuilder()
        .setAllocateBlockRequest(allocateBlockRequest.toBuilder().setKeyLocation(
            allocateBlockRequest.getKeyLocation().toBuilder()
                .setBlockID(suffixBlockId(allocatedSuffixBlocks++).getProtobuf())))
        .build();
    return new OMAllocateBlockRequestWithFSO(request, getBucketLayout()).validateAndUpdateCache(ozoneManager,
        ++txnId);
  }

  private OMClientResponse hsync(long sessionId, long fileLength, long... suffixBlockLengths) throws Exception {
    return commit(sessionId, true, false, fileLength, suffixBlockLengths);
  }

  private OMClientResponse close(long sessionId, long fileLength, long... suffixBlockLengths) throws Exception {
    return commit(sessionId, false, false, fileLength, suffixBlockLengths);
  }

  /** Commits the first suffix blocks of the session with the given lengths. */
  private OMClientResponse commit(long commitClientId, boolean hsync, boolean recovery, long fileLength,
      long... suffixBlockLengths) throws Exception {
    KeyArgs.Builder keyArgs = keyArgs().setDataSize(fileLength);
    for (int i = 0; i < suffixBlockLengths.length; i++) {
      keyArgs.addKeyLocations(KeyLocation.newBuilder()
          .setBlockID(suffixBlockId(i).getProtobuf()).setOffset(0).setLength(suffixBlockLengths[i]));
    }
    return commit(commitClientId, hsync, recovery, keyArgs);
  }

  private OMClientResponse commit(long commitClientId, boolean hsync, boolean recovery, KeyArgs.Builder keyArgs)
      throws Exception {
    OMRequest request = OMRequest.newBuilder()
        .setCmdType(Type.CommitKey)
        .setClientId(UUID.randomUUID().toString())
        .setUserInfo(CALLER)
        .setCommitKeyRequest(CommitKeyRequest.newBuilder()
            .setKeyArgs(keyArgs).setClientID(commitClientId).setHsync(hsync).setRecovery(recovery))
        .build();
    request = new OMKeyCommitRequestWithFSO(request, getBucketLayout()).preExecute(ozoneManager);
    return new OMKeyCommitRequestWithFSO(request, getBucketLayout()).validateAndUpdateCache(ozoneManager, ++txnId);
  }

  private static List<BlockID> blocksToDelete(OMClientResponse response) {
    List<BlockID> ids = new ArrayList<>();
    Map<String, RepeatedOmKeyInfo> keysToDelete = ((OMKeyCommitResponse) response).getKeysToDelete();
    if (keysToDelete != null) {
      keysToDelete.values().forEach(keys -> keys.getOmKeyInfoList().forEach(key -> ids.addAll(blockIds(key))));
    }
    return ids;
  }

  private long bucketUsedBytes() throws Exception {
    return omMetadataManager.getBucketTable().get(omMetadataManager.getBucketKey(volumeName, bucketName))
        .getUsedBytes();
  }

  private static BlockID prefixBlockId(int index) {
    return new BlockID(PREFIX_CONTAINER_ID + index, LOCAL_ID + index);
  }

  private static BlockID suffixBlockId(int index) {
    return new BlockID(SUFFIX_CONTAINER_ID + index, LOCAL_ID);
  }

  private KeyArgs.Builder keyArgs() {
    return KeyArgs.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName).setKeyName(keyName)
        .setType(replicationConfig.getReplicationType())
        .setFactor(((RatisReplicationConfig) replicationConfig).getReplicationFactor());
  }

  private void setPhase(long sessionId, AppendSessionPhase phase) throws Exception {
    OmKeyInfo open = openRecord(sessionId);
    omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(new CacheKey<>(dbOpenKey(sessionId)),
        CacheValue.get(++txnId, open.toBuilder().setAppendSession(open.getAppendSession().withPhase(phase)).build()));
  }

  /** Writes the response to the DB, as the double buffer does. */
  private void flush(OMClientResponse response) throws Exception {
    try (BatchOperation batch = omMetadataManager.getStore().initBatchOperation()) {
      response.checkAndUpdateDB(omMetadataManager, batch);
      omMetadataManager.getStore().commitBatchOperation(batch);
    }
  }

  private OmKeyInfo addCommittedFile(int blockCount, String... metadata) throws Exception {
    OmKeyInfo.Builder builder = newFileInfo(blockLocations(PREFIX_CONTAINER_ID, blockCount));
    for (int i = 0; i < metadata.length; i += 2) {
      builder.addMetadata(metadata[i], metadata[i + 1]);
    }
    OmKeyInfo file = builder.build();
    OMRequestTestUtils.addFileToKeyTable(false, false, file.getFileName(), file, 0, ++txnId, omMetadataManager);
    return file;
  }

  /** Adds the open record of a create or overwrite of the file, to the DB or only to the table cache. */
  private String addOrdinaryOpenRecord(long writerClientId, boolean flushed) throws Exception {
    OmKeyInfo open = newFileInfo(blockLocations(ORDINARY_CONTAINER_ID, 1)).build();
    String dbOpenKey = dbOpenKey(writerClientId);
    if (flushed) {
      omMetadataManager.getOpenKeyTable(getBucketLayout()).put(dbOpenKey, open);
    } else {
      omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(new CacheKey<>(dbOpenKey),
          CacheValue.get(++txnId, open));
    }
    return dbOpenKey;
  }

  private OmKeyInfo.Builder newFileInfo(List<OmKeyLocationInfo> locations) {
    return OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName, replicationConfig,
            new OmKeyLocationInfoGroup(version, locations))
        .setObjectID(parentId + 100)
        .setParentObjectID(parentId)
        .setModificationTime(1)
        .setDataSize(locations.size() * BLOCK_LENGTH);
  }

  private List<OmKeyLocationInfo> blockLocations(long firstContainerId, int count) {
    List<OmKeyLocationInfo> locations = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      locations.add(new OmKeyLocationInfo.Builder()
          .setBlockID(new BlockID(firstContainerId + i, LOCAL_ID + i))
          .setLength(BLOCK_LENGTH)
          .setCreateVersion(version)
          .build());
    }
    return locations;
  }

  private static List<BlockID> blockIds(OmKeyInfo keyInfo) {
    List<BlockID> ids = new ArrayList<>();
    keyInfo.getLatestVersionLocations().createLocationList().forEach(location -> ids.add(location.getBlockID()));
    return ids;
  }

  private String dbOpenKey(long sessionId) throws Exception {
    return omMetadataManager.getOpenFileName(omMetadataManager.getVolumeId(volumeName),
        omMetadataManager.getBucketId(volumeName, bucketName), parentId, "f", sessionId);
  }

  private OmKeyInfo committedFile() throws Exception {
    return omMetadataManager.getKeyTable(getBucketLayout()).get(dbFileKey);
  }

  private OmKeyInfo openRecord(long sessionId) throws Exception {
    return omMetadataManager.getOpenKeyTable(getBucketLayout()).get(dbOpenKey(sessionId));
  }
}
