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
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.APPEND_WRITER_CONFLICT;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.NOT_A_FILE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmAppendSession;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequestTests;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendConflictInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendFileResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionPhase;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendWriterKind;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests append sessions in OM: admission by {@link OMFileAppendRequest} and the session's later requests.
 */
public class TestOMFileAppendRequest extends OMKeyRequestTests {

  private static final String PARENT_DIR = "c/d/e";
  private static final long BLOCK_LENGTH = 200;

  private long parentId;
  private String dbFileKey;
  private long txnId = 1000;

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
        .setAppendFileRequest(AppendFileRequest.newBuilder().setKeyArgs(KeyArgs.newBuilder()
            .setVolumeName(volumeName).setBucketName(bucketName).setKeyName(keyName)))
        .build();
    request = new OMFileAppendRequest(request).preExecute(ozoneManager);
    return new OMFileAppendRequest(request).validateAndUpdateCache(ozoneManager, ++txnId);
  }

  /** Writes the response to the DB, as the double buffer does. */
  private void flush(OMClientResponse response) throws Exception {
    try (BatchOperation batch = omMetadataManager.getStore().initBatchOperation()) {
      response.checkAndUpdateDB(omMetadataManager, batch);
      omMetadataManager.getStore().commitBatchOperation(batch);
    }
  }

  private OmKeyInfo addCommittedFile(int blockCount, String... metadata) throws Exception {
    OmKeyInfo.Builder builder = newFileInfo(blockLocations(CONTAINER_ID, blockCount));
    for (int i = 0; i < metadata.length; i += 2) {
      builder.addMetadata(metadata[i], metadata[i + 1]);
    }
    OmKeyInfo file = builder.build();
    OMRequestTestUtils.addFileToKeyTable(false, false, file.getFileName(), file, 0, ++txnId, omMetadataManager);
    return file;
  }

  /** Adds the open record of a create or overwrite of the file, to the DB or only to the table cache. */
  private String addOrdinaryOpenRecord(long writerClientId, boolean flushed) throws Exception {
    OmKeyInfo open = newFileInfo(blockLocations(2 * CONTAINER_ID, 1)).build();
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
