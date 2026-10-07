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

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.KEY_ALREADY_CLOSED;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.PARTIAL_RENAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysMap;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;

/**
 * Runs the append session tests in a LEGACY bucket, whose rows are addressed by the full key name.
 */
public class TestOMFileAppendRequestLegacy extends TestOMFileAppendRequest {

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.LEGACY;
  }

  @Test
  public void testOpenKeyUnderFileNameIsNotAWriter() throws Exception {
    addCommittedFile(1);
    // Without file system paths a key name may continue the name of the file like a path. Its open keys start with
    // the open key prefix of the file.
    String childKeyName = keyName + "/child";
    OmKeyInfo child = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, childKeyName, replicationConfig)
        .build();
    Table<String, OmKeyInfo> openKeyTable = omMetadataManager.getOpenKeyTable(getBucketLayout());
    openKeyTable.put(omMetadataManager.getOpenKey(volumeName, bucketName, childKeyName, 11), child);
    openKeyTable.addCacheEntry(new CacheKey<>(omMetadataManager.getOpenKey(volumeName, bucketName, childKeyName, 12)),
        CacheValue.get(1L, child));

    assertThat(admit()).isNotZero();
  }

  @Test
  public void testAppendWithFileSystemPathsEnabled() throws Exception {
    when(ozoneManager.getEnableFileSystemPaths()).thenReturn(true);
    addCommittedFile(1);

    long sessionId = admit();

    assertThat(allocate(sessionId).getOMResponse().getStatus()).isEqualTo(OK);
    assertThat(hsync(sessionId, 2 * BLOCK_LENGTH, BLOCK_LENGTH).getOMResponse().getStatus()).isEqualTo(OK);
    assertThat(close(sessionId, 2 * BLOCK_LENGTH, BLOCK_LENGTH).getOMResponse().getStatus()).isEqualTo(OK);
    assertThat(blockIds(committedFile())).containsExactly(prefixBlockId(0), suffixBlockId(0));
    assertThat(committedFile().getAppendOwnerSessionId()).isNull();
    assertThat(openRecord(sessionId)).isNull();
  }

  /** Recovery by path ends the session like a commit that names it, without the checks of a new file. */
  @Test
  public void testRecoveryCommitByPathWithFileSystemPathsEnabled() throws Exception {
    when(ozoneManager.getEnableFileSystemPaths()).thenReturn(true);
    addCommittedFile(1);
    long sessionId = admit();
    // A key that was written before file system paths were enabled may have no directory key above it.
    omMetadataManager.getKeyTable(getBucketLayout()).delete(
        omMetadataManager.getOzoneDirKey(volumeName, bucketName, OzoneFSUtils.getParent(keyName)));
    assertThat(execute(recoverLease(true)).getOMResponse().getStatus()).isEqualTo(OK);

    assertThat(commit(0, false, true, BLOCK_LENGTH).getOMResponse().getStatus()).isEqualTo(OK);

    assertThat(committedFile().getAppendOwnerSessionId()).isNull();
    assertThat(openRecord(sessionId)).isNull();
  }

  /** A LEGACY bucket recovers the lease of append sessions only. */
  @Test
  public void testRecoverLeaseWithoutAppendSession() throws Exception {
    addCommittedFile(1);
    assertThat(execute(recoverLease(true)).getOMResponse().getStatus()).isEqualTo(KEY_ALREADY_CLOSED);

    addCommittedFile(1, OzoneConsts.HSYNC_CLIENT_ID, "11");
    assertThat(execute(recoverLease(true)).getOMResponse().getStatus()).isEqualTo(NOT_SUPPORTED_OPERATION);
    assertThat(committedFile().getMetadata()).containsEntry(OzoneConsts.HSYNC_CLIENT_ID, "11");
  }

  /** The batch rename does not move append sessions, so it must leave a reserved key alone. */
  @Test
  public void testBatchRenameSkipsReservedKey() throws Exception {
    OmKeyInfo before = addCommittedFile(1);
    long sessionId = admit();
    String otherKeyName = keyName + "-other";
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName, otherKeyName, 0, replicationConfig,
        omMetadataManager);

    // Neither away from its name nor replaced by another key.
    OMClientResponse response = execute(OMRequest.newBuilder()
        .setCmdType(Type.RenameKeys)
        .setClientId(UUID.randomUUID().toString())
        .setRenameKeysRequest(RenameKeysRequest.newBuilder().setRenameKeysArgs(RenameKeysArgs.newBuilder()
            .setVolumeName(volumeName).setBucketName(bucketName)
            .addRenameKeysMap(RenameKeysMap.newBuilder().setFromKeyName(keyName).setToKeyName(keyName + "-renamed"))
            .addRenameKeysMap(RenameKeysMap.newBuilder().setFromKeyName(otherKeyName).setToKeyName(keyName))))
        .build());

    assertThat(response.getOMResponse().getStatus()).isEqualTo(PARTIAL_RENAME);
    assertThat(response.getOMResponse().getRenameKeysResponse().getUnRenamedKeysList())
        .extracting(RenameKeysMap::getFromKeyName).containsExactly(keyName, otherKeyName);
    assertThat(committedFile().getAppendOwnerSessionId()).isEqualTo(sessionId);
    assertThat(blockIds(committedFile())).isEqualTo(blockIds(before));
    assertThat(openRecord(sessionId).getAppendSession().isActive()).isTrue();
    Table<String, OmKeyInfo> keyTable = omMetadataManager.getKeyTable(getBucketLayout());
    assertThat(keyTable.get(omMetadataManager.getOzoneKey(volumeName, bucketName, keyName + "-renamed"))).isNull();
    assertThat(keyTable.get(omMetadataManager.getOzoneKey(volumeName, bucketName, otherKeyName))).isNotNull();
  }
}
