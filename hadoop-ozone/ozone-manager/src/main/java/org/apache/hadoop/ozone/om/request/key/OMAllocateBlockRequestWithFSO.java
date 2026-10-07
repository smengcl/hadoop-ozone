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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.APPEND_SESSION_NOT_FOUND;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmFSOFile;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmAppendUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMAllocateBlockResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Handles allocate block request - prefix layout.
 */
public class OMAllocateBlockRequestWithFSO extends OMAllocateBlockRequest {

  public OMAllocateBlockRequestWithFSO(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  protected OmKeyInfo getOpenKeyInfo(OMMetadataManager omMetadataManager,
      String openKeyName, String keyName) throws IOException {
    OmKeyInfo openKeyInfo = omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openKeyName);
    if (openKeyInfo == null || openKeyInfo.getAppendSession() == null) {
      // Ordinary session, as OMFileRequest.getOmKeyInfoFromFileTable does it.
      if (openKeyInfo != null) {
        openKeyInfo.setKeyName(OzoneFSUtils.getFileName(keyName));
      }
      return openKeyInfo;
    }
    // Only the ACTIVE session that owns the file may allocate, also when the open record was found by path.
    long sessionId = getOmRequest().getAllocateBlockRequest().getClientID();
    if (!openKeyName.equals(omMetadataManager.getAppendSessionOpenKey(openKeyInfo.getVolumeName(),
        openKeyInfo.getBucketName(), sessionId))
        || !openKeyInfo.getAppendSession().isActive()
        || !OmAppendUtil.isOwnedBy(omMetadataManager.getKeyTable(getBucketLayout())
            .get(OmAppendUtil.getDbFileKey(omMetadataManager, openKeyInfo)), sessionId)) {
      throw new OMException("Append session " + sessionId + " of " + openKeyInfo.getKeyName()
          + " is not active", APPEND_SESSION_NOT_FOUND);
    }
    return openKeyInfo;
  }

  @Override
  protected String getOpenKeyName(String volumeName, String bucketName,
      String keyName, long clientID, OMMetadataManager omMetadataManager)
          throws IOException {
    // The open record of an append session follows renames, so it is found by session ID and not by path.
    String appendOpenKey = omMetadataManager.getAppendSessionOpenKey(volumeName, bucketName, clientID);
    if (appendOpenKey != null) {
      return appendOpenKey;
    }
    return new OmFSOFile.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName(keyName)
          .setOmMetadataManager(omMetadataManager)
          .build().getOpenFileName(clientID);
  }

  @Override
  protected void addOpenTableCacheEntry(long trxnLogIndex,
      OMMetadataManager omMetadataManager, String openKeyName, String keyName,
      OmKeyInfo openKeyInfo) {
    // The request of an append session may still carry the path the file had before a rename.
    OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager, openKeyName,
        openKeyInfo, openKeyInfo.getAppendSession() != null ? openKeyInfo.getKeyName() : keyName, trxnLogIndex);
  }

  @Override
  @Nonnull
  protected OMClientResponse getOmClientResponse(long clientID,
      OMResponse.Builder omResponse, OmKeyInfo openKeyInfo,
      OmBucketInfo omBucketInfo, OMMetadataManager omMetadataManager)
          throws IOException {
    long volumeId = omMetadataManager.getVolumeId(openKeyInfo.getVolumeName());
    return new OMAllocateBlockResponseWithFSO(omResponse.build(), openKeyInfo,
            clientID, getBucketLayout(), volumeId, omBucketInfo.getObjectID());
  }

  @Override
  @Nonnull
  protected OMClientResponse getOmClientErrorResponse(
      OMResponse.Builder omResponse, Exception exception) {
    return new OMAllocateBlockResponseWithFSO(
        createErrorOMResponse(omResponse, exception), getBucketLayout());
  }
}
