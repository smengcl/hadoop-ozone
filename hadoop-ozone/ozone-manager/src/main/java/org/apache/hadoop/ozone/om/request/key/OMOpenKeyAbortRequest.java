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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.ACCESS_DENIED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.APPEND;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmFSOFile;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.util.OMMultipartUploadUtils;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMOpenKeysDeleteResponse;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AbortOpenKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AbortOpenKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Handles an administrator's request to abort one ordinary open key whose writer is confirmed dead. The open key,
 * identified by its path and client ID, is removed from the open key table and its blocks are moved to the deleted
 * table, the same way {@link OMOpenKeysDeleteRequest} handles an expired open key. Append sessions, hsync'ed
 * writers and multipart upload open keys are rejected.
 */
public class OMOpenKeyAbortRequest extends OMKeyRequest {

  public OMOpenKeyAbortRequest(OMRequest omRequest, BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  @DisallowedUntilLayoutVersion(APPEND)
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMRequest request = super.preExecute(ozoneManager);
    AbortOpenKeyRequest abortRequest = request.getAbortOpenKeyRequest();
    try {
      if (ozoneManager.isAdminAuthorizationEnabled()) {
        UserGroupInformation ugi = createUGIForApi();
        if (!ozoneManager.isAdmin(ugi)) {
          throw new OMException("Access denied for user " + ugi
              + ". Admin privilege is required to abort an open key.", ACCESS_DENIED);
        }
      }
      ResolvedBucket bucket = ozoneManager.resolveBucketLink(
          Pair.of(abortRequest.getVolumeName(), abortRequest.getBucketName()), this);
      String keyName = validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(), abortRequest.getKeyName(),
          getBucketLayout());

      return request.toBuilder()
          .setAbortOpenKeyRequest(abortRequest.toBuilder()
              .setVolumeName(bucket.realVolume())
              .setBucketName(bucket.realBucket())
              .setKeyName(keyName))
          .build();
    } catch (IOException ex) {
      // A request rejected here never reaches validateAndUpdateCache, so audit it now.
      markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(OMAction.ABORT_OPEN_KEY,
          buildAuditMap(abortRequest), ex, request.getUserInfo()));
      throw ex;
    }
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();
    AbortOpenKeyRequest abortRequest = getOmRequest().getAbortOpenKeyRequest();
    String volumeName = abortRequest.getVolumeName();
    String bucketName = abortRequest.getBucketName();
    String keyName = abortRequest.getKeyName();
    long clientID = abortRequest.getClientID();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OMClientResponse omClientResponse;
    Exception exception = null;
    boolean acquiredLock = false;
    try {
      mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      final String dbOpenKey;
      final String dbKey;
      if (getBucketLayout().isFileSystemOptimized()) {
        OmFSOFile fsoFile = new OmFSOFile.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setOmMetadataManager(omMetadataManager)
            .build();
        dbOpenKey = fsoFile.getOpenFileName(clientID);
        dbKey = fsoFile.getOzonePathKey();
      } else {
        dbOpenKey = omMetadataManager.getOpenKey(volumeName, bucketName, keyName, clientID);
        dbKey = omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
      }

      Table<String, OmKeyInfo> openKeyTable = omMetadataManager.getOpenKeyTable(getBucketLayout());
      OmKeyInfo openKeyInfo = openKeyTable.get(dbOpenKey);
      checkAbortable(openKeyInfo, omMetadataManager.getKeyTable(getBucketLayout()).get(dbKey), keyName, clientID);

      // Same as OMOpenKeysDeleteRequest: tombstone the open key in the cache, the response deletes the row and moves
      // the blocks to the deleted table. Open keys hold no bucket quota, so there is none to release.
      openKeyInfo = openKeyInfo.toBuilder().setUpdateID(trxnLogIndex).build();
      openKeyTable.addCacheEntry(new CacheKey<>(dbOpenKey), CacheValue.get(trxnLogIndex));
      long bucketId = getBucketInfo(omMetadataManager, volumeName, bucketName).getObjectID();

      omClientResponse = new OMOpenKeysDeleteResponse(
          omResponse.setAbortOpenKeyResponse(AbortOpenKeyResponse.newBuilder()).build(),
          Collections.singletonMap(dbOpenKey, Pair.of(bucketId, openKeyInfo)), getBucketLayout());
    } catch (IOException | InvalidPathException ex) {
      exception = ex;
      omClientResponse = new OMOpenKeysDeleteResponse(createErrorOMResponse(omResponse, exception),
          getBucketLayout());
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
    }
    omClientResponse.setOmLockDetails(getOmLockDetails());

    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(OMAction.ABORT_OPEN_KEY,
        buildAuditMap(abortRequest), exception, getOmRequest().getUserInfo()));
    return omClientResponse;
  }

  private static Map<String, String> buildAuditMap(AbortOpenKeyRequest abortRequest) {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, abortRequest.getVolumeName());
    auditMap.put(OzoneConsts.BUCKET, abortRequest.getBucketName());
    auditMap.put(OzoneConsts.KEY, abortRequest.getKeyName());
    auditMap.put(OzoneConsts.CLIENT_ID, String.valueOf(abortRequest.getClientID()));
    return auditMap;
  }

  /**
   * Only an ordinary open key can be aborted. A missing open key means the writer has committed or the key has
   * already been cleaned up, so there is nothing to abort.
   */
  private static void checkAbortable(OmKeyInfo openKeyInfo, OmKeyInfo committedKeyInfo, String keyName,
      long clientID) throws OMException {
    final String openKeyDesc = "Open key " + keyName + " with client ID " + clientID;
    if (openKeyInfo == null) {
      throw new OMException(openKeyDesc + " not found. It may have been committed or cleaned up already.",
          KEY_NOT_FOUND);
    }
    if (openKeyInfo.getAppendSession() != null) {
      throw new OMException(openKeyDesc + " is an append session, which cannot be aborted."
          + " An abandoned append session is closed by lease recovery.", NOT_SUPPORTED_OPERATION);
    }
    final String clientIdString = String.valueOf(clientID);
    if (clientIdString.equals(openKeyInfo.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID)) || (committedKeyInfo != null
        && clientIdString.equals(committedKeyInfo.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID)))) {
      throw new OMException(openKeyDesc + " belongs to an hsync'ed writer, which cannot be aborted."
          + " Recover its data with 'ozone admin om lease recover' instead.", NOT_SUPPORTED_OPERATION);
    }
    if (openKeyInfo.getKeyLocationVersions().size() > 1) {
      // The open record of an overwrite in a versioning-enabled bucket also lists the blocks of the committed versions.
      throw new OMException(openKeyDesc + " carries older versions of the key, which an abort would delete.",
          NOT_SUPPORTED_OPERATION);
    }
    if (OMMultipartUploadUtils.isMultipartKeySet(openKeyInfo)) {
      throw new OMException(openKeyDesc + " belongs to a multipart upload. Abort the multipart upload instead.",
          NOT_SUPPORTED_OPERATION);
    }
  }
}
