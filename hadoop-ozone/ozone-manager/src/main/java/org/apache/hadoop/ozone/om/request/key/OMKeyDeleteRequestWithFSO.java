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

import static org.apache.hadoop.ozone.OzoneConsts.DELETED_HSYNC_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DIRECTORY_NOT_EMPTY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMPerformanceMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyDeleteResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles DeleteKey request - prefix layout.
 */
public class OMKeyDeleteRequestWithFSO extends OMKeyDeleteRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyDeleteRequestWithFSO.class);

  public OMKeyDeleteRequestWithFSO(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();
    DeleteKeyRequest deleteKeyRequest = getOmRequest().getDeleteKeyRequest();

    OzoneManagerProtocolProtos.KeyArgs keyArgs =
        deleteKeyRequest.getKeyArgs();
    Map<String, String> auditMap = buildLightKeyArgsAuditMap(keyArgs);

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyDeletes();
    OMPerformanceMetrics perfMetrics = ozoneManager.getPerfMetrics();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    Exception exception = null;
    boolean acquiredLock = false;
    OMClientResponse omClientResponse = null;
    Result result = null;
    long startNanos = Time.monotonicNowNanos();
    try {
      // Serial OM apply keeps path resolution and the subtree-emptiness check stable during preparation.
      // Parallel apply must coordinate the subtree as well as revalidate the path before publishing.
      // Validate bucket and volume exists or not.
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      OzoneFileStatus keyStatus = OMFileRequest.getOMKeyInfoIfExists(
          omMetadataManager, volumeName, bucketName, keyName, 0,
          ozoneManager.getDefaultReplicationConfig());

      if (keyStatus == null) {
        throw new OMException("Key not found. Key:" + keyName, KEY_NOT_FOUND);
      }

      OmKeyInfo omKeyInfo = keyStatus.getKeyInfo();
      validateIfMatchETag(keyArgs, omKeyInfo);
      // New key format for the fileTable & dirTable.
      // For example, the user given key path is '/a/b/c/d/e/file1', then in DB
      // keyName field stores only the leaf node name, which is 'file1'.
      String fileName = OzoneFSUtils.getFileName(keyName);
      omKeyInfo.setKeyName(fileName);

      // Set the UpdateID to current transactionLogIndex
      omKeyInfo = omKeyInfo.toBuilder()
          .setUpdateID(trxnLogIndex)
          .build();

      final long volumeId = omMetadataManager.getVolumeId(volumeName);
      final long bucketId = omMetadataManager.getBucketId(volumeName,
              bucketName);
      String ozonePathKey = omMetadataManager.getOzonePathKey(volumeId,
              bucketId, omKeyInfo.getParentObjectID(),
              omKeyInfo.getFileName());

      if (keyStatus.isDirectory() && !keyArgs.getRecursive()
          && OMFileRequest.hasChildren(omKeyInfo, omMetadataManager)) {
        // Check if there are any sub path exists under the user requested path
        throw new OMException("Directory is not empty. Key:" + keyName,
                DIRECTORY_NOT_EMPTY);
      }

      // If omKeyInfo has hsync metadata, delete its corresponding open key as well. Only the cache
      // entry is published under the lock in the mutation tail; reading and rewriting the copy is done here.
      OmKeyInfo deletedOpenKeyInfo = null;
      String dbOpenKey = null;
      String hsyncClientId = omKeyInfo.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
      if (hsyncClientId != null) {
        long parentId = omKeyInfo.getParentObjectID();
        dbOpenKey = omMetadataManager.getOpenFileName(volumeId, bucketId, parentId, fileName, hsyncClientId);
        OmKeyInfo openKeyInfo = omMetadataManager.getOpenKeyTable(getBucketLayout()).get(dbOpenKey);
        if (openKeyInfo != null) {
          deletedOpenKeyInfo = openKeyInfo.withMetadataMutations(
              metadata -> metadata.put(DELETED_HSYNC_KEY, "true"));
        } else {
          LOG.warn("Potentially inconsistent DB state: open key not found with dbOpenKey '{}'", dbOpenKey);
        }
      }

      if (keyStatus.isFile()) {
        auditMap.put(OzoneConsts.DATA_SIZE, String.valueOf(omKeyInfo.getDataSize()));
        auditMap.put(OzoneConsts.REPLICATION_CONFIG, omKeyInfo.getReplicationConfig().toString());
      }

      long quotaReleased = sumBlockLengths(omKeyInfo);

      // Keep quota updates and all cache mutations under the bucket write lock.
      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();

      // Re-check the key before publishing its deletion.
      final boolean keyStillExists = keyStatus.isDirectory()
          ? omMetadataManager.getDirectoryTable().isExist(ozonePathKey)
          : omMetadataManager.getKeyTable(getBucketLayout()).isExist(ozonePathKey);
      if (!keyStillExists) {
        throw new OMException("Key not found. Key:" + keyName, KEY_NOT_FOUND);
      }

      if (keyStatus.isDirectory()) {
        // Update dir cache.
        omMetadataManager.getDirectoryTable().addCacheEntry(
                new CacheKey<>(ozonePathKey),
                CacheValue.get(trxnLogIndex));
      } else {
        // Update table cache.
        omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
                new CacheKey<>(ozonePathKey),
                CacheValue.get(trxnLogIndex));
      }

      OmBucketInfo omBucketInfo =
          getBucketInfoForUpdate(omMetadataManager, volumeName, bucketName);

      // Empty entries won't be added to deleted table so this key shouldn't get added to snapshotUsed space.
      boolean isKeyNonEmpty = !OmKeyInfo.isKeyEmpty(omKeyInfo);
      omBucketInfo.decrUsedBytes(quotaReleased, isKeyNonEmpty);
      omBucketInfo.decrUsedNamespace(1L, isKeyNonEmpty || keyStatus.isDirectory());

      if (deletedOpenKeyInfo != null) {
        omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
            dbOpenKey, deletedOpenKeyInfo, trxnLogIndex);
      }

      omMetadataManager.getBucketTable().addCacheEntry(
          omMetadataManager.getBucketKey(volumeName, bucketName), omBucketInfo, trxnLogIndex);

      omClientResponse = new OMKeyDeleteResponseWithFSO(omResponse
          .setDeleteKeyResponse(DeleteKeyResponse.newBuilder()).build(),
          keyName, omKeyInfo,
          omBucketInfo.copyObject(), keyStatus.isDirectory(), volumeId,
          deletedOpenKeyInfo);

      result = Result.SUCCESS;
      long endNanosDeleteKeySuccessLatencyNs = Time.monotonicNowNanos();
      perfMetrics.setDeleteKeySuccessLatencyNs(endNanosDeleteKeySuccessLatencyNs - startNanos);
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyDeleteResponseWithFSO(
          createErrorOMResponse(omResponse, exception), getBucketLayout());
      long endNanosDeleteKeyFailureLatencyNs = Time.monotonicNowNanos();
      perfMetrics.setDeleteKeyFailureLatencyNs(endNanosDeleteKeyFailureLatencyNs - startNanos);
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(OMAction.DELETE_KEY, auditMap,
        exception, getOmRequest().getUserInfo()));

    switch (result) {
    case SUCCESS:
      omMetrics.decNumKeys();
      LOG.debug("Key deleted. Volume:{}, Bucket:{}, Key:{}", volumeName,
          bucketName, keyName);
      break;
    case FAILURE:
      omMetrics.incNumKeyDeleteFails();
      LOG.error("Key delete failed. Volume:{}, Bucket:{}, Key:{}.",
          volumeName, bucketName, keyName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyDeleteRequest: {}",
          deleteKeyRequest);
    }

    return omClientResponse;
  }

  @Override
  protected OzoneManagerProtocolProtos.KeyArgs resolveBucketAndCheckAcls(
      OzoneManager ozoneManager,
      OzoneManagerProtocolProtos.KeyArgs.Builder newKeyArgs)
      throws IOException {
    return captureLatencyNs(
        ozoneManager.getPerfMetrics().getDeleteKeyResolveBucketAndAclCheckLatencyNs(),
        () -> resolveBucketAndCheckKeyAclsWithFSO(newKeyArgs.build(),
            ozoneManager, IAccessAuthorizer.ACLType.DELETE));
  }
}
