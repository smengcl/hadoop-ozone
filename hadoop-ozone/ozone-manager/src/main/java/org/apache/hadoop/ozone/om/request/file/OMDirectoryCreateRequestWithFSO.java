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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.DIRECTORY_EXISTS_IN_GIVENPATH;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS_IN_GIVENPATH;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.NONE;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.file.OMDirectoryCreateResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDirectoryRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDirectoryResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle create directory request. It will add path components to the directory
 * table and maintains file system semantics.
 */
public class OMDirectoryCreateRequestWithFSO extends OMDirectoryCreateRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMDirectoryCreateRequestWithFSO.class);

  public OMDirectoryCreateRequestWithFSO(OMRequest omRequest,
                                         BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    CreateDirectoryRequest createDirectoryRequest = getOmRequest()
        .getCreateDirectoryRequest();
    KeyArgs keyArgs = createDirectoryRequest.getKeyArgs();

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();
    int numKeysCreated = 0;

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    omResponse.setCreateDirectoryResponse(CreateDirectoryResponse.newBuilder());
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumCreateDirectory();

    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean acquiredLock = false;
    Exception exception = null;
    OMClientResponse omClientResponse = null;
    Result result = Result.FAILURE;

    try {
      // Check if this is the root of the filesystem.
      if (keyName.isEmpty()) {
        throw new OMException("Directory create failed. Cannot create " +
            "directory at root of the filesystem",
            OMException.ResultCodes.CANNOT_CREATE_DIRECTORY_AT_ROOT);
      }

      // Lock-free preparation relies on serial OM apply; parallel apply must revalidate the full path.
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      Path keyPath = Paths.get(keyName);

      // Need to check if any files exist in the given path, if they exist we
      // cannot create a directory with the given key.
      // Verify the path against directory table
      OMFileRequest.OMPathInfoWithFSO omPathInfo =
          OMFileRequest.verifyDirectoryKeysInPath(omMetadataManager, volumeName,
              bucketName, keyName, keyPath);
      OMFileRequest.OMDirectoryResult omDirectoryResult =
          omPathInfo.getDirectoryResult();

      if (omDirectoryResult == FILE_EXISTS ||
          omDirectoryResult == FILE_EXISTS_IN_GIVENPATH) {
        throw new OMException("Unable to create directory: " + keyName
            + " in volume/bucket: " + volumeName + "/" + bucketName + " as " +
                "file:" + omPathInfo.getFileExistsInPath() + " already exists",
            FILE_ALREADY_EXISTS);
      }

      if (omDirectoryResult == DIRECTORY_EXISTS_IN_GIVENPATH || omDirectoryResult == NONE) {
        final long volumeId = omMetadataManager.getVolumeId(volumeName);
        final long bucketId = omMetadataManager
                .getBucketId(volumeName, bucketName);

        // Prepare with a read-only bucket copy; quota updates take a separate copy under the lock.
        OmBucketInfo bucketInfo = omMetadataManager.getBucketTable()
            .get(omMetadataManager.getBucketKey(volumeName, bucketName));
        // prepare all missing parents
        List<OmDirectoryInfo> missingParentInfos = getAllMissingParentDirInfo(
                ozoneManager, keyArgs, bucketInfo, omPathInfo, trxnLogIndex);

        // Missing-parent preparation sets the leaf and parent IDs; ACL validation must precede publication.
        OmDirectoryInfo dirInfo = createDirectoryInfoWithACL(
            omPathInfo.getLeafNodeName(),
            keyArgs, omPathInfo.getLeafNodeObjectId(),
            omPathInfo.getLastKnownParentId(), trxnLogIndex,
            bucketInfo, omPathInfo, ozoneManager.getConfig());

        numKeysCreated = missingParentInfos.size() + 1;

        // Keep quota updates and all cache mutations under the bucket write lock.
        mergeOmLockDetails(
            omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName,
                bucketName));
        acquiredLock = getOmLockDetails().isLockAcquired();

        // Re-check the leaf before publishing.
        final String dbLeafKey = omMetadataManager.getOzonePathKey(volumeId,
            bucketId, omPathInfo.getLastKnownParentId(),
            omPathInfo.getLeafNodeName());
        if (omMetadataManager.getDirectoryTable().isExist(dbLeafKey)) {
          throw new OMException("Unable to create directory: " + keyName
              + " in volume/bucket: " + volumeName + "/" + bucketName + " as it already exists",
              OMException.ResultCodes.DIRECTORY_ALREADY_EXISTS);
        }
        if (omMetadataManager.getKeyTable(getBucketLayout()).isExist(dbLeafKey)) {
          throw new OMException("Unable to create directory: " + keyName
              + " in volume/bucket: " + volumeName + "/" + bucketName
              + " as a file already exists at that path", FILE_ALREADY_EXISTS);
        }

        OmBucketInfo omBucketInfo =
            getBucketInfoForUpdate(omMetadataManager, volumeName, bucketName);
        checkBucketQuotaInNamespace(omBucketInfo, numKeysCreated);
        omBucketInfo.incrUsedNamespace(numKeysCreated);

        OMFileRequest.addDirectoryTableCacheEntries(omMetadataManager,
            volumeId, bucketId, trxnLogIndex,
            missingParentInfos, dirInfo);

        omMetadataManager.getBucketTable().addCacheEntry(
            omMetadataManager.getBucketKey(volumeName, bucketName), omBucketInfo, trxnLogIndex);

        omClientResponse = new OMDirectoryCreateResponseWithFSO(omResponse.build(),
            volumeId, bucketId, dirInfo,
            missingParentInfos, getBucketLayout(), omBucketInfo.copyObject());
        result = Result.SUCCESS;
      } else {
        result = Result.DIRECTORY_ALREADY_EXISTS;
        omResponse.setStatus(Status.DIRECTORY_ALREADY_EXISTS);
        omClientResponse = new OMDirectoryCreateResponseWithFSO(omResponse.build());
      }
    } catch (IOException | InvalidPathException ex) {
      exception = ex;
      omClientResponse = new OMDirectoryCreateResponseWithFSO(
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(OMAction.CREATE_DIRECTORY,
        auditMap, exception, getOmRequest().getUserInfo()));

    switch (result) {
    case SUCCESS:
      omMetrics.incNumKeys(numKeysCreated);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Directory created. Volume:{}, Bucket:{}, Key:{}",
            volumeName, bucketName, keyName);
      }
      break;
    case DIRECTORY_ALREADY_EXISTS:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Directory already exists. Volume:{}, Bucket:{}, Key:{}",
            volumeName, bucketName, keyName, exception);
      }
      break;
    case FAILURE:
      omMetrics.incNumCreateDirectoryFails();
      LOG.error("Directory creation failed. Volume:{}, Bucket:{}, Key:{}. " +
          "Exception:{}", volumeName, bucketName, keyName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMDirectoryCreateRequest: {}",
          createDirectoryRequest);
    }

    return omClientResponse;
  }
}
