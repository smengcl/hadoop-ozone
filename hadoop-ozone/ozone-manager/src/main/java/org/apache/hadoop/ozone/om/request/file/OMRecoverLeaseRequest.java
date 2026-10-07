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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.READ;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.WRITE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_ALREADY_CLOSED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.HBASE_SUPPORT;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.RecoverLease;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmAppendSession;
import org.apache.hadoop.ozone.om.helpers.OmFSOFile;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OmAppendUtil;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.file.OMRecoverLeaseResponse;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionPhase;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RecoverLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RecoverLeaseResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform actions for RecoverLease requests.
 */
public class OMRecoverLeaseRequest extends OMKeyRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMRecoverLeaseRequest.class);

  private String volumeName;
  private String bucketName;
  private String keyName;
  private OmKeyInfo openKeyInfo;
  private String dbOpenFileKey;
  private boolean force;

  private OMMetadataManager omMetadataManager;

  public OMRecoverLeaseRequest(OMRequest omRequest, BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
    RecoverLeaseRequest recoverLeaseRequest = getOmRequest()
        .getRecoverLeaseRequest();

    Objects.requireNonNull(recoverLeaseRequest, "recoverLeaseRequest == null");
    volumeName = recoverLeaseRequest.getVolumeName();
    bucketName = recoverLeaseRequest.getBucketName();
    keyName = recoverLeaseRequest.getKeyName();
    force = recoverLeaseRequest.getForce();
  }

  @Override
  @DisallowedUntilLayoutVersion(HBASE_SUPPORT)
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMRequest request = super.preExecute(ozoneManager);
    RecoverLeaseRequest recoverLeaseRequest = request.getRecoverLeaseRequest();

    String keyPath = recoverLeaseRequest.getKeyName();
    String normalizedKeyPath =
        validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(),
            keyPath, getBucketLayout());

    // The file is in the bucket that a link resolves to, as for the append that reserved it.
    KeyArgs resolvedArgs = resolveBucketAndCheckKeyAcls(KeyArgs.newBuilder()
        .setVolumeName(recoverLeaseRequest.getVolumeName())
        .setBucketName(recoverLeaseRequest.getBucketName())
        .setKeyName(recoverLeaseRequest.getKeyName())
        .build(), ozoneManager, IAccessAuthorizer.ACLType.WRITE);
    OmAppendUtil.checkNativeFileAcls(ozoneManager, this, resolvedArgs.getVolumeName(),
        resolvedArgs.getBucketName(), normalizedKeyPath);

    return request.toBuilder()
        .setRecoverLeaseRequest(
            recoverLeaseRequest.toBuilder()
                .setVolumeName(resolvedArgs.getVolumeName())
                .setBucketName(resolvedArgs.getBucketName())
                .setKeyName(normalizedKeyPath))
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    RecoverLeaseRequest recoverLeaseRequest = getOmRequest()
        .getRecoverLeaseRequest();
    Objects.requireNonNull(recoverLeaseRequest, "recoverLeaseRequest == null");

    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, volumeName);
    auditMap.put(OzoneConsts.BUCKET, bucketName);
    auditMap.put(OzoneConsts.KEY, keyName);

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    omMetadataManager = ozoneManager.getMetadataManager();
    OMClientResponse omClientResponse = null;
    Exception exception = null;
    // increment metric
    OMMetrics omMetrics = ozoneManager.getMetrics();

    boolean acquiredLock = false;
    try {
      // acquire lock
      mergeOmLockDetails(
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
              volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      RecoverLeaseResponse recoverLeaseResponse = doWork(ozoneManager, context.getIndex());

      // Prepare response
      omResponse.setRecoverLeaseResponse(recoverLeaseResponse).setCmdType(RecoverLease);
      omClientResponse = new OMRecoverLeaseResponse(omResponse.build(), getBucketLayout(),
          dbOpenFileKey, openKeyInfo);
      omMetrics.incNumRecoverLease();
      LOG.debug("Key recovered. Volume:{}, Bucket:{}, Key:{}",
          volumeName, bucketName, keyName);
    } catch (IOException | InvalidPathException ex) {
      LOG.error("Fail for recovering lease. Volume:{}, Bucket:{}, Key:{}",
          volumeName, bucketName, keyName, ex);
      exception = ex;
      omMetrics.incNumRecoverLeaseFails();
      omClientResponse = new OMRecoverLeaseResponse(
          createErrorOMResponse(omResponse, exception), getBucketLayout());
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(
            omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK,
                volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    // Audit Log outside the lock
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.RECOVER_LEASE, auditMap, exception,
        getOmRequest().getUserInfo()));

    return omClientResponse;
  }

  private RecoverLeaseResponse doWork(OzoneManager ozoneManager,
      long transactionLogIndex) throws IOException {

    String errMsg = "Cannot recover file : " + keyName
        + " as parent directory doesn't exist";

    // The rows of a LEGACY key are addressed by its full key name.
    OmFSOFile fsoFile = !getBucketLayout().isFileSystemOptimized() ? null : new OmFSOFile.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setOmMetadataManager(omMetadataManager)
        .setErrMsg(errMsg)
        .build();

    String dbFileKey = fsoFile != null ? fsoFile.getOzonePathKey()
        : omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);

    OmKeyInfo keyInfo = getKey(dbFileKey);
    if (keyInfo == null) {
      throw new OMException("Key:" + keyName + " not found in keyTable.", KEY_NOT_FOUND);
    }

    final Long appendSessionId = keyInfo.getAppendOwnerSessionId();
    if (appendSessionId != null) {
      startAppendRecovery(ozoneManager, fsoFile != null ? fsoFile.getOpenFileName(appendSessionId)
          : omMetadataManager.getOpenKey(volumeName, bucketName, keyName, appendSessionId), appendSessionId,
          transactionLogIndex);
      return buildResponse(ozoneManager, keyInfo);
    }

    final String writerId = keyInfo.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
    if (writerId == null) {
      // if file is closed, do nothing and return right away.
      throw new OMException("Key: " + keyName + " is already closed", KEY_ALREADY_CLOSED);
    }
    if (fsoFile == null) {
      // ponytail: a LEGACY bucket recovers append sessions only, as it rejected every lease recovery before append.
      // The hsync branch below needs LEGACY open key addressing and its own tests to lift this.
      throw new OMException("Bucket " + bucketName + " is not FSO layout. It does not support lease recovery of an"
          + " hsync'ed key", NOT_SUPPORTED_OPERATION);
    }

    dbOpenFileKey = fsoFile.getOpenFileName(Long.parseLong(writerId));
    openKeyInfo = omMetadataManager.getOpenKeyTable(getBucketLayout()).get(dbOpenFileKey);
    if (openKeyInfo == null) {
      throw new OMException("Open Key " + dbOpenFileKey + " not found in openKeyTable", KEY_NOT_FOUND);
    }

    if (openKeyInfo.getMetadata().containsKey(OzoneConsts.DELETED_HSYNC_KEY)) {
      throw new OMException("Open Key " + keyName + " is already deleted",
          KEY_NOT_FOUND);
    }
    if (openKeyInfo.getMetadata().containsKey(OzoneConsts.LEASE_RECOVERY)) {
      LOG.debug("Key: " + keyName + " is already under recovery");
    } else {
      final long leaseSoftLimit = ozoneManager.getConfiguration()
          .getTimeDuration(OZONE_OM_LEASE_SOFT_LIMIT, OZONE_OM_LEASE_SOFT_LIMIT_DEFAULT, TimeUnit.MILLISECONDS);
      if (!force && Time.now() < openKeyInfo.getModificationTime() + leaseSoftLimit) {
        throw new OMException("Open Key " + keyName + " updated recently and is inside soft limit period",
            KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD);
      }
      openKeyInfo = openKeyInfo.toBuilder()
          .addMetadata(OzoneConsts.LEASE_RECOVERY, "true")
          .setUpdateID(transactionLogIndex)
          .build();
      openKeyInfo.setModificationTime(Time.now());
      // add to cache.
      omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
          dbOpenFileKey, openKeyInfo, transactionLogIndex);
    }
    return buildResponse(ozoneManager, keyInfo);
  }

  /**
   * Fences the append session that owns the file: ACTIVE becomes RECOVERING once the lease is past the soft limit
   * (or with force). A session already RECOVERING is joined. The session stays in the append session index.
   *
   * @param dbOpenKeyOfFile open key table DB key of the session at the location of the file
   */
  private void startAppendRecovery(OzoneManager ozoneManager, String dbOpenKeyOfFile, long sessionId,
      long transactionLogIndex) throws IOException {
    final String indexedOpenKey = omMetadataManager.getAppendSessionOpenKey(volumeName, bucketName, sessionId);
    dbOpenFileKey = indexedOpenKey != null ? indexedOpenKey : dbOpenKeyOfFile;
    openKeyInfo = omMetadataManager.getOpenKeyTable(getBucketLayout()).get(dbOpenFileKey);
    final OmAppendSession session = openKeyInfo == null ? null : openKeyInfo.getAppendSession();
    if (session == null || session.getPhase() == AppendSessionPhase.APPEND_INVALIDATED) {
      throw new OMException("Open Key " + dbOpenFileKey + " of append session " + sessionId
          + " not found in openKeyTable", KEY_NOT_FOUND);
    }
    if (!session.isActive()) {
      LOG.debug("Key: {} is already under recovery", keyName);
      return;
    }
    final long leaseSoftLimit = ozoneManager.getConfiguration()
        .getTimeDuration(OZONE_OM_LEASE_SOFT_LIMIT, OZONE_OM_LEASE_SOFT_LIMIT_DEFAULT, TimeUnit.MILLISECONDS);
    // ponytail: local clock read during apply like the hsync branch, so replicas can decide differently near the
    // limit. Carry a leader supplied time in RecoverLeaseRequest once the proto can change.
    if (!force && Time.now() < session.getLastRenewedAt() + leaseSoftLimit) {
      throw new OMException("Append session " + sessionId + " of " + keyName
          + " renewed recently and is inside soft limit period", KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD);
    }
    // The file's modification time is left alone: fencing a writer publishes no data.
    openKeyInfo = openKeyInfo.toBuilder()
        .setAppendSession(session.withPhase(AppendSessionPhase.APPEND_RECOVERING))
        .setUpdateID(transactionLogIndex)
        .build();
    omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(dbOpenFileKey, openKeyInfo, transactionLogIndex);
  }

  private RecoverLeaseResponse buildResponse(OzoneManager ozoneManager, OmKeyInfo keyInfo) throws IOException {
    // override key name with normalizedKeyPath
    keyInfo.setKeyName(keyName);
    openKeyInfo.setKeyName(keyName);

    OmKeyLocationInfoGroup keyLatestVersionLocations = keyInfo.getLatestVersionLocations();
    List<OmKeyLocationInfo> keyLocationInfoList = keyLatestVersionLocations.createLocationList();
    OmKeyLocationInfoGroup openKeyLatestVersionLocations = openKeyInfo.getLatestVersionLocations();
    // An append session may not have allocated any block yet.
    List<OmKeyLocationInfo> openKeyLocationInfoList = openKeyLatestVersionLocations == null
        ? Collections.emptyList() : openKeyLatestVersionLocations.createLocationList();

    // An append session owns only the blocks after its prefix. Never issue a write token for a prefix block.
    final OmAppendSession appendSession = openKeyInfo.getAppendSession();
    if (!keyLocationInfoList.isEmpty()
        && (appendSession == null || keyLocationInfoList.size() > appendSession.getPrefixBlockCount())) {
      updateBlockInfo(ozoneManager, keyLocationInfoList.get(keyLocationInfoList.size() - 1));
    }
    if (openKeyLocationInfoList.size() > 1) {
      updateBlockInfo(ozoneManager, openKeyLocationInfoList.get(openKeyLocationInfoList.size() - 1));
      updateBlockInfo(ozoneManager, openKeyLocationInfoList.get(openKeyLocationInfoList.size() - 2));
    } else if (!openKeyLocationInfoList.isEmpty()) {
      updateBlockInfo(ozoneManager, openKeyLocationInfoList.get(0));
    }

    RecoverLeaseResponse.Builder rb = RecoverLeaseResponse.newBuilder();
    rb.setKeyInfo(keyInfo.getNetworkProtobuf(getOmRequest().getVersion(), true));
    rb.setOpenKeyInfo(openKeyInfo.getNetworkProtobuf(getOmRequest().getVersion(), true));
    return rb.build();
  }

  private void updateBlockInfo(OzoneManager ozoneManager, OmKeyLocationInfo blockInfo) throws IOException {
    if (blockInfo != null) {
      // set token to last block if enabled
      if (ozoneManager.isGrpcBlockTokenEnabled()) {
        String remoteUser = getRemoteUser().getShortUserName();
        OzoneBlockTokenSecretManager secretManager = ozoneManager.getBlockTokenSecretManager();
        blockInfo.setToken(secretManager.generateToken(remoteUser, blockInfo.getBlockID(),
            EnumSet.of(READ, WRITE), blockInfo.getLength()));
      }
      // refresh last block pipeline
      ContainerWithPipeline containerWithPipeline =
          ozoneManager.getScmClient().getContainerClient().getContainerWithPipeline(blockInfo.getContainerID());
      blockInfo.setPipeline(containerWithPipeline.getPipeline());
    }
  }

  private OmKeyInfo getKey(String dbOzoneKey) throws IOException {
    return omMetadataManager.getKeyTable(getBucketLayout()).get(dbOzoneKey);
  }
}
