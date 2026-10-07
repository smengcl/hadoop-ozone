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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.APPEND_SESSION_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_ALREADY_CLOSED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_UNDER_LEASE_RECOVERY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmAppendSession;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmFSOFile;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithMetadata;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmAppendUtil;
import org.apache.hadoop.ozone.om.request.util.OmKeyHSyncUtil;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCommitResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionPhase;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles CommitKey request - prefix layout.
 */
public class OMKeyCommitRequestWithFSO extends OMKeyCommitRequest {

  @VisibleForTesting
  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyCommitRequestWithFSO.class);

  public OMKeyCommitRequestWithFSO(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    CommitKeyRequest commitKeyRequest = getOmRequest().getCommitKeyRequest();

    KeyArgs commitKeyArgs = commitKeyRequest.getKeyArgs();

    String volumeName = commitKeyArgs.getVolumeName();
    String bucketName = commitKeyArgs.getBucketName();
    String keyName = commitKeyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    Map<String, String> auditMap = buildKeyArgsAuditMap(commitKeyArgs);

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
            getOmRequest());

    Exception exception = null;
    OmKeyInfo omKeyInfo = null;
    OmBucketInfo omBucketInfo;
    OMClientResponse omClientResponse = null;
    boolean bucketLockAcquired = false;
    Result result;
    boolean isHSync = commitKeyRequest.hasHsync() && commitKeyRequest.getHsync();
    boolean isRecovery = commitKeyRequest.hasRecovery() && commitKeyRequest.getRecovery();
    // isHsync = true, a commit request as a result of client side hsync call
    // isRecovery = true, a commit request as a result of client side recoverLease call
    // none of isHsync and isRecovery is true, a commit request as a result of client side normal
    // outputStream#close call.
    if (isHSync) {
      omMetrics.incNumKeyHSyncs();
    } else {
      omMetrics.incNumKeyCommits();
    }

    LOG.debug("isHSync = {}, isRecovery = {}, volumeName = {}, bucketName = {}, keyName = {}",
        isHSync, isRecovery, volumeName, bucketName, keyName);

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    try {
      String dbOpenFileKey = null;

      List<OmKeyLocationInfo>
          locationInfoList = getOmKeyLocationInfos(ozoneManager, commitKeyArgs);

      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      bucketLockAcquired = getOmLockDetails().isLockAcquired();

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);
      omBucketInfo = getBucketInfoForUpdate(omMetadataManager, volumeName, bucketName);

      // An append session is found through the session index, which follows renames, and not through the path.
      String dbAppendOpenKey =
          omMetadataManager.getAppendSessionOpenKey(volumeName, bucketName, commitKeyRequest.getClientID());
      if (dbAppendOpenKey != null) {
        omClientResponse = commitAppendSession(ozoneManager, trxnLogIndex, omBucketInfo,
            commitKeyRequest.getClientID(), dbAppendOpenKey, locationInfoList, omResponse, auditMap);
        return omClientResponse;
      }

      String errMsg = "Cannot create file : " + keyName
              + " as parent directory doesn't exist";
      OmFSOFile fsoFile =  new OmFSOFile.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName(keyName)
          .setOmMetadataManager(omMetadataManager)
          .setErrMsg(errMsg)
          .build();

      String fileName = fsoFile.getFileName();
      long volumeId = fsoFile.getVolumeId();
      String dbFileKey = fsoFile.getOzonePathKey();
      OmKeyInfo keyToDelete =
          omMetadataManager.getKeyTable(getBucketLayout()).get(dbFileKey);
      long writerClientId = commitKeyRequest.getClientID();
      if (keyToDelete != null && keyToDelete.getAppendOwnerSessionId() != null && isRecovery && writerClientId == 0) {
        // Recovery by a client that does not know the session ID: the file names its append session.
        long owner = keyToDelete.getAppendOwnerSessionId();
        omClientResponse = commitAppendSession(ozoneManager, trxnLogIndex, omBucketInfo, owner,
            omMetadataManager.getAppendSessionOpenKey(volumeName, bucketName, owner), locationInfoList, omResponse,
            auditMap);
        return omClientResponse;
      }
      boolean isSameHsyncKey = false;
      boolean isOverwrittenHsyncKey = false;
      final String clientIdString = String.valueOf(writerClientId);
      if (null != keyToDelete) {
        isSameHsyncKey = java.util.Optional.of(keyToDelete)
            .map(WithMetadata::getMetadata)
            .map(meta -> meta.get(OzoneConsts.HSYNC_CLIENT_ID))
            .filter(id -> id.equals(clientIdString))
            .isPresent();
        if (!isSameHsyncKey) {
          isOverwrittenHsyncKey = java.util.Optional.of(keyToDelete)
              .map(WithMetadata::getMetadata)
              .map(meta -> meta.get(OzoneConsts.HSYNC_CLIENT_ID))
              .filter(id -> !id.equals(clientIdString))
              .isPresent() && !isRecovery;
        }
      }

      if (isRecovery && keyToDelete != null) {
        String clientId = keyToDelete.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
        if (clientId == null) {
          throw new OMException("Failed to recovery key, as " +
              dbFileKey + " is already closed", KEY_ALREADY_CLOSED);
        }
        writerClientId = Long.parseLong(clientId);
      }
      dbOpenFileKey = fsoFile.getOpenFileName(writerClientId);
      omKeyInfo = OMFileRequest.getOmKeyInfoFromFileTable(true,
              omMetadataManager, dbOpenFileKey, keyName);
      if (omKeyInfo == null) {
        String action = isRecovery ? "recovery" : isHSync ? "hsync" : "commit";
        throw new OMException("Failed to " + action + " key, as " +
            dbOpenFileKey + " entry is not found in the OpenKey table", KEY_NOT_FOUND);
      } else if (omKeyInfo.getAppendSession() != null) {
        // Not in the session index: the session was invalidated when its file was deleted.
        throw new OMException("Append session " + writerClientId + " of " + keyName + " is not active",
            APPEND_SESSION_NOT_FOUND);
      } else if (omKeyInfo.getMetadata().containsKey(OzoneConsts.DELETED_HSYNC_KEY) ||
          omKeyInfo.getMetadata().containsKey(OzoneConsts.OVERWRITTEN_HSYNC_KEY)) {
        throw new OMException("Open Key " + keyName + " is already deleted/overwritten",
            KEY_NOT_FOUND);
      }

      if (omKeyInfo.getMetadata().containsKey(OzoneConsts.LEASE_RECOVERY) &&
          omKeyInfo.getMetadata().containsKey(OzoneConsts.HSYNC_CLIENT_ID)) {
        if (!isRecovery) {
          throw new OMException("Cannot commit key " + dbOpenFileKey + " with " + OzoneConsts.LEASE_RECOVERY +
              " metadata while recovery flag is not set in request", KEY_UNDER_LEASE_RECOVERY);
        }
      }

      OmKeyInfo openKeyToDelete = null;
      String dbOpenKeyToDeleteKey = null;
      if (isOverwrittenHsyncKey) {
        // find the overwritten openKey and add OVERWRITTEN_HSYNC_KEY to it.
        dbOpenKeyToDeleteKey = fsoFile.getOpenFileName(
            Long.parseLong(keyToDelete.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID)));
        openKeyToDelete = OMFileRequest.getOmKeyInfoFromFileTable(true,
            omMetadataManager, dbOpenKeyToDeleteKey, keyName);
        openKeyToDelete = openKeyToDelete.toBuilder()
            .addMetadata(OzoneConsts.OVERWRITTEN_HSYNC_KEY, "true")
            .setUpdateID(trxnLogIndex)
            .build();
        openKeyToDelete.setModificationTime(Time.now());
        OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager,
            dbOpenKeyToDeleteKey, openKeyToDelete, keyName, trxnLogIndex);
      }

      omKeyInfo.setModificationTime(commitKeyArgs.getModificationTime());
      // non-null indicates it is necessary to update the open key
      OmKeyInfo newOpenKeyInfo = null;

      if (isHSync) {
        if (!OmKeyHSyncUtil.isHSyncedPreviously(omKeyInfo, clientIdString, dbOpenFileKey)) {
          // Update open key as well if it is the first hsync of this key
          omKeyInfo = omKeyInfo.withMetadataMutations(
              metadata -> metadata.put(OzoneConsts.HSYNC_CLIENT_ID, clientIdString));
          newOpenKeyInfo = omKeyInfo.copyObject();
        }
      }

      // Set the new metadata from the request and UpdateID to current
      // transactionLogIndex
      omKeyInfo = omKeyInfo.toBuilder()
          .addAllMetadata(KeyValueUtil.getFromProtobuf(
              commitKeyArgs.getMetadataList()))
          .setDataSize(commitKeyArgs.getDataSize())
          .setUpdateID(trxnLogIndex)
          .build();

      List<OmKeyLocationInfo> uncommitted =
          omKeyInfo.updateLocationInfoList(locationInfoList, false);

      // If bucket versioning is turned on during the update, between key
      // creation and key commit, old versions will be just overwritten and
      // not kept. Bucket versioning will be effective from the first key
      // creation after the knob turned on.
      Map<String, RepeatedOmKeyInfo> oldKeyVersionsToDeleteMap = null;

      validateAtomicRewrite(keyToDelete, omKeyInfo, auditMap);
      // Optimistic locking validation has passed. Now set the rewrite fields to null so they are
      // not persisted in the key table.
      omKeyInfo = omKeyInfo.toBuilder()
          .setExpectedDataGeneration(null)
          .build();

      long correctedSpace = omKeyInfo.getReplicatedSize();
      // Same-client hsync re-commit does not consume namespace.
      if (keyToDelete != null && isSameHsyncKey) {
        correctedSpace -= keyToDelete.getReplicatedSize();
        checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
            correctedSpace);
      } else if (keyToDelete != null && !omBucketInfo.getIsVersionEnabled()) {
        RepeatedOmKeyInfo oldVerKeyInfo = getOldVersionsToCleanUp(
            keyToDelete, omBucketInfo.getObjectID(), trxnLogIndex);
        String delKeyName = omMetadataManager
            .getOzoneKey(volumeName, bucketName, fileName);
        // using pseudoObjId as objectId can be same in case of overwrite key
        long pseudoObjId = ozoneManager.getObjectIdFromTxId(trxnLogIndex);
        delKeyName = omMetadataManager.getOzoneDeletePathKey(
            pseudoObjId, delKeyName);
        if (null == oldKeyVersionsToDeleteMap) {
          oldKeyVersionsToDeleteMap = new HashMap<>();
        }

        // Remove any block from oldVerKeyInfo that share the same container ID
        // and local ID with omKeyInfo blocks'.
        // Otherwise, it causes data loss once those shared blocks are added
        // to deletedTable and processed by KeyDeletingService for deletion.
        Pair<Map<OmKeyInfo, List<OmKeyLocationInfo>>, Integer> filteredUsedBlockCnt =
            filterOutBlocksStillInUse(omKeyInfo, oldVerKeyInfo);
        Map<OmKeyInfo, List<OmKeyLocationInfo>> blocks = filteredUsedBlockCnt.getLeft();
        correctedSpace -= blocks.entrySet().stream().mapToLong(filteredKeyBlocks ->
            filteredKeyBlocks.getValue().stream().mapToLong(block -> QuotaUtil.getReplicatedSize(
                block.getLength(), filteredKeyBlocks.getKey().getReplicationConfig())).sum()).sum();
        long totalSize = 0;
        long totalNamespace = 0;
        if (!oldVerKeyInfo.getOmKeyInfoList().isEmpty()) {
          oldKeyVersionsToDeleteMap.put(delKeyName, oldVerKeyInfo);
          List<OmKeyInfo> oldKeys = oldVerKeyInfo.getOmKeyInfoList();
          for (int i = 0; i < oldKeys.size(); i++) {
            OmKeyInfo updatedOlderKeyVersions =
                oldKeys.get(i).withCommittedKeyDeletedFlag(true);
            oldKeys.set(i, updatedOlderKeyVersions);
            totalSize += sumBlockLengths(updatedOlderKeyVersions);
            totalNamespace += 1;
          }
        }
        // Subtract the size of blocks to be overwritten.
        checkBucketQuotaInNamespace(omBucketInfo, 1L);
        checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
            correctedSpace);
        // Subtract the size of blocks to be overwritten.
        omBucketInfo.decrUsedNamespace(totalNamespace, true);
        omBucketInfo.decrUsedNamespace(filteredUsedBlockCnt.getRight(), false);
        omBucketInfo.decrUsedBytes(totalSize, true);
        omBucketInfo.incrUsedNamespace(1L);
      } else {
        checkBucketQuotaInNamespace(omBucketInfo, 1L);
        checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
            correctedSpace);
        omBucketInfo.incrUsedNamespace(1L);
      }

      // let the uncommitted blocks pretend as key's old version blocks
      // which will be deleted as RepeatedOmKeyInfo
      final OmKeyInfo pseudoKeyInfo = isHSync ? null
          : wrapUncommittedBlocksAsPseudoKey(uncommitted, omKeyInfo);
      if (pseudoKeyInfo != null) {
        String delKeyName = omMetadataManager
            .getOzoneKey(volumeName, bucketName, fileName);
        long pseudoObjId = ozoneManager.getObjectIdFromTxId(trxnLogIndex);
        delKeyName = omMetadataManager.getOzoneDeletePathKey(
            pseudoObjId, delKeyName);
        if (null == oldKeyVersionsToDeleteMap) {
          oldKeyVersionsToDeleteMap = new HashMap<>();
        }
        oldKeyVersionsToDeleteMap.computeIfAbsent(delKeyName,
            key -> new RepeatedOmKeyInfo(omBucketInfo.getObjectID())).addOmKeyInfo(pseudoKeyInfo);
      }

      // Add to cache of open key table and key table.
      if (!isHSync) {
        // If isHSync = false, put a tombstone in OpenKeyTable cache,
        // indicating the key is removed from OpenKeyTable.
        // So that this key can't be committed again.
        OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager,
            dbOpenFileKey, null, keyName, trxnLogIndex);

        // Prevent hsync metadata from getting committed to the final key
        omKeyInfo = omKeyInfo.withMetadataMutations(
            metadata -> metadata.remove(OzoneConsts.HSYNC_CLIENT_ID));
        if (isRecovery) {
          omKeyInfo = omKeyInfo.withMetadataMutations(
              metadata -> metadata.remove(OzoneConsts.LEASE_RECOVERY));
        }
      } else if (newOpenKeyInfo != null) {
        // isHSync is true and newOpenKeyInfo is set, update OpenKeyTable
        OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager,
            dbOpenFileKey, newOpenKeyInfo, keyName, trxnLogIndex);
      }

      OMFileRequest.addFileTableCacheEntry(omMetadataManager, dbFileKey,
              omKeyInfo, fileName, trxnLogIndex);

      omBucketInfo.incrUsedBytes(correctedSpace);

      omMetadataManager.getBucketTable().addCacheEntry(
          omMetadataManager.getBucketKey(volumeName, bucketName), omBucketInfo, trxnLogIndex);

      omResponse.setCommitKeyResponse(CommitKeyResponse.newBuilder()
          .setModificationTime(commitKeyArgs.getModificationTime())
          .build());

      omClientResponse = new OMKeyCommitResponseWithFSO(omResponse.build(),
          omKeyInfo, dbFileKey, dbOpenFileKey, omBucketInfo.copyObject(),
          oldKeyVersionsToDeleteMap, volumeId, isHSync, newOpenKeyInfo, dbOpenKeyToDeleteKey, openKeyToDelete);

      result = Result.SUCCESS;
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyCommitResponseWithFSO(createErrorOMResponse(
              omResponse, exception), getBucketLayout());
    } finally {
      if (bucketLockAcquired) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    // Debug logging for any key commit operation, successful or not
    LOG.debug("Key commit {} with isHSync = {}, omKeyInfo = {}",
        result == Result.SUCCESS ? "succeeded" : "failed", isHSync, omKeyInfo);

    if (!isHSync) {
      markForAudit(auditLogger, buildAuditMessage(OMAction.COMMIT_KEY, auditMap,
              exception, getOmRequest().getUserInfo()));
      processResult(commitKeyRequest, volumeName, bucketName, keyName,
          omMetrics, exception, omKeyInfo, result);
    }

    return omClientResponse;
  }

  /**
   * Publishes the suffix of an append session into its committed file: hsync, close or recovery completion. The
   * result is built from the current committed record, so attribute updates made since admission are kept. Must be
   * called with the bucket write lock held.
   *
   * @param dbOpenFileKey open file table DB key of the session from the session index, null if it has none
   * @param submitted the suffix blocks sent by the client, with their lengths
   */
  @SuppressWarnings("parameternumber")
  private OMClientResponse commitAppendSession(OzoneManager ozoneManager, long trxnLogIndex, OmBucketInfo omBucketInfo,
      long sessionId, String dbOpenFileKey, List<OmKeyLocationInfo> submitted, OMResponse.Builder omResponse,
      Map<String, String> auditMap) throws IOException {
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    CommitKeyRequest commitKeyRequest = getOmRequest().getCommitKeyRequest();
    KeyArgs commitKeyArgs = commitKeyRequest.getKeyArgs();
    boolean isHSync = commitKeyRequest.hasHsync() && commitKeyRequest.getHsync();
    boolean isRecovery = commitKeyRequest.hasRecovery() && commitKeyRequest.getRecovery();

    if (isHSync && isRecovery) {
      // Would publish for a fenced session without ending it.
      throw new OMException("Append session " + sessionId + " cannot hsync and recover in one request",
          INVALID_REQUEST);
    }
    OmKeyInfo openRecord = dbOpenFileKey == null ? null
        : omMetadataManager.getOpenKeyTable(getBucketLayout()).get(dbOpenFileKey);
    OmAppendSession session = openRecord == null ? null : openRecord.getAppendSession();
    String dbFileKey = session == null ? null : OmAppendUtil.getDbFileKey(omMetadataManager, openRecord);
    OmKeyInfo committed = session == null ? null : omMetadataManager.getKeyTable(getBucketLayout()).get(dbFileKey);
    OmKeyLocationInfoGroup committedGroup = committed == null ? null : committed.getLatestVersionLocations();
    List<OmKeyLocationInfo> committedBlocks =
        committedGroup == null ? new ArrayList<>() : committedGroup.createLocationList();
    // A RECOVERING session is fenced: only a recovery commit may finish it.
    if (!OmAppendUtil.isOwnedBy(committed, sessionId) || committedBlocks.size() < session.getPrefixBlockCount()
        || !(session.isActive() || isRecovery && session.getPhase() == AppendSessionPhase.APPEND_RECOVERING)) {
      throw new OMException("Append session " + sessionId + " of " + commitKeyArgs.getKeyName() + " is not active",
          APPEND_SESSION_NOT_FOUND);
    }
    if (isRecovery && session.isActive()) {
      // The request carries the time, so every OM decides the same way.
      long softLimit = ozoneManager.getConfiguration().getTimeDuration(OZONE_OM_LEASE_SOFT_LIMIT,
          OZONE_OM_LEASE_SOFT_LIMIT_DEFAULT, TimeUnit.MILLISECONDS);
      if (commitKeyArgs.getModificationTime() < session.getLastRenewedAt() + softLimit) {
        throw new OMException("Append session " + sessionId + " of " + openRecord.getKeyName()
            + " was renewed recently and is inside soft limit period", KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD);
      }
    }

    List<OmKeyLocationInfo> prefix = committedBlocks.subList(0, session.getPrefixBlockCount());
    List<OmKeyLocationInfo> published = committedBlocks.subList(session.getPrefixBlockCount(), committedBlocks.size());
    // Only blocks that this session allocated can be published, each once, with the lengths the client reports.
    Set<ContainerBlockID> allocated = openRecord.getLatestVersionLocations().createLocationList().stream()
        .map(location -> location.getBlockID().getContainerBlockID()).collect(Collectors.toSet());
    List<OmKeyLocationInfo> suffix = submitted.stream()
        .filter(location -> allocated.remove(location.getBlockID().getContainerBlockID()))
        .collect(Collectors.toList());
    if (!coversPublished(suffix, published)) {
      if (!isRecovery) {
        throw new OMException("Append session " + sessionId + " of " + openRecord.getKeyName()
            + " cannot unpublish data", INVALID_REQUEST);
      }
      // Recovery found less than what hsync already published: the published data is the lower bound.
      suffix = published;
    } else if (session.getPrefixLength() + sumLengths(suffix) != commitKeyArgs.getDataSize()) {
      throw new OMException("Append session " + sessionId + " of " + openRecord.getKeyName() + " submitted data size "
          + commitKeyArgs.getDataSize() + " that does not match its blocks", INVALID_REQUEST);
    }

    long locationVersion = committedGroup == null ? 0 : committedGroup.getVersion();
    List<OmKeyLocationInfo> newBlocks = new ArrayList<>(prefix);
    for (OmKeyLocationInfo location : suffix) {
      location.setCreateVersion(locationVersion);
      newBlocks.add(location);
    }
    long newDataSize = session.getPrefixLength() + sumLengths(suffix);
    OmKeyInfo.Builder builder = committed.toBuilder()
        .setDataSize(newDataSize)
        .setUpdateID(trxnLogIndex);
    if (newDataSize > committed.getDataSize()) {
      // The checksum and the ETag of the whole file were computed from the old contents.
      builder.setModificationTime(commitKeyArgs.getModificationTime()).setFileChecksum(null);
      builder.metadata().remove(OzoneConsts.ETAG);
    }
    if (!isHSync) {
      builder.setAppendOwnerSessionId(null);
    }
    OmKeyInfo newCommitted = builder.build();
    List<OmKeyLocationInfoGroup> locationVersions = newCommitted.getKeyLocationVersions();
    if (!locationVersions.isEmpty()) {
      locationVersions.remove(locationVersions.size() - 1);
    }
    locationVersions.add(new OmKeyLocationInfoGroup(locationVersion, newBlocks,
        committedGroup != null && committedGroup.isMultipartKey()));

    // Only the newly published bytes are charged. The file already exists, so the namespace does not change.
    // ponytail: uses the whole-file replicated size formula, which undercounts an EC file whose prefix ends in a
    // partial block group. Delete releases by the same formula, so usage does not drift. Per block group accounting
    // is the upgrade.
    long addedSpace = newCommitted.getReplicatedSize() - committed.getReplicatedSize();
    checkBucketQuotaInBytes(omMetadataManager, omBucketInfo, addedSpace);

    Map<String, RepeatedOmKeyInfo> unusedBlocksToDelete = null;
    if (!isHSync) {
      // Close or recovery completion ends the session. Blocks it allocated but never published are released.
      List<OmKeyLocationInfo> unused = OmAppendUtil.privateAllocations(openRecord, newCommitted);
      // The pseudo key carries the size of the unused blocks, so that it is also created for an empty file.
      OmKeyInfo pseudoKeyInfo = wrapUncommittedBlocksAsPseudoKey(unused, newCommitted.toBuilder()
          .setKeyName(openRecord.getKeyName()).setDataSize(sumLengths(unused)).build());
      unusedBlocksToDelete = addKeyInfoToDeleteMap(ozoneManager, trxnLogIndex,
          omMetadataManager.getOzoneKey(openRecord.getVolumeName(), openRecord.getBucketName(),
              openRecord.getFileName()),
          omBucketInfo.getObjectID(), pseudoKeyInfo, null);
      OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager, dbOpenFileKey, null, null, trxnLogIndex);
      omMetadataManager.removeAppendSession(openRecord.getVolumeName(), openRecord.getBucketName(), sessionId);
    }
    OMFileRequest.addFileTableCacheEntry(omMetadataManager, dbFileKey, newCommitted, openRecord.getFileName(),
        trxnLogIndex);
    omBucketInfo.incrUsedBytes(addedSpace);
    omMetadataManager.getBucketTable().addCacheEntry(
        omMetadataManager.getBucketKey(openRecord.getVolumeName(), openRecord.getBucketName()), omBucketInfo,
        trxnLogIndex);

    omResponse.setCommitKeyResponse(CommitKeyResponse.newBuilder()
        .setModificationTime(commitKeyArgs.getModificationTime())
        .build());
    if (!isHSync) {
      markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(OMAction.COMMIT_KEY, auditMap, null,
          getOmRequest().getUserInfo()));
      ozoneManager.getMetrics().incDataCommittedBytes(newDataSize - session.getPrefixLength());
    }
    return new OMKeyCommitResponseWithFSO(omResponse.build(), newCommitted, dbFileKey, dbOpenFileKey,
        omBucketInfo.copyObject(), unusedBlocksToDelete, omMetadataManager.getVolumeId(openRecord.getVolumeName()),
        isHSync, null, null, null);
  }

  /** Returns true if the suffix starts with the published blocks, in the same order, and none of them is shorter. */
  private static boolean coversPublished(List<OmKeyLocationInfo> suffix, List<OmKeyLocationInfo> published) {
    if (suffix.size() < published.size()) {
      return false;
    }
    for (int i = 0; i < published.size(); i++) {
      if (!suffix.get(i).getBlockID().getContainerBlockID().equals(published.get(i).getBlockID().getContainerBlockID())
          || suffix.get(i).getLength() < published.get(i).getLength()) {
        return false;
      }
    }
    return true;
  }

  private static long sumLengths(List<OmKeyLocationInfo> locations) {
    return locations.stream().mapToLong(OmKeyLocationInfo::getLength).sum();
  }
}
