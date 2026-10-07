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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_APPEND_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_APPEND_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.APPEND_NOT_SUPPORTED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.APPEND_WRITER_CONFLICT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.DIRECTORY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.APPEND;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
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
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OmAppendUtil;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.file.OMFileAppendResponse;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendConflictInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendFileResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionPhase;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendWriterKind;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles AppendFile requests: admits an append session on an existing file of an FSO bucket. The committed file is
 * reserved for the session and an open record that will hold the session's suffix blocks is created.
 */
public class OMFileAppendRequest extends OMKeyRequest {
  private static final Logger LOG = LoggerFactory.getLogger(OMFileAppendRequest.class);
  private static final SecureRandom SESSION_ID_GENERATOR = new SecureRandom();

  /** Describes the writer that blocks admission. Set together with an APPEND_WRITER_CONFLICT failure. */
  private AppendConflictInfo conflict;

  public OMFileAppendRequest(OMRequest omRequest) {
    super(omRequest, BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  @Override
  @DisallowedUntilLayoutVersion(APPEND)
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMRequest request = super.preExecute(ozoneManager);
    // Checked on the leader only: the setting may differ between OMs during a rolling restart.
    if (!ozoneManager.getConfiguration().getBoolean(OZONE_OM_APPEND_ENABLED, OZONE_OM_APPEND_ENABLED_DEFAULT)) {
      throw new OMException("Append is not enabled. To enable, set " + OZONE_OM_APPEND_ENABLED + " = true",
          APPEND_NOT_SUPPORTED);
    }
    AppendFileRequest appendFileRequest = request.getAppendFileRequest();
    KeyArgs keyArgs = appendFileRequest.getKeyArgs();

    String keyPath = validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(), keyArgs.getKeyName(),
        getBucketLayout());
    KeyArgs resolvedArgs = resolveBucketAndCheckKeyAcls(
        keyArgs.toBuilder().setKeyName(keyPath).setModificationTime(Time.now()).build(),
        ozoneManager, IAccessAuthorizer.ACLType.WRITE);
    OmAppendUtil.checkNativeFileAcls(ozoneManager, this, resolvedArgs.getVolumeName(), resolvedArgs.getBucketName(),
        keyPath);

    return request.toBuilder()
        .setAppendFileRequest(appendFileRequest.toBuilder().setKeyArgs(resolvedArgs).setClientID(newSessionId()))
        .build();
  }

  /**
   * Session IDs must not be predictable: allocate, hsync and close are resolved through the session index by ID, and a
   * request that names the ID of a session before its admission is applied would be authorized by its own path.
   */
  private static long newSessionId() {
    long id;
    do {
      id = SESSION_ID_GENERATOR.nextLong() & Long.MAX_VALUE;
    } while (id == 0);
    return id;
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();
    AppendFileRequest appendFileRequest = getOmRequest().getAppendFileRequest();
    KeyArgs keyArgs = appendFileRequest.getKeyArgs();
    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();
    long sessionId = appendFileRequest.getClientID();

    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OMMetrics omMetrics = ozoneManager.getMetrics();
    OMClientResponse omClientResponse = null;
    Exception exception = null;
    boolean acquiredLock = false;
    try {
      if (keyName.isEmpty()) {
        throw new OMException("Cannot append to the bucket root", NOT_A_FILE);
      }

      mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      OmFSOFile fsoFile = resolveFile(omMetadataManager, volumeName, bucketName, keyName);
      String dbFileKey = fsoFile.getOzonePathKey();
      OmKeyInfo committed = omMetadataManager.getKeyTable(getBucketLayout()).get(dbFileKey);
      if (committed == null) {
        throw new OMException("Cannot append to " + keyName + " as the file does not exist", KEY_NOT_FOUND);
      }
      if (Boolean.parseBoolean(committed.getMetadata().get(OzoneConsts.GDPR_FLAG))) {
        // The GDPR cipher stream cannot resume at an offset.
        throw new OMException("Cannot append to GDPR encrypted file " + keyName, APPEND_NOT_SUPPORTED);
      }
      String openKeyPrefix = omMetadataManager.getOpenFileName(fsoFile.getVolumeId(), fsoFile.getBucketId(),
          fsoFile.getParentID(), fsoFile.getFileName(), "");
      checkNoWriter(ozoneManager, committed, openKeyPrefix, keyName, keyArgs.getModificationTime());

      OmKeyLocationInfoGroup committedLocations = committed.getLatestVersionLocations();
      long openVersion = committedLocations == null ? 0 : committedLocations.getVersion();
      int prefixBlockCount = committedLocations == null ? 0 : (int) committedLocations.getLocationListCount();
      // The open record starts without blocks: it only ever holds the blocks this session allocates.
      OmKeyInfo openRecord = committed.toBuilder()
          .setOmKeyLocationInfos(
              Collections.singletonList(new OmKeyLocationInfoGroup(openVersion, Collections.emptyList())))
          .setAppendSession(
              OmAppendSession.newActive(committed.getDataSize(), prefixBlockCount, keyArgs.getModificationTime()))
          .setUpdateID(trxnLogIndex)
          .build();
      OmKeyInfo reserved = committed.toBuilder()
          .setAppendOwnerSessionId(sessionId)
          .setUpdateID(trxnLogIndex)
          .build();

      String dbOpenFileKey = fsoFile.getOpenFileName(sessionId);
      OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager, dbOpenFileKey, openRecord, keyName, trxnLogIndex);
      OMFileRequest.addFileTableCacheEntry(omMetadataManager, dbFileKey, reserved, fsoFile.getFileName(),
          trxnLogIndex);
      omMetadataManager.putAppendSession(volumeName, bucketName, sessionId, dbOpenFileKey);

      omResponse.setAppendFileResponse(AppendFileResponse.newBuilder()
          .setKeyInfo(openRecord.getNetworkProtobuf(keyName, getOmRequest().getVersion(),
              keyArgs.getLatestVersionLocation()))
          .setID(sessionId)
          .setOpenVersion(openVersion))
          .setCmdType(Type.AppendFile);
      omClientResponse = new OMFileAppendResponse(omResponse.build(), reserved, openRecord, sessionId,
          fsoFile.getVolumeId(), fsoFile.getBucketId());
      omMetrics.incNumAppendFile();
      LOG.debug("Append session {} admitted. Volume:{}, Bucket:{}, Key:{}", sessionId, volumeName, bucketName,
          keyName);
    } catch (IOException | InvalidPathException ex) {
      LOG.error("Append admission failed. Volume:{}, Bucket:{}, Key:{}", volumeName, bucketName, keyName, ex);
      exception = ex;
      omMetrics.incNumAppendFileFails();
      if (conflict != null) {
        omResponse.setAppendFileResponse(AppendFileResponse.newBuilder().setConflict(conflict));
      }
      omClientResponse = new OMFileAppendResponse(createErrorOMResponse(omResponse, exception), getBucketLayout());
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(OMAction.APPEND_FILE, auditMap, exception,
        getOmRequest().getUserInfo()));
    return omClientResponse;
  }

  private static OmFSOFile resolveFile(OMMetadataManager omMetadataManager, String volumeName, String bucketName,
      String keyName) throws IOException {
    try {
      return new OmFSOFile.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName(keyName)
          .setOmMetadataManager(omMetadataManager)
          .setErrMsg("Cannot append to " + keyName + " as the file does not exist")
          .build();
    } catch (OMException e) {
      if (e.getResult() == DIRECTORY_NOT_FOUND) {
        throw new OMException(e.getMessage(), KEY_NOT_FOUND);
      }
      // NOT_A_FILE when the path is a directory.
      throw e;
    }
  }

  /**
   * Fails with APPEND_WRITER_CONFLICT, and sets {@link #conflict}, if any writer still targets the file.
   *
   * @param openKeyPrefix open file table DB key of the file without the client ID
   */
  private void checkNoWriter(OzoneManager ozoneManager, OmKeyInfo committed, String openKeyPrefix, String keyName,
      long now) throws IOException {
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    Long owner = committed.getAppendOwnerSessionId();
    if (owner != null) {
      AppendConflictInfo.Builder info = AppendConflictInfo.newBuilder()
          .setWriterKind(AppendWriterKind.APPEND_WRITER)
          .setSessionId(owner);
      String dbOwnerOpenKey = omMetadataManager.getAppendSessionOpenKey(committed.getVolumeName(),
          committed.getBucketName(), owner);
      OmKeyInfo ownerRecord = dbOwnerOpenKey == null ? null
          : omMetadataManager.getOpenKeyTable(getBucketLayout()).get(dbOwnerOpenKey);
      if (ownerRecord != null && ownerRecord.getAppendSession() != null) {
        OmAppendSession session = ownerRecord.getAppendSession();
        long softLimit = ozoneManager.getConfiguration().getTimeDuration(OZONE_OM_LEASE_SOFT_LIMIT,
            OZONE_OM_LEASE_SOFT_LIMIT_DEFAULT, TimeUnit.MILLISECONDS);
        info.setPhase(session.getPhase())
            .setLastRenewedAt(session.getLastRenewedAt())
            .setRemainingMsUntilRecoverable(
                session.isActive() ? Math.max(0, session.getLastRenewedAt() + softLimit - now) : 0);
      }
      conflict = info.build();
    } else if (committed.getMetadata().containsKey(OzoneConsts.HSYNC_CLIENT_ID)) {
      conflict = AppendConflictInfo.newBuilder().setWriterKind(AppendWriterKind.HSYNC_WRITER).build();
    } else if (hasOpenWriter(omMetadataManager.getOpenKeyTable(getBucketLayout()), openKeyPrefix)) {
      conflict = AppendConflictInfo.newBuilder().setWriterKind(AppendWriterKind.ORDINARY_WRITER).build();
    }
    if (conflict != null) {
      throw new OMException("Cannot append to " + keyName + " as it has an active writer: "
          + conflict.getWriterKind(), APPEND_WRITER_CONFLICT);
    }
  }

  /**
   * Returns true if an open record of the file can still be committed. Applied but unflushed records are only in the
   * table cache, and a cache entry (a tombstone included) overrides the DB row of the same key.
   */
  private static boolean hasOpenWriter(Table<String, OmKeyInfo> openTable, String openKeyPrefix) throws IOException {
    // ponytail: scans the whole open file table cache under the bucket lock. Keep a per file view of open records,
    // maintained like the append session index, if admission shows up in profiles.
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>>> cache = openTable.cacheIterator();
    while (cache.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>> entry = cache.next();
      if (entry.getKey().getCacheKey().startsWith(openKeyPrefix) && isWriter(entry.getValue().getCacheValue())) {
        return true;
      }
    }
    try (Table.KeyValueIterator<String, OmKeyInfo> rows = openTable.iterator(openKeyPrefix)) {
      while (rows.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> row = rows.next();
        if (openTable.getCacheValue(new CacheKey<>(row.getKey())) == null && isWriter(row.getValue())) {
          return true;
        }
      }
    }
    return false;
  }

  /** Open records that no request can commit anymore only wait for open key cleanup and do not block append. */
  private static boolean isWriter(OmKeyInfo openRecord) {
    return openRecord != null
        && !(openRecord.getAppendSession() != null
            && openRecord.getAppendSession().getPhase() == AppendSessionPhase.APPEND_INVALIDATED)
        && !openRecord.getMetadata().containsKey(OzoneConsts.DELETED_HSYNC_KEY)
        && !openRecord.getMetadata().containsKey(OzoneConsts.OVERWRITTEN_HSYNC_KEY);
  }
}
