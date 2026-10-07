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

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.file.OMAppendLeaseRenewResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionKey;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenewAppendLeasesRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenewAppendLeasesResponse;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles RenewAppendLeases requests: extends the lease of every listed append session that is still APPEND_ACTIVE.
 * A session that is unknown, fenced or gone is answered with false and does not fail the others.
 */
public class OMAppendLeaseRenewRequest extends OMClientRequest {
  private static final Logger LOG = LoggerFactory.getLogger(OMAppendLeaseRenewRequest.class);

  public OMAppendLeaseRenewRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMRequest request = super.preExecute(ozoneManager);
    RenewAppendLeasesRequest.Builder renewRequest = request.getRenewAppendLeasesRequest().toBuilder()
        .setRenewalTime(Time.now());
    // Sessions are indexed by the bucket that holds the file, not by a link to it.
    for (int i = 0; i < renewRequest.getSessionsCount(); i++) {
      AppendSessionKey session = renewRequest.getSessions(i);
      try {
        ResolvedBucket bucket = ozoneManager.resolveBucketLink(
            Pair.of(session.getVolumeName(), session.getBucketName()), this);
        renewRequest.setSessions(i, session.toBuilder()
            .setVolumeName(bucket.realVolume()).setBucketName(bucket.realBucket()));
      } catch (IOException e) {
        // The session is answered with false, as its bucket has no such session.
        LOG.debug("Cannot resolve bucket {}/{} of append session {}", session.getVolumeName(),
            session.getBucketName(), session.getSessionId(), e);
      }
    }
    return request.toBuilder().setRenewAppendLeasesRequest(renewRequest).build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();
    RenewAppendLeasesRequest renewRequest = getOmRequest().getRenewAppendLeasesRequest();
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    OMClientResponse omClientResponse;
    try {
      RenewAppendLeasesResponse.Builder results = RenewAppendLeasesResponse.newBuilder();
      Map<String, OmKeyInfo> renewedOpenKeys = new HashMap<>();
      for (AppendSessionKey session : renewRequest.getSessionsList()) {
        results.addRenewed(renew(ozoneManager.getMetadataManager(), session, renewRequest.getRenewalTime(),
            trxnLogIndex, renewedOpenKeys));
      }
      omClientResponse = new OMAppendLeaseRenewResponse(omResponse.setRenewAppendLeasesResponse(results).build(),
          renewedOpenKeys);
    } catch (IOException ex) {
      LOG.error("Append lease renewal failed for {} sessions", renewRequest.getSessionsCount(), ex);
      omClientResponse = new OMAppendLeaseRenewResponse(createErrorOMResponse(omResponse, ex));
      // Successful renewals are frequent and, like hsync, are not audited.
      Map<String, String> auditMap = new LinkedHashMap<>();
      auditMap.put("sessions", String.valueOf(renewRequest.getSessionsCount()));
      markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(OMAction.RENEW_APPEND_LEASES, auditMap, ex,
          getOmRequest().getUserInfo()));
    }
    omClientResponse.setOmLockDetails(getOmLockDetails());
    return omClientResponse;
  }

  /**
   * Renews one session under the write lock of its bucket. The request may span buckets, which are locked one at a
   * time.
   */
  private boolean renew(OMMetadataManager omMetadataManager, AppendSessionKey session, long renewalTime,
      long trxnLogIndex, Map<String, OmKeyInfo> renewedOpenKeys) throws IOException {
    String volumeName = session.getVolumeName();
    String bucketName = session.getBucketName();
    mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
    try {
      String dbOpenKey = omMetadataManager.getAppendSessionOpenKey(volumeName, bucketName, session.getSessionId());
      Table<String, OmKeyInfo> openKeyTable = omMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED);
      OmKeyInfo openRecord = dbOpenKey == null ? null : openKeyTable.get(dbOpenKey);
      if (openRecord == null || openRecord.getAppendSession() == null
          || !openRecord.getAppendSession().isActive()) {
        return false;
      }
      // Only the lease moves: the file and its modification time are not touched.
      OmKeyInfo renewed = openRecord.toBuilder()
          .setAppendSession(openRecord.getAppendSession().withRenewal(renewalTime))
          .setUpdateID(trxnLogIndex)
          .build();
      openKeyTable.addCacheEntry(dbOpenKey, renewed, trxnLogIndex);
      renewedOpenKeys.put(dbOpenKey, renewed);
      return true;
    } finally {
      mergeOmLockDetails(omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
    }
  }
}
