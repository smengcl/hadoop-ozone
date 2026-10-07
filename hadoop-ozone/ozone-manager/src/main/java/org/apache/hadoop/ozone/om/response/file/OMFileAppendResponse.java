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

package org.apache.hadoop.ozone.om.response.file;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.key.OmKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for AppendFile request: persists the reserved committed file and the open record of the new session.
 */
public class OMFileAppendResponse extends OmKeyResponse {

  private OmKeyInfo committedKeyInfo;
  private OmKeyInfo openKeyInfo;
  private long sessionId;
  private long volumeId;
  private long bucketId;

  public OMFileAppendResponse(@Nonnull OMResponse omResponse, @Nonnull OmKeyInfo committedKeyInfo,
      @Nonnull OmKeyInfo openKeyInfo, long sessionId, long volumeId, long bucketId) {
    super(omResponse, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    this.committedKeyInfo = committedKeyInfo;
    this.openKeyInfo = openKeyInfo;
    this.sessionId = sessionId;
    this.volumeId = volumeId;
    this.bucketId = bucketId;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMFileAppendResponse(@Nonnull OMResponse omResponse, @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager, BatchOperation batchOperation)
      throws IOException {
    OMFileRequest.addToFileTable(omMetadataManager, batchOperation, committedKeyInfo, volumeId, bucketId);
    OMFileRequest.addToOpenFileTable(omMetadataManager, batchOperation, openKeyInfo, sessionId, volumeId, bucketId);
  }
}
