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
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.key.OmKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for RenewAppendLeases request: persists the open records of the renewed append sessions.
 */
public class OMAppendLeaseRenewResponse extends OmKeyResponse {

  private Map<String, OmKeyInfo> renewedOpenKeys;

  /**
   * @param renewedOpenKeys renewed open records by their open file table DB key
   */
  public OMAppendLeaseRenewResponse(@Nonnull OMResponse omResponse, @Nonnull Map<String, OmKeyInfo> renewedOpenKeys) {
    super(omResponse, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    this.renewedOpenKeys = renewedOpenKeys;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMAppendLeaseRenewResponse(@Nonnull OMResponse omResponse) {
    super(omResponse, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager, BatchOperation batchOperation)
      throws IOException {
    for (Map.Entry<String, OmKeyInfo> renewed : renewedOpenKeys.entrySet()) {
      omMetadataManager.getOpenKeyTable(getBucketLayout()).putWithBatch(batchOperation, renewed.getKey(),
          renewed.getValue());
    }
  }
}
