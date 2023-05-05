/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.response.key;

import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.om.IOmMetadataReader;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.request.key.OMKeyPurgeRequest;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;

/**
 * Response for {@link OMKeyPurgeRequest} request.
 */
@CleanupTableInfo(cleanupTables = {DELETED_TABLE})
public class OMKeyPurgeResponse extends OmKeyResponse {
  private List<String> purgeKeyList;
  private ReferenceCounted<IOmMetadataReader> rcFromSnapshot;

  public OMKeyPurgeResponse(@Nonnull OMResponse omResponse,
      @Nonnull List<String> keyList,
      ReferenceCounted<IOmMetadataReader> rcFromSnapshot) {
    super(omResponse);
    this.purgeKeyList = keyList;
    this.rcFromSnapshot = rcFromSnapshot;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMKeyPurgeResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    if (rcFromSnapshot != null) {
      OmSnapshot fromSnapshot = (OmSnapshot) rcFromSnapshot.get();
      DBStore fromSnapshotStore = fromSnapshot.getMetadataManager().getStore();
      // Init Batch Operation for snapshot db.
      try (BatchOperation writeBatch = fromSnapshotStore.initBatchOperation()) {
        processKeys(writeBatch, fromSnapshot.getMetadataManager());
        fromSnapshotStore.commitBatchOperation(writeBatch);
      }
      // Decrement ref count
      rcFromSnapshot.close();
    } else {
      processKeys(batchOperation, omMetadataManager);
    }
  }

  private void processKeys(BatchOperation batchOp,
      OMMetadataManager metadataManager) throws IOException {
    for (String key : purgeKeyList) {
      metadataManager.getDeletedTable().deleteWithBatch(batchOp,
          key);
    }
  }

}
