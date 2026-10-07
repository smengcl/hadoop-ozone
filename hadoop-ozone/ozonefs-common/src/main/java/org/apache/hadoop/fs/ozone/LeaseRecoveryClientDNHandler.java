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

package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_NOT_FOUND;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.NO_SUCH_BLOCK;
import static org.apache.hadoop.ozone.OzoneConsts.FORCE_LEASE_RECOVERY_ENV;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.om.helpers.LeaseKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmAppendSession;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles lease recovery call between client and DN.
 */
public final class LeaseRecoveryClientDNHandler {
  static final Logger LOG = LoggerFactory.getLogger(LeaseRecoveryClientDNHandler.class);

  private LeaseRecoveryClientDNHandler() {
      // Not required.
  }

  /**
   * Get actual block length from DN for the last/penultimate block and update in KeyLocationInfo.
   * For a file under an append session only the blocks written by the session (the suffix) are recovered and
   * returned. OM keeps the blocks the file had before the session (the prefix), they are never finalized here.
   * @param leaseKeyInfo keyInfo received from OM
   * @param adapter client adapter
   * @param forceRecovery whether to do force recovery
   * @return List<OmKeyLocationInfo>
   */
  @Nonnull
  public static List<OmKeyLocationInfo> getOmKeyLocationInfos(LeaseKeyInfo leaseKeyInfo,
      OzoneClientAdapter adapter, boolean forceRecovery) throws IOException {
    OmKeyLocationInfoGroup keyLatestVersionLocations = leaseKeyInfo.getKeyInfo().getLatestVersionLocations();
    List<OmKeyLocationInfo> keyLocationInfoList = keyLatestVersionLocations.createLocationList();
    OmKeyLocationInfoGroup openKeyLatestVersionLocations = leaseKeyInfo.getOpenKeyInfo().getLatestVersionLocations();
    List<OmKeyLocationInfo> openKeyLocationInfoList = openKeyLatestVersionLocations.createLocationList();

    OmAppendSession appendSession = leaseKeyInfo.getOpenKeyInfo().getAppendSession();
    if (appendSession != null) {
      // The open record holds the suffix only. The published suffix is what hsync added after the prefix blocks.
      keyLocationInfoList = new ArrayList<>(keyLocationInfoList.subList(
          Math.min(appendSession.getPrefixBlockCount(), keyLocationInfoList.size()), keyLocationInfoList.size()));
      if (openKeyLocationInfoList.isEmpty()
          || leaseKeyInfo.getKeyInfo().getReplicationConfig().getReplicationType() == ReplicationType.EC) {
        // Nothing was allocated, or EC, which publishes on close only and has no block finalization:
        // keep what was published.
        return keyLocationInfoList;
      }
      if (keyLocationInfoList.isEmpty()) {
        return finalizeUnpublishedSuffix(openKeyLocationInfoList, adapter, forceRecovery);
      }
      // The cases below do not apply: the open record keeps every block the session allocated, also the ones the
      // writer abandoned, so its block count says nothing about what is published.
      // The writer may have written past the last publication in the last published block, and in the blocks
      // allocated after it.
      OmKeyLocationInfo lastPublished = keyLocationInfoList.get(keyLocationInfoList.size() - 1);
      try {
        lastPublished.setLength(adapter.finalizeBlock(lastPublished));
      } catch (IOException e) {
        if (!forceRecovery) {
          throw e;
        }
        LOG.warn("Failed to finalize block. Continue to recover the file since {} is enabled.",
            FORCE_LEASE_RECOVERY_ENV, e);
      }
      int next = openKeyLocationInfoList.size();
      for (int i = 0; i < openKeyLocationInfoList.size(); i++) {
        // Not BlockID.equals: the open record keeps the block commit sequence ID of the allocation.
        if (openKeyLocationInfoList.get(i).getBlockID().getContainerBlockID()
            .equals(lastPublished.getBlockID().getContainerBlockID())) {
          next = i + 1;
        }
      }
      keyLocationInfoList.addAll(finalizeUnpublishedSuffix(
          openKeyLocationInfoList.subList(next, openKeyLocationInfoList.size()), adapter, forceRecovery));
      return keyLocationInfoList;
    }

    int openKeyLocationSize = openKeyLocationInfoList.size();
    int keyLocationSize = keyLocationInfoList.size();
    OmKeyLocationInfo openKeyFinalBlock = null;
    OmKeyLocationInfo openKeyPenultimateBlock = null;
    OmKeyLocationInfo keyFinalBlock;

    if (keyLocationSize > 0) {
      // Block info from fileTable
      keyFinalBlock = keyLocationInfoList.get(keyLocationSize - 1);
      // Block info from openFileTable
      if (openKeyLocationSize > 1) {
        openKeyFinalBlock = openKeyLocationInfoList.get(openKeyLocationSize - 1);
        openKeyPenultimateBlock = openKeyLocationInfoList.get(openKeyLocationSize - 2);
      } else if (openKeyLocationSize > 0) {
        openKeyFinalBlock = openKeyLocationInfoList.get(0);
      }
      // Finalize the final block and get block length
      try {
        // CASE 1: When openFileTable has more block than fileTable
        // Try to finalize last block of openFileTable
        // Add that block into fileTable locationInfo
        if (openKeyLocationSize > keyLocationSize) {
          openKeyFinalBlock.setLength(adapter.finalizeBlock(openKeyFinalBlock));
          keyLocationInfoList.add(openKeyFinalBlock);
        }
        // CASE 2: When openFileTable penultimate block length is not equal to fileTable block length of last block
        // Finalize and get the actual block length and update in fileTable last block
        if ((openKeyPenultimateBlock != null && keyFinalBlock != null) &&
            openKeyPenultimateBlock.getLength() != keyFinalBlock.getLength() &&
            openKeyPenultimateBlock.getBlockID().getLocalID() == keyFinalBlock.getBlockID().getLocalID()) {
          keyFinalBlock.setLength(adapter.finalizeBlock(keyFinalBlock));
        }
        // CASE 3: When openFileTable has same number of blocks as fileTable
        // Finalize and get actual length of fileTable final block
        if (keyLocationInfoList.size() == openKeyLocationInfoList.size() && keyFinalBlock != null) {
          keyFinalBlock.setLength(adapter.finalizeBlock(keyFinalBlock));
        }
      } catch (Throwable e) {
        if (e instanceof StorageContainerException &&
            (((StorageContainerException) e).getResult().equals(NO_SUCH_BLOCK)
            || ((StorageContainerException) e).getResult().equals(CONTAINER_NOT_FOUND))
            && openKeyPenultimateBlock != null && keyFinalBlock != null &&
            openKeyPenultimateBlock.getBlockID().getLocalID() == keyFinalBlock.getBlockID().getLocalID()) {
          try {
            keyFinalBlock.setLength(adapter.finalizeBlock(keyFinalBlock));
          } catch (Throwable exp) {
            if (!forceRecovery) {
              throw exp;
            }
            LOG.warn("Failed to finalize block. Continue to recover the file since {} is enabled.",
                FORCE_LEASE_RECOVERY_ENV, exp);
          }
        } else if (!forceRecovery) {
          throw e;
        } else {
          LOG.warn("Failed to finalize block. Continue to recover the file since {} is enabled.",
              FORCE_LEASE_RECOVERY_ENV, e);
        }
      }
    }
    return keyLocationInfoList;
  }

  /**
   * OM only knows the allocated lengths of the suffix blocks that no hsync of the append session published.
   * Finalizes them in allocation order and keeps the leading blocks that have data.
   */
  private static List<OmKeyLocationInfo> finalizeUnpublishedSuffix(List<OmKeyLocationInfo> openSuffix,
      OzoneClientAdapter adapter, boolean forceRecovery) throws IOException {
    List<OmKeyLocationInfo> recovered = new ArrayList<>();
    for (OmKeyLocationInfo block : openSuffix) {
      if (block.getPipeline() == null) {
        // ponytail: OM returns the pipeline of the last two open blocks only, so with more unpublished blocks none
        // is recovered and the file ends at its last published byte. Have OM return the pipeline of every
        // unpublished block to recover them.
        LOG.warn("Not recovering block {} and the blocks after it, OM returned no pipeline for it", block.getBlockID());
        break;
      }
      long length;
      try {
        length = adapter.finalizeBlock(block);
      } catch (IOException e) {
        boolean neverWritten = e instanceof StorageContainerException &&
            (((StorageContainerException) e).getResult() == NO_SUCH_BLOCK
            || ((StorageContainerException) e).getResult() == CONTAINER_NOT_FOUND);
        if (!neverWritten) {
          if (!forceRecovery) {
            throw e;
          }
          LOG.warn("Failed to finalize block. Continue to recover the file since {} is enabled.",
              FORCE_LEASE_RECOVERY_ENV, e);
        }
        break;
      }
      if (length == 0) {
        break;
      }
      block.setLength(length);
      recovered.add(block);
    }
    return recovered;
  }

  /**
   * @param leaseKeyInfo keyInfo received from OM
   * @param recoveredLocations result of {@link #getOmKeyLocationInfos}
   * @return the file length to commit: the recovered blocks, plus the prefix length for an append session
   */
  public static long getRecoveredLength(LeaseKeyInfo leaseKeyInfo, List<OmKeyLocationInfo> recoveredLocations) {
    long length = recoveredLocations.stream().mapToLong(OmKeyLocationInfo::getLength).sum();
    OmAppendSession appendSession = leaseKeyInfo.getOpenKeyInfo().getAppendSession();
    return appendSession == null ? length : appendSession.getPrefixLength() + length;
  }
}
