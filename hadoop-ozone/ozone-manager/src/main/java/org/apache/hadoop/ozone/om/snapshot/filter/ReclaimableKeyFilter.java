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

package org.apache.hadoop.ozone.om.snapshot.filter;

import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.getContainerBlockIds;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.hasSharedBlocks;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.isBlockLocationInfoSame;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.ratis.util.MemoizedCheckedSupplier;
import org.apache.ratis.util.function.CheckedSupplier;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;

/**
 * Filter to return deleted keys which are reclaimable based on their presence in previous snapshot in
 * the snapshot chain.
 */
public class ReclaimableKeyFilter extends ReclaimableFilter<OmKeyInfo> {
  private final Map<UUID, Long> exclusiveSizeMap;
  private final Map<UUID, Long> exclusiveReplicatedSizeMap;
  private Set<ContainerBlockID> retainedBlocks = Collections.emptySet();

  /**
   * @param currentSnapshotInfo  : If null the deleted keys in AOS needs to be processed, hence the latest snapshot
   *                             in the snapshot chain corresponding to bucket key needs to be processed.
   * @param keyManager      : keyManager corresponding to snapshot or AOS.
   * @param lock                 : Lock for Active OM.
   */
  public ReclaimableKeyFilter(OzoneManager ozoneManager,
                              OmSnapshotManager omSnapshotManager, SnapshotChainManager snapshotChainManager,
                              SnapshotInfo currentSnapshotInfo, KeyManager keyManager,
                              IOzoneManagerLock lock) {
    super(ozoneManager, omSnapshotManager, snapshotChainManager, currentSnapshotInfo, keyManager, lock, 2);
    this.exclusiveSizeMap = new HashMap<>();
    this.exclusiveReplicatedSizeMap = new HashMap<>();
  }

  @Override
  public synchronized Boolean apply(Table.KeyValue<String, OmKeyInfo> deletedKeyInfo) throws IOException {
    retainedBlocks = Collections.emptySet();
    Boolean reclaimable = super.apply(deletedKeyInfo);
    // Like a reclaimable answer, the blocks to retain only hold if the chain did not change while processing the entry.
    if (!retainedBlocks.isEmpty()
        && !validateExistingLastNSnapshotsInChain(getVolumeName(deletedKeyInfo), getBucketName(deletedKeyInfo))) {
      retainedBlocks = Collections.emptySet();
    }
    return reclaimable;
  }

  /**
   * Tells which blocks of the key last rejected by {@link #apply} have to be retained when the key shares only some
   * of its blocks with the previous snapshot (an appended file). Every other block of that key is reclaimable.
   *
   * @return the blocks referenced by the previous snapshot, or an empty set if the last key was reclaimable or has to
   * be retained as a whole.
   */
  public synchronized Set<ContainerBlockID> getRetainedBlocks() {
    return retainedBlocks;
  }

  @Override
  protected String getVolumeName(Table.KeyValue<String, OmKeyInfo> keyValue) throws IOException {
    return keyValue.getValue().getVolumeName();
  }

  @Override
  protected String getBucketName(Table.KeyValue<String, OmKeyInfo> keyValue) throws IOException {
    return keyValue.getValue().getBucketName();
  }

  @Override
  /**
   * Determines whether a deleted key entry is reclaimable by checking its presence in prior snapshots.
   *
   * This method validates the existence of the deleted key in the previous snapshot's key table or file table.
   * If the key is not found in the previous snapshot, it is marked as reclaimable. Otherwise, additional checks
   * are performed using the previous-to-previous snapshot to confirm if the key is exclusively present in the
   * previous snapshot and accounted in the previous snapshot's exclusive size.
   *
   * @param deletedKeyInfo The key-value pair representing the deleted key information.
   * @return {@code true} if the key is reclaimable (not present in prior snapshots), {@code false} otherwise.
   * @throws IOException If an error occurs while accessing snapshot data or key information.
   */
  protected Boolean isReclaimable(Table.KeyValue<String, OmKeyInfo> deletedKeyInfo) throws IOException {
    UncheckedAutoCloseableSupplier<OmSnapshot> previousSnapshot = getPreviousOmSnapshot(1);


    KeyManager previousKeyManager = Optional.ofNullable(previousSnapshot)
        .map(i -> i.get().getKeyManager()).orElse(null);


    // Getting keyInfo from prev snapshot's keyTable/fileTable
    CheckedSupplier<Optional<OmKeyInfo>, IOException> previousKeyInfo =
        MemoizedCheckedSupplier.valueOf(() -> getPreviousSnapshotKeyInfo(getVolumeId(), getBucketInfo(),
            deletedKeyInfo.getValue(), getKeyManager(), previousKeyManager));
    // If file not present in previous snapshot then it won't be present in previous to previous snapshot either.
    if (!previousKeyInfo.get().isPresent()) {
      return true;
    }
    // ponytail: only the previous snapshot is consulted, not every snapshot of the bucket as the append design
    // proposes. This entry was deleted after the previous snapshot was taken, append only adds blocks and released
    // blocks are never attached to a file again, so a block of this key referenced by an older snapshot is also
    // referenced by the previous snapshot's version of the key. Walking the whole chain is defense in depth still to
    // be done.
    if (!isBlockLocationInfoSame(previousKeyInfo.get().get(), deletedKeyInfo.getValue())) {
      retainedBlocks = getContainerBlockIds(previousKeyInfo.get().get());
    }

    UncheckedAutoCloseableSupplier<OmSnapshot> previousToPreviousSnapshot = getPreviousOmSnapshot(0);
    KeyManager previousToPreviousKeyManager = Optional.ofNullable(previousToPreviousSnapshot)
        .map(i -> i.get().getKeyManager()).orElse(null);

    // Getting keyInfo from prev to prev snapshot's keyTable/fileTable based on keyInfo of prev keyTable
    CheckedSupplier<Optional<OmKeyInfo>, IOException> previousPrevKeyInfo =
        MemoizedCheckedSupplier.valueOf(() -> getPreviousSnapshotKeyInfo(
            getVolumeId(), getBucketInfo(), previousKeyInfo.get().orElse(null), previousKeyManager,
            previousToPreviousKeyManager));
    SnapshotInfo previousSnapshotInfo = getPreviousSnapshotInfo(1);
    calculateExclusiveSize(previousSnapshotInfo, previousKeyInfo, previousPrevKeyInfo,
        exclusiveSizeMap, exclusiveReplicatedSizeMap);
    return false;
  }

  public Map<UUID, Long> getExclusiveSizeMap() {
    return exclusiveSizeMap;
  }

  public Map<UUID, Long> getExclusiveReplicatedSizeMap() {
    return exclusiveReplicatedSizeMap;
  }

  /**
   * To calculate Exclusive Size for current snapshot, Check
   * the next snapshot deletedTable if the deleted key is
   * referenced in current snapshot and not referenced in the
   * previous snapshot then that key is exclusive to the current
   * snapshot. Here since we are only iterating through
   * deletedTable we can check the previous and previous to
   * previous snapshot to achieve the same.
   * previousSnapshot - Snapshot for which exclusive size is
   *                    getting calculating.
   * currSnapshot - Snapshot's deletedTable is used to calculate
   *                previousSnapshot snapshot's exclusive size.
   * previousToPrevSnapshot - Snapshot which is used to check
   *                 if key is exclusive to previousSnapshot.
   */
  private void calculateExclusiveSize(SnapshotInfo previousSnapshotInfo,
                                      CheckedSupplier<Optional<OmKeyInfo>, IOException> keyInfoPrevSnapshot,
                                      CheckedSupplier<Optional<OmKeyInfo>, IOException> keyInfoPrevToPrevSnapshot,
                                      Map<UUID, Long> exclusiveSizes, Map<UUID, Long> exclusiveReplicatedSizes)
      throws IOException {
    if (keyInfoPrevSnapshot.get().isPresent() && !keyInfoPrevToPrevSnapshot.get().isPresent()) {
      OmKeyInfo keyInfo = keyInfoPrevSnapshot.get().get();
      exclusiveSizes.compute(previousSnapshotInfo.getSnapshotId(),
          (k, v) -> (v == null ? 0 : v) + keyInfo.getDataSize());
      exclusiveReplicatedSizes.compute(previousSnapshotInfo.getSnapshotId(),
          (k, v) -> (v == null ? 0 : v) + keyInfo.getReplicatedSize());
    }
  }

  private Optional<OmKeyInfo> getPreviousSnapshotKeyInfo(long volumeId, OmBucketInfo bucketInfo,
                                                         OmKeyInfo keyInfo, KeyManager keyManager,
                                                         KeyManager previousKeyManager) throws IOException {
    if (keyInfo == null || previousKeyManager == null) {
      return Optional.empty();
    }
    OmKeyInfo prevKeyInfo = keyManager.getPreviousSnapshotOzoneKeyInfo(volumeId, bucketInfo, keyInfo)
        .apply(previousKeyManager);

    // Check if objectIds are matching then the keys are the same.
    if (prevKeyInfo == null || prevKeyInfo.getObjectID() != keyInfo.getObjectID()) {
      return Optional.empty();
    }
    // A version sharing only some blocks with the previous snapshot (an appended file) is not reclaimable as a whole,
    // see getRetainedBlocks().
    // ponytail: whole version exclusive size, a previous version sharing any block with the one before it counts as
    // not exclusive, so its unshared suffix is left out; per block exclusive size is AP-31.
    return isBlockLocationInfoSame(prevKeyInfo, keyInfo) || hasSharedBlocks(prevKeyInfo, keyInfo)
        ? Optional.of(prevKeyInfo) : Optional.empty();
  }
}
