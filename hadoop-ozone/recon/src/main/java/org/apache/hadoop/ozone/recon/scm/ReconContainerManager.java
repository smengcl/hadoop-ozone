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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.FINALIZE;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaNotFoundException;
import org.apache.hadoop.hdds.scm.container.SCMContainerManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.persistence.ContainerSchemaManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon's overriding implementation of SCM's Container Manager.
 */
public class ReconContainerManager extends SCMContainerManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconContainerManager.class);
  private StorageContainerServiceProvider scmClient;
  private ContainerSchemaManager containerSchemaManager;

  // Container ID -> Datanode UUID -> Timestamp
  private final Map<Long, Map<UUID, ContainerReplicaTimestamp>>
      containerReplicaLastSeenMap;

  /**
   * Constructs a mapping class that creates mapping between container names
   * and pipelines.
   * <p>
   * passed to LevelDB and this memory is allocated in Native code space.
   * CacheSize is specified
   * in MB.
   *
   * @throws IOException on Failure.
   */
  public ReconContainerManager(
      ConfigurationSource conf,
      Table<ContainerID, ContainerInfo> containerStore,
      DBStore batchHandler,
      PipelineManager pipelineManager,
      StorageContainerServiceProvider scm,
      ContainerSchemaManager containerSchemaManager) throws IOException {
    super(conf, containerStore, batchHandler, pipelineManager);
    this.scmClient = scm;
    this.containerSchemaManager = containerSchemaManager;
    this.containerReplicaLastSeenMap = new ConcurrentHashMap<>();
    containerSchemaManager.setLastSeenMap(containerReplicaLastSeenMap);
    containerSchemaManager.setScmDBStore(batchHandler);
  }

  /**
   * Check and add new container if not already present in Recon.
   *
   * @param containerID     containerID to check.
   * @param datanodeDetails Datanode from where we got this container.
   * @throws IOException on Error.
   */
  public void checkAndAddNewContainer(ContainerID containerID,
      ContainerReplicaProto.State replicaState,
      DatanodeDetails datanodeDetails)
      throws IOException {
    if (!exists(containerID)) {
      LOG.info("New container {} got from {}.", containerID,
          datanodeDetails.getHostName());
      ContainerWithPipeline containerWithPipeline =
          scmClient.getContainerWithPipeline(containerID.getId());
      LOG.debug("Verified new container from SCM {}, {} ",
          containerID, containerWithPipeline.getPipeline().getId());
      // If no other client added this, go ahead and add this container.
      if (!exists(containerID)) {
        addNewContainer(containerID.getId(), containerWithPipeline);
      }
    } else {
      // Check if container state is not open. In SCM, container state
      // changes to CLOSING first, and then the close command is pushed down
      // to Datanodes. Recon 'learns' this from DN, and hence replica state
      // will move container state to 'CLOSING'.
      ContainerInfo containerInfo = getContainer(containerID);
      if (containerInfo.getState().equals(HddsProtos.LifeCycleState.OPEN)
          && !replicaState.equals(ContainerReplicaProto.State.OPEN)
          && isHealthy(replicaState)) {
        LOG.info("Container {} has state OPEN, but Replica has State {}.",
            containerID, replicaState);
        updateContainerState(containerID, FINALIZE);
      }
    }
  }

  private boolean isHealthy(ContainerReplicaProto.State replicaState) {
    return replicaState != ContainerReplicaProto.State.UNHEALTHY
        && replicaState != ContainerReplicaProto.State.INVALID
        && replicaState != ContainerReplicaProto.State.DELETED;
  }

  /**
   * Adds a new container to Recon's container manager.
   * @param containerId id
   * @param containerWithPipeline containerInfo with pipeline info
   * @throws IOException on Error.
   */
  public void addNewContainer(long containerId,
                              ContainerWithPipeline containerWithPipeline)
      throws IOException {
    ContainerInfo containerInfo = containerWithPipeline.getContainerInfo();
    getLock().lock();
    try {
      boolean success = false;
      if (containerInfo.getState().equals(HddsProtos.LifeCycleState.OPEN)) {
        PipelineID pipelineID = containerWithPipeline.getPipeline().getId();
        if (getPipelineManager().containsPipeline(pipelineID)) {
          getContainerStateManager().addContainerInfo(containerId,
              containerInfo, getPipelineManager(),
              containerWithPipeline.getPipeline());
          success = true;
        } else {
          // Get open container for a pipeline that Recon does not know
          // about yet. Cannot update internal state until pipeline is synced.
          LOG.warn(String.format(
              "Pipeline %s not found. Cannot add container %s",
              pipelineID, containerInfo.containerID()));
        }
      } else {
        // Non 'Open' Container. No need to worry about pipeline since SCM
        // returns a random pipelineID.
        getContainerStateManager().addContainerInfo(containerId,
            containerInfo, getPipelineManager(), null);
        success = true;
      }
      if (success) {
        addContainerToDB(containerInfo);
        LOG.info("Successfully added container {} to Recon.",
            containerInfo.containerID());
      }
    } catch (IOException ex) {
      LOG.info("Exception while adding container {} .",
          containerInfo.containerID(), ex);
      getPipelineManager().removeContainerFromPipeline(
          containerInfo.getPipelineID(),
          new ContainerID(containerInfo.getContainerID()));
      throw ex;
    } finally {
      getLock().unlock();
    }
  }

  /**
   * Add a container Replica for given DataNode.
   */
  @Override
  public void updateContainerReplica(ContainerID containerID,
      ContainerReplica replica)
      throws ContainerNotFoundException {
    super.updateContainerReplica(containerID, replica);

    final long currTime = System.currentTimeMillis();
    final long id = containerID.getId();
    final DatanodeDetails dnInfo = replica.getDatanodeDetails();
    final UUID uuid = dnInfo.getUuid();

    // Map from DataNode UUID to replica last seen time
    final Map<UUID, ContainerReplicaTimestamp> replicaLastSeenMap =
        containerReplicaLastSeenMap.get(id);

    boolean flushToDB = false;

    // If replica doesn't exist in in-memory map, add to DB and add to map
    if (replicaLastSeenMap == null) {
      // putIfAbsent to avoid TOCTOU
      containerReplicaLastSeenMap.putIfAbsent(id,
          new ConcurrentHashMap<UUID, ContainerReplicaTimestamp>() {{
            put(uuid, new ContainerReplicaTimestamp(uuid, currTime, currTime));
          }});
      flushToDB = true;
    } else {
      // ContainerID exists, update timestamp in memory
      final ContainerReplicaTimestamp ts = replicaLastSeenMap.get(uuid);
      if (ts == null) {
        // New Datanode
        replicaLastSeenMap.put(uuid,
            new ContainerReplicaTimestamp(uuid, currTime, currTime));
        flushToDB = true;
      } else {
        // if the object exists, only update the last seen time field
        ts.setLastSeenTime(currTime);
      }
    }

    if (flushToDB) {
      containerSchemaManager.upsertContainerHistory(id, uuid, currTime);
    }
  }

  /**
   * Remove a Container Replica of a given DataNode.
   */
  @Override
  public void removeContainerReplica(ContainerID containerID,
      ContainerReplica replica) throws ContainerNotFoundException,
      ContainerReplicaNotFoundException {
    super.removeContainerReplica(containerID, replica);

    final long id = containerID.getId();
    final DatanodeDetails dnInfo = replica.getDatanodeDetails();
    final UUID uuid = dnInfo.getUuid();

    final Map<UUID, ContainerReplicaTimestamp> replicaLastSeenMap =
        containerReplicaLastSeenMap.get(id);
    if (replicaLastSeenMap != null) {
      final ContainerReplicaTimestamp ts = replicaLastSeenMap.get(uuid);
      if (ts != null) {
        // Flush to DB, then remove from in-memory map
        containerSchemaManager.upsertContainerHistory(id, uuid,
            ts.getLastSeenTime());
        replicaLastSeenMap.remove(uuid);
      }
    }
  }

  @VisibleForTesting
  public ContainerSchemaManager getContainerSchemaManager() {
    return containerSchemaManager;
  }

  /**
   * Flush the container replica history in-memory map to DB.
   * Expected to be called on Recon graceful shutdown.
   * @param clearMap true to clear the in-memory map after flushing completes.
   */
  public void flushLastSeenMapToDB(boolean clearMap) {
    containerSchemaManager.flushLastSeenMapToDB(clearMap);
  }
}
