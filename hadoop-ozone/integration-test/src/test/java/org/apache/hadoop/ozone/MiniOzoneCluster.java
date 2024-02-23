/**
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
package org.apache.hadoop.ozone;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdds.DatanodeVersion;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.recon.ReconServer;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.ExitUtils;

/**
 * Interface used for MiniOzoneClusters.
 */
public interface MiniOzoneCluster extends AutoCloseable {

  /**
   * Returns the Builder to construct MiniOzoneCluster.
   *
   * @param conf OzoneConfiguration
   *
   * @return MiniOzoneCluster builder
   */
  static Builder newBuilder(OzoneConfiguration conf) {
    return new MiniOzoneClusterImpl.Builder(conf);
  }

  /**
   * Returns the Builder to construct MiniOzoneHACluster.
   *
   * @param conf OzoneConfiguration
   *
   * @return MiniOzoneCluster builder
   */
  static Builder newOMHABuilder(OzoneConfiguration conf) {
    return new MiniOzoneHAClusterImpl.Builder(conf);
  }

  static Builder newHABuilder(OzoneConfiguration conf) {
    return new MiniOzoneHAClusterImpl.Builder(conf);
  }

  /**
   * Returns the configuration object associated with the MiniOzoneCluster.
   *
   * @return Configuration
   */
  OzoneConfiguration getConf();

  /**
   * Set the configuration for the MiniOzoneCluster.
   */
  void setConf(OzoneConfiguration newConf);

  /**
   * Waits for the cluster to be ready, this call blocks till all the
   * configured {@link HddsDatanodeService} registers with
   * {@link StorageContainerManager}.
   *
   * @throws TimeoutException In case of timeout
   * @throws InterruptedException In case of interrupt while waiting
   */
  void waitForClusterToBeReady() throws TimeoutException, InterruptedException;

  /**
   * Waits for at least one RATIS pipeline of given factor to be reported in open
   * state.
   *
   * @param factor replication factor
   * @param timeoutInMs timeout value in milliseconds
   * @throws TimeoutException In case of timeout
   * @throws InterruptedException In case of interrupt while waiting
   */
  void waitForPipelineTobeReady(HddsProtos.ReplicationFactor factor,
                                int timeoutInMs)
          throws TimeoutException, InterruptedException;

  /**
   * Sets the timeout value after which
   * {@link MiniOzoneCluster#waitForClusterToBeReady} times out.
   *
   * @param timeoutInMs timeout value in milliseconds
   */
  void setWaitForClusterToBeReadyTimeout(int timeoutInMs);

  /**
   * Waits/blocks till the cluster is out of safe mode.
   *
   * @throws TimeoutException TimeoutException In case of timeout
   * @throws InterruptedException In case of interrupt while waiting
   */
  void waitTobeOutOfSafeMode() throws TimeoutException, InterruptedException;

  /**
   * Returns {@link StorageContainerManager} associated with this
   * {@link MiniOzoneCluster} instance.
   *
   * @return {@link StorageContainerManager} instance
   */
  StorageContainerManager getStorageContainerManager();

  /**
   * Returns {@link OzoneManager} associated with this
   * {@link MiniOzoneCluster} instance.
   *
   * @return {@link OzoneManager} instance
   */
  OzoneManager getOzoneManager();

  /**
   * Returns the list of {@link HddsDatanodeService} which are part of this
   * {@link MiniOzoneCluster} instance.
   *
   * @return List of {@link HddsDatanodeService}
   */
  List<HddsDatanodeService> getHddsDatanodes();

  HddsDatanodeService getHddsDatanode(DatanodeDetails dn) throws IOException;

  /**
   * Returns a {@link ReconServer} instance.
   *
   * @return List of {@link ReconServer}
   */
  ReconServer getReconServer();

  /**
   * Returns an {@link OzoneClient} to access the {@link MiniOzoneCluster}.
   * The caller is responsible for closing the client after use.
   *
   * @return {@link OzoneClient}
   */
  OzoneClient newClient() throws IOException;

  /**
   * Returns StorageContainerLocationClient to communicate with
   * {@link StorageContainerManager} associated with the MiniOzoneCluster.
   */
  StorageContainerLocationProtocolClientSideTranslatorPB
      getStorageContainerLocationClient() throws IOException;

  /**
   * Restarts StorageContainerManager instance.
   */
  void restartStorageContainerManager(boolean waitForDatanode)
      throws InterruptedException, TimeoutException, IOException,
      AuthenticationException;

  /**
   * Restarts OzoneManager instance.
   */
  void restartOzoneManager() throws IOException;

  /**
   * Restarts Recon instance.
   */
  void restartReconServer() throws Exception;

  /**
   * Restart a particular HddsDatanode.
   *
   * @param i index of HddsDatanode in the MiniOzoneCluster
   */
  void restartHddsDatanode(int i, boolean waitForDatanode)
      throws InterruptedException, TimeoutException;

  int getHddsDatanodeIndex(DatanodeDetails dn) throws IOException;

  /**
   * Restart a particular HddsDatanode.
   *
   * @param dn HddsDatanode in the MiniOzoneCluster
   */
  void restartHddsDatanode(DatanodeDetails dn, boolean waitForDatanode)
      throws InterruptedException, TimeoutException, IOException;
  /**
   * Shutdown a particular HddsDatanode.
   *
   * @param i index of HddsDatanode in the MiniOzoneCluster
   */
  void shutdownHddsDatanode(int i);

  /**
   * Shutdown a particular HddsDatanode.
   *
   * @param dn HddsDatanode in the MiniOzoneCluster
   */
  void shutdownHddsDatanode(DatanodeDetails dn) throws IOException;

  /**
   * Start Recon.
   */
  void startRecon();

  /**
   * Stop Recon.
   */
  void stopRecon();

  /**
   * Shutdown the MiniOzoneCluster and delete the storage dirs.
   */
  void shutdown();

  default void close() {
    shutdown();
  }

  /**
   * Stop the MiniOzoneCluster without any cleanup.
   */
  void stop();

  /**
   * Start DataNodes.
   */
  void startHddsDatanodes();

  /**
   * Shuts down all the DataNodes.
   */
  void shutdownHddsDatanodes();

  String getClusterId();

  default String getName() {
    return getClass().getSimpleName() + "-" + getClusterId();
  }

  default String getBaseDir() {
    return GenericTestUtils.getTempPath(getName());
  }

  /**
   * Builder class for MiniOzoneCluster.
   */
  @SuppressWarnings("visibilitymodifier")
  abstract class Builder {

    protected static final int ACTIVE_OMS_NOT_SET = -1;
    protected static final int ACTIVE_SCMS_NOT_SET = -1;
    protected static final int DEFAULT_RATIS_RPC_TIMEOUT_SEC = 1;

    protected OzoneConfiguration conf;
    protected String path;

    protected String clusterId;
    protected String omServiceId;
    protected int numOfOMs;
    protected int numOfActiveOMs = ACTIVE_OMS_NOT_SET;

    protected String scmServiceId;
    protected int numOfSCMs;
    protected int numOfActiveSCMs = ACTIVE_SCMS_NOT_SET;
    protected SCMConfigurator scmConfigurator;

    protected String scmId = UUID.randomUUID().toString();
    protected String omId = UUID.randomUUID().toString();

    protected Optional<String> datanodeReservedSpace = Optional.empty();
    protected boolean includeRecon = false;

    protected Optional<Integer> omLayoutVersion = Optional.empty();
    protected Optional<Integer> scmLayoutVersion = Optional.empty();
    protected Optional<Integer> dnLayoutVersion = Optional.empty();

    protected int dnInitialVersion = DatanodeVersion.FUTURE_VERSION.toProtoValue();
    protected int dnCurrentVersion = DatanodeVersion.FUTURE_VERSION.toProtoValue();

    // Use relative smaller number of handlers for testing
    protected int numOfOmHandlers = 20;
    protected int numOfScmHandlers = 20;
    protected int numOfDatanodes = 3;
    protected int numDataVolumes = 1;
    protected boolean  startDataNodes = true;
    protected CertificateClient certClient;
    protected SecretKeyClient secretKeyClient;

    protected Builder(OzoneConfiguration conf) {
      this.conf = conf;
      setClusterId(UUID.randomUUID().toString());
      // Use default SCM configurations if no override is provided.
      setSCMConfigurator(new SCMConfigurator());
      ExitUtils.disableSystemExit();
    }

    public Builder setConf(OzoneConfiguration config) {
      this.conf = config;
      return this;
    }

    public Builder setSCMConfigurator(SCMConfigurator configurator) {
      this.scmConfigurator = configurator;
      return this;
    }

    /**
     * Sets the cluster Id.
     *
     * @param id cluster Id
     */
    void setClusterId(String id) {
      clusterId = id;
      path = GenericTestUtils.getTempPath(
          MiniOzoneClusterImpl.class.getSimpleName() + "-" + clusterId);
    }

    /**
     * For tests that do not use any features of SCM, we can get by with
     * 0 datanodes.  Also need to skip safemode in this case.
     * This allows the cluster to come up much faster.
     */
    public Builder withoutDatanodes() {
      setNumDatanodes(0);
      conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED, false);
      return this;
    }

    public Builder setStartDataNodes(boolean nodes) {
      this.startDataNodes = nodes;
      return this;
    }

    public Builder setCertificateClient(CertificateClient client) {
      this.certClient = client;
      return this;
    }

    public Builder setSecretKeyClient(SecretKeyClient client) {
      this.secretKeyClient = client;
      return this;
    }

    /**
     * Sets the number of HddsDatanodes to be started as part of
     * MiniOzoneCluster.
     *
     * @param val number of datanodes
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder setNumDatanodes(int val) {
      numOfDatanodes = val;
      return this;
    }

    /**
     * Set the initialVersion for all datanodes.
     *
     * @param val initialVersion value to be set for all datanodes.
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder setDatanodeInitialVersion(int val) {
      dnInitialVersion = val;
      return this;
    }

    /**
     * Set the currentVersion for all datanodes.
     *
     * @param val currentVersion value to be set for all datanodes.
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder setDatanodeCurrentVersion(int val) {
      dnCurrentVersion = val;
      return this;
    }

    /**
     * Sets the number of data volumes per datanode.
     *
     * @param val number of volumes per datanode.
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder setNumDataVolumes(int val) {
      numDataVolumes = val;
      return this;
    }

    /**
     * Sets the reserved space
     * {@link org.apache.hadoop.hdds.scm.ScmConfigKeys}
     * HDDS_DATANODE_DIR_DU_RESERVED
     * for each volume in each datanode.
     * @param reservedSpace String that contains the numeric size value and
     *                      ends with a
     *                      {@link org.apache.hadoop.hdds.conf.StorageUnit}
     *                      suffix. For example, "50GB".
     * @see org.apache.hadoop.ozone.container.common.volume.VolumeInfo
     *
     * @return {@link MiniOzoneCluster} Builder
     */
    public Builder setDatanodeReservedSpace(String reservedSpace) {
      datanodeReservedSpace = Optional.of(reservedSpace);
      return this;
    }

    public Builder setNumOfOzoneManagers(int numOMs) {
      this.numOfOMs = numOMs;
      return this;
    }

    public Builder setNumOfActiveOMs(int numActiveOMs) {
      this.numOfActiveOMs = numActiveOMs;
      return this;
    }

    public Builder setOMServiceId(String serviceId) {
      this.omServiceId = serviceId;
      return this;
    }

    public Builder includeRecon(boolean include) {
      this.includeRecon = include;
      return this;
    }

    public Builder setNumOfStorageContainerManagers(int numSCMs) {
      this.numOfSCMs = numSCMs;
      return this;
    }

    public Builder setNumOfActiveSCMs(int numActiveSCMs) {
      this.numOfActiveSCMs = numActiveSCMs;
      return this;
    }

    public Builder setSCMServiceId(String serviceId) {
      this.scmServiceId = serviceId;
      return this;
    }

    public Builder setDnLayoutVersion(int layoutVersion) {
      dnLayoutVersion = Optional.of(layoutVersion);
      return this;
    }

    /**
     * Constructs and returns MiniOzoneCluster.
     *
     * @return {@link MiniOzoneCluster}
     */
    public abstract MiniOzoneCluster build() throws IOException;
  }
}
