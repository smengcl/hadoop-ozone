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

package org.apache.hadoop.ozone.om;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT;
import static org.apache.ozone.test.OzoneTestBase.uniqueObjectName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.primitives.Bytes;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdfs.LogVerificationAppender;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.AppendConflictException;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.OMHAMetrics;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.service.KeyDeletingService;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendFileResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionKey;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenewAppendLeasesRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.LoggerFactory;

/**
 * Ozone Manager HA tests that stop/restart one or more OM nodes.
 * @see TestOzoneManagerHAWithAllRunning
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestOzoneManagerHAWithStoppedNodes extends OzoneManagerHATests {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      TestOzoneManagerHAWithStoppedNodes.class);

  static {
    setExtraClusterConfig(c -> c.set(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, "2s"));
  }

  /**
   * After restarting OMs we need to wait
   * for a leader to be elected and ready.
   */
  @BeforeEach
  void setup() throws Exception {
    waitForLeaderToBeReady();
  }

  /**
   * Restart all OMs after each test.
   */
  @AfterEach
  void resetCluster() throws Exception {
    MiniOzoneHAClusterImpl cluster = getCluster();
    if (cluster != null) {
      cluster.restartOzoneManager();
    }
  }

  /**
   * Test client request succeeds when one OM node is down.
   */
  @Test
  void oneOMDown() throws Exception {
    getCluster().stopOzoneManager(1);
    waitForLeaderToBeReady();

    createVolumeTest(true);
    createKeyTest(true);
  }

  /**
   * Test client request fails when 2 OMs are down.
   */
  @Test
  void twoOMDown() throws Exception {
    getCluster().stopOzoneManager(1);
    getCluster().stopOzoneManager(2);

    createVolumeTest(false);
    createKeyTest(false);
  }

  @Test
  void testMultipartUpload() throws Exception {

    // Happy scenario when all OM's are up.
    OzoneBucket ozoneBucket = setupBucket();

    String keyName = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(ozoneBucket, keyName);

    createMultipartKeyAndReadKey(ozoneBucket, keyName, uploadID);

    testMultipartUploadWithOneOmNodeDown();
  }

  private void testMultipartUploadWithOneOmNodeDown() throws Exception {

    OzoneBucket ozoneBucket = setupBucket();

    String keyName = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(ozoneBucket, keyName);

    // After initiate multipartupload, shutdown leader OM.
    // Stop leader OM, to see when the OM leader changes
    // multipart upload is happening successfully or not.

    final HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> omFailoverProxyProvider
        = OmTestUtil.getFailoverProxyProvider(getObjectStore());

    // The omFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // Stop one of the ozone manager, to see when the OM leader changes
    // multipart upload is happening successfully or not.
    getCluster().stopOzoneManager(leaderOMNodeId);
    waitForLeaderToBeReady();

    createMultipartKeyAndReadKey(ozoneBucket, keyName, uploadID);

    String newLeaderOMNodeId =
        omFailoverProxyProvider.getCurrentProxyOMNodeId();

    assertNotEquals(leaderOMNodeId, newLeaderOMNodeId);
  }

  private String initiateMultipartUpload(OzoneBucket ozoneBucket,
      String keyName) throws Exception {

    OmMultipartInfo omMultipartInfo =
        ozoneBucket.initiateMultipartUpload(keyName,
            ReplicationType.RATIS,
            ReplicationFactor.ONE);

    String uploadID = omMultipartInfo.getUploadID();
    assertNotNull(uploadID);
    return uploadID;
  }

  private void createMultipartKeyAndReadKey(OzoneBucket ozoneBucket,
      String keyName, String uploadID) throws Exception {

    String value = "random data";
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createMultipartKey(
        keyName, value.length(), 1, uploadID);
    ozoneOutputStream.write(value.getBytes(UTF_8), 0, value.length());
    ozoneOutputStream.getMetadata().put(OzoneConsts.ETAG, DigestUtils.md5Hex(value));
    ozoneOutputStream.close();


    Map<Integer, String> partsMap = new HashMap<>();
    partsMap.put(1, ozoneOutputStream.getCommitUploadPartInfo().getETag());
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo =
        ozoneBucket.completeMultipartUpload(keyName, uploadID, partsMap);

    assertNotNull(omMultipartUploadCompleteInfo);
    assertNotNull(omMultipartUploadCompleteInfo.getHash());


    try (OzoneInputStream ozoneInputStream = ozoneBucket.readKey(keyName)) {
      byte[] fileContent = new byte[value.getBytes(UTF_8).length];
      IOUtils.readFully(ozoneInputStream, fileContent);
      assertEquals(value, new String(fileContent, UTF_8));
    }
  }

  /**
   * Test HadoopRpcOMFailoverProxyProvider failover on connection exception
   * to OM client.
   */
  @Test
  public void testOMProxyProviderFailoverOnConnectionFailure()
      throws Exception {
    ObjectStore objectStore = getObjectStore();
    final HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> omFailoverProxyProvider
        = OmTestUtil.getFailoverProxyProvider(objectStore);

    createVolumeTest(true);
    String firstProxyNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // On stopping the current OM Proxy, the next connection attempt should
    // failover to a another OM proxy.
    getCluster().stopOzoneManager(firstProxyNodeId);
    waitForLeaderToBeReady();

    // Next request to the proxy provider should result in a failover
    createVolumeTest(true);
    GenericTestUtils.waitFor(
        () -> !firstProxyNodeId.equals(omFailoverProxyProvider.getCurrentProxyOMNodeId()),
        100, (int) (OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT * 5));

    // Get the new OM Proxy NodeId
    String newProxyNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    // Verify that a failover occurred. the new proxy nodeId should be
    // different from the old proxy nodeId.
    assertNotEquals(firstProxyNodeId, newProxyNodeId);
  }

  @Test
  @Order(Integer.MAX_VALUE)
  void testOMRestart() throws Exception {
    // start fresh cluster, with small log segments: only closed segments are purged, and without a purge the
    // restarted OM gets the log entries it missed instead of a snapshot
    shutdown();
    setExtraClusterConfig(c -> {
      c.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_KEY, 16, StorageUnit.KB);
      c.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY, 16, StorageUnit.KB);
    });
    init();

    ObjectStore objectStore = getObjectStore();
    // Get the leader OM
    final String leaderOMNodeId = OmTestUtil.getCurrentOmProxyNodeId(objectStore);
    OzoneManager leaderOM = getCluster().getOzoneManager(leaderOMNodeId);

    // Get follower OM
    OzoneManager followerOM1 = getCluster().getOzoneManager(
        leaderOM.getPeerNodes().get(0).getNodeId());

    // Do some transactions so that the log index increases
    String userName = "user" + RandomStringUtils.secure().nextNumeric(5);
    String adminName = "admin" + RandomStringUtils.secure().nextNumeric(5);
    String volumeName = uniqueObjectName("volume");
    String bucketName = uniqueObjectName("bucket");

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);

    ozoneVolume.createBucket(bucketName);
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      createKey(ozoneBucket);
    }

    final long followerOM1LastAppliedIndex =
        followerOM1.getOmRatisServer().getLastAppliedTermIndex().getIndex();

    // Stop one follower OM
    followerOM1.stop();

    // An append session that the stopped OM only learns about from the installed snapshot.
    OzoneClient appendClient = newAppendClient();
    OzoneBucket fsoBucket = createFsoBucket(appendClient, volumeName);
    String appendKey = createReplicatedKey(fsoBucket);
    OzoneOutputStream appendStream = fsoBucket.appendFile(appendKey);
    appendStream.write("synced".getBytes(UTF_8));
    appendStream.hsync();
    long sessionId = fsoBucket.getFileStatus(appendKey).getKeyInfo().getAppendOwnerSessionId();

    // Do more transactions. Stopped OM should miss these transactions and
    // the logs corresponding to at least some missed transactions
    // should be purged. This will force the OM to install snapshot when
    // restarted.
    long minNewTxIndex = followerOM1LastAppliedIndex + getLogPurgeGap() * 10L;
    while (leaderOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
        < minNewTxIndex) {
      createKey(ozoneBucket);
    }

    // Get the latest snapshotIndex from the leader OM.
    final long leaderOMSnaphsotIndex = leaderOM.getRatisSnapshotIndex();

    // The stopped OM should be lagging behind the leader OM.
    assertThat(followerOM1LastAppliedIndex).isLessThan(leaderOMSnaphsotIndex);

    // Restart the stopped OM.
    LogCapturer logCapture = LogCapturer.captureLogs(OzoneManager.class);
    followerOM1.restart();

    // Wait for the follower OM to catch up
    GenericTestUtils.waitFor(() -> followerOM1.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex() >= leaderOMSnaphsotIndex,
        100, 200000);
    GenericTestUtils.waitFor(() -> logCapture.getOutput().contains("Install Checkpoint is finished"), 100, 30000);

    // Do more transactions. The restarted OM should receive the
    // new transactions. It's last applied tx index should increase from the
    // last snapshot index after more transactions are applied.
    for (int i = 0; i < 10; i++) {
      createKey(ozoneBucket);
    }

    GenericTestUtils.waitFor(() -> followerOM1.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex() > leaderOMSnaphsotIndex,
        100, 200000);

    final long followerOM1LastAppliedIndexNew =
        followerOM1.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    assertThat(followerOM1LastAppliedIndexNew).isGreaterThan(leaderOMSnaphsotIndex);

    // The session index of the restarted OM was built from the installed DB, and the close finds the session there.
    waitForApplied(followerOM1, leaderOM);
    assertThat(appendOpenRecord(followerOM1, fsoBucket, sessionId).getAppendSession().isActive()).isTrue();
    assertThat(committedSummary(followerOM1, fsoBucket, appendKey)).startsWith("owner=" + sessionId)
        .isEqualTo(committedSummary(leaderOM, fsoBucket, appendKey));
    appendStream.write("closed".getBytes(UTF_8));
    appendStream.close();
    appendClient.close();
    waitForApplied(followerOM1, leaderOM);
    assertThat(appendOpenRecord(followerOM1, fsoBucket, sessionId)).isNull();
    assertThat(committedSummary(followerOM1, fsoBucket, appendKey)).startsWith("owner=null")
        .isEqualTo(committedSummary(leaderOM, fsoBucket, appendKey));
  }

  /**
   * An open append stream keeps allocating blocks, publishing and renewing its lease when leadership is transferred
   * and when the leader is lost, also when the new leader was down while the session was admitted.
   */
  @Test
  void testAppendAcrossFailover() throws Exception {
    MiniOzoneHAClusterImpl cluster = getCluster();
    byte[] afterTransfer = "after transfer".getBytes(UTF_8);
    byte[] afterLeaderLoss = "after leader loss".getBytes(UTF_8);
    byte[] beforeClose = "before close".getBytes(UTF_8);
    OzoneManager firstLeader = cluster.getOMLeader();
    OzoneManager downAtAdmission = cluster.getOzoneManager(firstLeader.getPeerNodes().get(0).getNodeId());
    long sessionId;
    OzoneBucket bucket;
    String key;
    byte[] prefix;

    try (OzoneClient appendClient = newAppendClient()) {
      bucket = createFsoBucket(appendClient, setupBucket().getVolumeName());
      key = createReplicatedKey(bucket);
      prefix = readKey(bucket, key);
      cluster.stopOzoneManager(downAtAdmission.getOMNodeId());
      try (OzoneOutputStream out = bucket.appendFile(key)) {
        sessionId = bucket.getFileStatus(key).getKeyInfo().getAppendOwnerSessionId();

        // The OM that missed the admission replays it from the Raft log and becomes the leader. The first block of
        // the session is allocated there.
        cluster.restartOzoneManager(downAtAdmission, true);
        waitForApplied(downAtAdmission, firstLeader);
        assertThat(appendOpenRecord(downAtAdmission, bucket, sessionId).getAppendSession().isActive()).isTrue();
        transferLeader(firstLeader, downAtAdmission);
        out.write(afterTransfer);
        out.hsync();
        assertThat(bucket.getKey(key).getDataSize()).isEqualTo(prefix.length + afterTransfer.length);
        long renewedAt = lastRenewedAt(downAtAdmission, bucket, sessionId);
        GenericTestUtils.waitFor(() -> lastRenewedAt(downAtAdmission, bucket, sessionId) > renewedAt, 100, 30000);
        assertThrows(AppendConflictException.class, () -> appendClient.getObjectStore()
            .getVolume(bucket.getVolumeName()).getBucket(bucket.getName()).appendFile(key));

        cluster.stopOzoneManager(downAtAdmission.getOMNodeId());
        waitForLeaderToBeReady();
        out.write(afterLeaderLoss);
        out.hsync();
        assertThat(bucket.getKey(key).getDataSize())
            .isEqualTo(prefix.length + afterTransfer.length + afterLeaderLoss.length);
        out.write(beforeClose);
      }
      assertThat(readKey(bucket, key)).isEqualTo(Bytes.concat(prefix, afterTransfer, afterLeaderLoss, beforeClose));
    }

    // The OM that missed the second half of the session catches up, and all OMs hold the same closed file.
    cluster.restartOzoneManager(downAtAdmission, true);
    OzoneManager leader = cluster.waitForLeaderOM();
    for (OzoneManager om : cluster.getOzoneManagersList()) {
      waitForApplied(om, leader);
      assertThat(appendOpenRecord(om, bucket, sessionId)).isNull();
      assertThat(committedSummary(om, bucket, key)).startsWith("owner=null")
          .isEqualTo(committedSummary(leader, bucket, key));
    }
  }

  /**
   * An admission, an hsync, a renewal and a close whose response was lost are retried with the same client ID and
   * call ID. The retry is answered from the Ratis retry cache, also by a new leader, and is not applied again.
   */
  @Test
  void testAppendRetryCache() throws Exception {
    MiniOzoneHAClusterImpl cluster = getCluster();
    enableAppend();
    OzoneBucket bucket = createFsoBucket(getClient(), setupBucket().getVolumeName());
    String key = createReplicatedKey(bucket);
    long prefixLength = bucket.getKey(key).getDataSize();
    ClientId clientId = ClientId.randomId();
    OzoneManager omLeader = cluster.getOMLeader();
    OzoneManagerProtocolProtos.KeyArgs keyArgs = OzoneManagerProtocolProtos.KeyArgs.newBuilder()
        .setVolumeName(bucket.getVolumeName()).setBucketName(bucket.getName()).setKeyName(key).build();

    // A second admission would be a conflict with the session of the first one.
    OMRequest appendFile = newRequest(Type.AppendFile, clientId)
        .setAppendFileRequest(AppendFileRequest.newBuilder().setKeyArgs(keyArgs)).build();
    AppendFileResponse admitted = submit(omLeader, clientId, 10, appendFile).getAppendFileResponse();
    long sessionId = admitted.getID();
    assertThat(submit(omLeader, clientId, 10, appendFile).getAppendFileResponse()).isEqualTo(admitted);
    assertThat(committedSummary(omLeader, bucket, key)).startsWith("owner=" + sessionId);

    OmKeyLocationInfo block = getObjectStore().getClientProxy().getOzoneManagerClient().allocateBlock(
        new OmKeyArgs.Builder().setVolumeName(bucket.getVolumeName()).setBucketName(bucket.getName()).setKeyName(key)
            .setReplicationConfig(bucket.getKey(key).getReplicationConfig()).build(), sessionId, new ExcludeList());
    block.setLength(100);
    OzoneManagerProtocolProtos.KeyArgs suffix = keyArgs.toBuilder().setDataSize(prefixLength + block.getLength())
        .addKeyLocations(block.getProtobuf(ClientVersion.CURRENT_VERSION)).build();
    OMRequest hsync = newRequest(Type.CommitKey, clientId).setCommitKeyRequest(
        CommitKeyRequest.newBuilder().setKeyArgs(suffix).setClientID(sessionId).setHsync(true)).build();
    submit(omLeader, clientId, 11, hsync);
    String synced = committedSummary(omLeader, bucket, key);
    submit(omLeader, clientId, 11, hsync);
    // The summary has the update ID, which every applied commit sets to its transaction index.
    assertThat(committedSummary(omLeader, bucket, key)).contains("size=" + suffix.getDataSize()).isEqualTo(synced);

    // A renewal that was applied again would carry a later time.
    OMRequest renew = newRequest(Type.RenewAppendLeases, clientId).setRenewAppendLeasesRequest(
        RenewAppendLeasesRequest.newBuilder().addSessions(AppendSessionKey.newBuilder()
            .setVolumeName(bucket.getVolumeName()).setBucketName(bucket.getName()).setSessionId(sessionId))).build();
    assertThat(submit(omLeader, clientId, 12, renew).getRenewAppendLeasesResponse().getRenewedList())
        .containsExactly(true);
    long renewedAt = lastRenewedAt(omLeader, bucket, sessionId);
    Thread.sleep(10);
    assertThat(submit(omLeader, clientId, 12, renew).getRenewAppendLeasesResponse().getRenewedList())
        .containsExactly(true);
    assertThat(lastRenewedAt(omLeader, bucket, sessionId)).isEqualTo(renewedAt);

    // The other OMs built their retry cache while they applied the log as followers.
    OzoneManager newLeader = cluster.getOzoneManager(omLeader.getPeerNodes().get(0).getNodeId());
    transferLeader(omLeader, newLeader);
    cluster.shutdownOzoneManager(omLeader);

    // A second close would not find the session.
    OMRequest close = newRequest(Type.CommitKey, clientId).setCommitKeyRequest(
        CommitKeyRequest.newBuilder().setKeyArgs(suffix).setClientID(sessionId)).build();
    submit(newLeader, clientId, 13, close);
    String closed = committedSummary(newLeader, bucket, key);
    submit(newLeader, clientId, 13, close);
    // Late retries of the earlier calls neither admit another session nor publish again.
    assertThat(submit(newLeader, clientId, 10, appendFile).getAppendFileResponse()).isEqualTo(admitted);
    submit(newLeader, clientId, 11, hsync);
    assertThat(committedSummary(newLeader, bucket, key)).startsWith("owner=null").isEqualTo(closed);
    assertThat(appendOpenRecord(newLeader, bucket, sessionId)).isNull();
  }

  private static OMRequest.Builder newRequest(Type type, ClientId clientId) {
    return OMRequest.newBuilder().setCmdType(type).setVersion(ClientVersion.CURRENT_VERSION)
        .setClientId(clientId.toString());
  }

  /** Submits the request to the OM as the RPC call with the given ID of the client and expects it to succeed. */
  private static OMResponse submit(OzoneManager om, ClientId clientId, int callId, OMRequest request)
      throws Exception {
    Server.getCurCall().set(new Server.Call(callId, 0, null, null,
        RPC.RpcKind.RPC_BUILTIN, clientId.toByteString().toByteArray()));
    OMResponse response = om.getOmServerProtocol().processRequest(request);
    assertThat(response.getSuccess()).as(response.getMessage()).isTrue();
    return response;
  }

  /**
   * The static initializer of a test class runs after the cluster is started, so cluster configuration set there
   * does not reach the OMs. The append settings are read when a request arrives.
   */
  private OzoneConfiguration enableAppend() {
    OzoneConfiguration clientConf = new OzoneConfiguration(getConf());
    List<OzoneConfiguration> confs = getCluster().getOzoneManagersList().stream()
        .map(OzoneManager::getConfiguration).collect(Collectors.toList());
    confs.add(clientConf);
    for (OzoneConfiguration c : confs) {
      c.setBoolean(OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
      c.setBoolean("ozone.client.hbase.enhancements.allowed", true);
      c.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
      c.setBoolean(OMConfigKeys.OZONE_OM_APPEND_ENABLED, true);
    }
    return clientConf;
  }

  private OzoneClient newAppendClient() throws IOException {
    OzoneConfiguration clientConf = enableAppend();
    clientConf.set("ozone.client.append.lease.renew.interval", "500ms");
    return OzoneClientFactory.getRpcClient(getOmServiceId(), clientConf);
  }

  private static OzoneBucket createFsoBucket(OzoneClient client, String volumeName) throws IOException {
    OzoneVolume volume = client.getObjectStore().getVolume(volumeName);
    String bucketName = uniqueObjectName("fso");
    volume.createBucket(bucketName,
        BucketArgs.newBuilder().setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED).build());
    return volume.getBucket(bucketName);
  }

  /** hsync needs more than one replica. */
  private static String createReplicatedKey(OzoneBucket bucket) throws IOException {
    String key = uniqueObjectName("key");
    try (OzoneOutputStream out = bucket.createKey(key, 0, ReplicationType.RATIS, ReplicationFactor.THREE,
        new HashMap<>())) {
      out.write("prefix".getBytes(UTF_8));
    }
    return key;
  }

  private static byte[] readKey(OzoneBucket bucket, String key) throws IOException {
    try (OzoneInputStream in = bucket.readKey(key)) {
      return IOUtils.toByteArray(in);
    }
  }

  private static void waitForApplied(OzoneManager om, OzoneManager leader) throws Exception {
    // The leader answers a request before the double buffer flush that advances its last applied index.
    leader.awaitDoubleBufferFlush();
    long leaderIndex = leader.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    GenericTestUtils.waitFor(() -> om.getOmRatisServer().getLastAppliedTermIndex().getIndex() >= leaderIndex,
        100, 60000);
  }

  /** @return the open record of an append session as the given OM resolves it, null if the OM has no such session. */
  private static OmKeyInfo appendOpenRecord(OzoneManager om, OzoneBucket bucket, long sessionId) throws IOException {
    OMMetadataManager metadataManager = om.getMetadataManager();
    String dbOpenKey = metadataManager.getAppendSessionOpenKey(bucket.getVolumeName(), bucket.getName(), sessionId);
    return dbOpenKey == null ? null
        : metadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).get(dbOpenKey);
  }

  private static long lastRenewedAt(OzoneManager om, OzoneBucket bucket, long sessionId) {
    try {
      return appendOpenRecord(om, bucket, sessionId).getAppendSession().getLastRenewedAt();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** @return what the OMs must agree on for a committed file in the root of an FSO bucket. */
  private static String committedSummary(OzoneManager om, OzoneBucket bucket, String key) throws IOException {
    OMMetadataManager metadataManager = om.getMetadataManager();
    long bucketId = metadataManager.getBucketId(bucket.getVolumeName(), bucket.getName());
    OmKeyInfo committed = metadataManager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).get(
        metadataManager.getOzonePathKey(metadataManager.getVolumeId(bucket.getVolumeName()), bucketId, bucketId, key));
    return "owner=" + committed.getAppendOwnerSessionId() + " size=" + committed.getDataSize()
        + " updateID=" + committed.getUpdateID() + " mtime=" + committed.getModificationTime()
        + " blocks=" + committed.getLatestVersionLocations().getBlocksLatestVersionOnly().stream()
            .map(location -> location.getBlockID().getContainerBlockID() + ":" + location.getLength())
            .collect(Collectors.toList());
  }

  @Test
  void testListParts() throws Exception {

    OzoneBucket ozoneBucket = setupBucket();
    String keyName = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(ozoneBucket, keyName);

    Map<Integer, String> partsMap = new HashMap<>();
    partsMap.put(1, createMultipartUploadPartKey(ozoneBucket, 1, keyName,
        uploadID));
    partsMap.put(2, createMultipartUploadPartKey(ozoneBucket, 2, keyName,
        uploadID));
    partsMap.put(3, createMultipartUploadPartKey(ozoneBucket, 3, keyName,
        uploadID));

    validateListParts(ozoneBucket, keyName, uploadID, partsMap);

    // Stop leader OM, and then validate list parts.
    stopLeaderOM();
    waitForLeaderToBeReady();

    validateListParts(ozoneBucket, keyName, uploadID, partsMap);

  }

  /**
   * Validate parts uploaded to a MPU Key.
   */
  private void validateListParts(OzoneBucket ozoneBucket, String keyName,
      String uploadID, Map<Integer, String> partsMap) throws Exception {
    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        ozoneBucket.listParts(keyName, uploadID, 0, 1000);

    List<OzoneMultipartUploadPartListParts.PartInfo> partInfoList =
        ozoneMultipartUploadPartListParts.getPartInfoList();

    assertEquals(partInfoList.size(), partsMap.size());

    for (int i = 0; i < partsMap.size(); i++) {
      assertEquals(partsMap.get(partInfoList.get(i).getPartNumber()),
          partInfoList.get(i).getETag());

    }

    assertFalse(ozoneMultipartUploadPartListParts.isTruncated());
  }

  /**
   * Create a Multipart upload part Key with specified partNumber and uploadID.
   * @return Part name for the uploaded part.
   */
  private String createMultipartUploadPartKey(OzoneBucket ozoneBucket,
      int partNumber, String keyName, String uploadID) throws Exception {
    String value = "random data";
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createMultipartKey(
        keyName, value.length(), partNumber, uploadID);
    ozoneOutputStream.write(value.getBytes(UTF_8), 0, value.length());
    ozoneOutputStream.getMetadata().put(OzoneConsts.ETAG, DigestUtils.md5Hex(value));
    ozoneOutputStream.close();

    return ozoneOutputStream.getCommitUploadPartInfo().getETag();
  }

  @Test
  public void testConf() {
    final RaftProperties p = getCluster()
        .getOzoneManager()
        .getOmRatisServer()
        .getServerDivision()
        .getRaftServer()
        .getProperties();
    final TimeDuration t = RaftServerConfigKeys.Log.Appender.waitTimeMin(p);
    assertEquals(TimeDuration.ZERO, t,
        RaftServerConfigKeys.Log.Appender.WAIT_TIME_MIN_KEY);
  }

  @Test
  public void testKeyDeletion() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String data = "random data";
    String keyName1 = "dir/file1";
    String keyName2 = "dir/file2";
    String keyName3 = "dir/file3";
    String keyName4 = "dir/file4";

    testCreateFile(ozoneBucket, keyName1, data, true, false);
    testCreateFile(ozoneBucket, keyName2, data, true, false);
    testCreateFile(ozoneBucket, keyName3, data, true, false);
    testCreateFile(ozoneBucket, keyName4, data, true, false);

    ozoneBucket.deleteKey(keyName1);
    ozoneBucket.deleteKey(keyName2);
    ozoneBucket.deleteKey(keyName3);
    ozoneBucket.deleteKey(keyName4);

    // Now check delete table has entries been removed.

    OzoneManager ozoneManager = getCluster().getOMLeader();

    final KeyDeletingService keyDeletingService = ozoneManager.getKeyManager().getDeletingService();

    // Check on leader OM Count.
    GenericTestUtils.waitFor(() ->
        keyDeletingService.getRunCount().get() >= 2, 1000, 120000);
    GenericTestUtils.waitFor(() ->
        keyDeletingService.getDeletedKeyCount().get() == 4, 1000, 120000);

    // Check delete table is empty or not on all OMs.
    getCluster().getOzoneManagersList().forEach((om) -> {
      try {
        GenericTestUtils.waitFor(() -> {
          Table<String, RepeatedOmKeyInfo> deletedTable =
              om.getMetadataManager().getDeletedTable();
          try (Table.KeyValueIterator<String, RepeatedOmKeyInfo> iterator = deletedTable.iterator()) {
            return !iterator.hasNext();
          } catch (Exception ex) {
            return false;
          }
        },
            1000, 120000);
      } catch (Exception ex) {
        fail("TestOzoneManagerHAKeyDeletion failed");
      }
    });
  }

  /**
   * 1. Stop one of the OM
   * 2. make a call to OM, this will make failover attempts to find new node.
   * a) if LE finishes but leader not ready, it retries to same node
   * b) if LE not done, it will failover to new node and check
   * 3. Try failover to same OM explicitly.
   * Now #3 should wait additional waitBetweenRetries time.
   * LE: Leader Election.
   */
  @Test
  @Order(Integer.MAX_VALUE - 1)
  void testIncrementalWaitTimeWithSameNodeFailover() throws Exception {
    long waitBetweenRetries = getConf().getLong(
        OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY,
        OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT);
    final HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> omFailoverProxyProvider
        = OmTestUtil.getFailoverProxyProvider(getObjectStore());

    // The omFailoverProxyProvider will point to the current leader OM node.
    String leaderOMNodeId = omFailoverProxyProvider.getCurrentProxyOMNodeId();

    getCluster().stopOzoneManager(leaderOMNodeId);
    getCluster().waitForLeaderOM();
    createKeyTest(true); // failover should happen to new node

    long numTimesTriedToSameNode = omFailoverProxyProvider.getWaitTime()
        / waitBetweenRetries;
    omFailoverProxyProvider.setNextOmProxy(omFailoverProxyProvider.
        getCurrentProxyOMNodeId());
    assertEquals((numTimesTriedToSameNode + 1) * waitBetweenRetries,
        omFailoverProxyProvider.getWaitTime());
  }

  @Test
  void testOMHAMetrics() throws Exception {
    // Get leader OM
    OzoneManager leaderOM = getCluster().getOMLeader();
    // Store current leader's node ID,
    // to use it after restarting the OM
    String leaderOMId = leaderOM.getOMNodeId();
    // Get a list of all OMs
    List<OzoneManager> omList = getCluster().getOzoneManagersList();
    // Check metrics for all OMs
    checkOMHAMetricsForAllOMs(omList, leaderOMId);

    // Restart leader OM
    getCluster().shutdownOzoneManager(leaderOM);
    getCluster().restartOzoneManager(leaderOM, true);
    waitForLeaderToBeReady();

    // Do some writes so that the old leader can receive AppendEntries
    // which will trigger notifyLeaderChanged, instead of relying on
    // AppendEntries
    setupBucket();

    // Get the new leader
    OzoneManager newLeaderOM = getCluster().getOMLeader();
    String newLeaderOMId = newLeaderOM.getOMNodeId();
    // Get a list of all OMs again
    omList = getCluster().getOzoneManagersList();
    // New state for the old leader
    int newState = leaderOMId.equals(newLeaderOMId) ? 1 : 0;

    // Get old leader
    OzoneManager oldLeader = getCluster().getOzoneManager(leaderOMId);
    // Get old leader's metrics
    OMHAMetrics omhaMetrics = oldLeader.getOmhaMetrics();

    assertEquals(newState,
        omhaMetrics.getOmhaInfoOzoneManagerHALeaderState());

    // Check that metrics for all OMs have been updated
    checkOMHAMetricsForAllOMs(omList, newLeaderOMId);
  }

  private void checkOMHAMetricsForAllOMs(List<OzoneManager> omList,
      String leaderOMId) {
    for (OzoneManager om : omList) {
      // Get OMHAMetrics for the current OM
      OMHAMetrics omhaMetrics = om.getOmhaMetrics();
      String nodeId = om.getOMNodeId();

      // If current OM is leader, state should be 1
      int expectedState = nodeId
          .equals(leaderOMId) ? 1 : 0;
      assertEquals(expectedState,
          omhaMetrics.getOmhaInfoOzoneManagerHALeaderState());

      assertEquals(nodeId, omhaMetrics.getOmhaInfoNodeId());
    }
  }

  @Test
  void testOMRetryProxy() {
    int maxFailoverAttempts = getOzoneClientFailoverMaxAttempts();
    // Stop all the OMs.
    for (int i = 0; i < getNumOfOMs(); i++) {
      getCluster().stopOzoneManager(i);
    }

    final LogVerificationAppender appender = new LogVerificationAppender();
    final Logger logger = Logger.getRootLogger();
    logger.addAppender(appender);

    // After making N (set maxRetries value) connection attempts to OMs,
    // the RpcClient should give up.
    assertThrows(ConnectException.class, () -> createVolumeTest(true));
    assertEquals(1,
        appender.countLinesWithMessage("Failed to connect to OMs:"));
    assertEquals(maxFailoverAttempts,
        appender.countLinesWithMessage("Trying to failover"));
    assertEquals(1, appender.countLinesWithMessage("Attempted " +
        maxFailoverAttempts + " failovers."));
  }

  @Test
  void testListVolumes() throws Exception {
    String userName = UserGroupInformation.getCurrentUser().getUserName();
    ObjectStore objectStore = getObjectStore();

    String prefix = uniqueObjectName("vol-") + "-";
    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(userName)
        .build();

    Set<String> expectedVolumes = new TreeSet<>();
    for (int i = 0; i < 100; i++) {
      String volumeName = prefix + i;
      expectedVolumes.add(volumeName);
      objectStore.createVolume(volumeName, createVolumeArgs);
    }

    validateVolumesList(expectedVolumes,
        objectStore.listVolumesByUser(userName, prefix, ""));

    // Stop leader OM, and then validate list volumes for user.
    stopLeaderOM();
    waitForLeaderToBeReady();

    validateVolumesList(expectedVolumes,
        objectStore.listVolumesByUser(userName, prefix, ""));
  }

  @Test
  void testRetryCacheWithDownedOM() throws Exception {
    // Create a volume, a bucket and a key
    String userName = "user" + RandomStringUtils.secure().nextNumeric(5);
    String adminName = "admin" + RandomStringUtils.secure().nextNumeric(5);
    String volumeName = uniqueObjectName("volume");
    String bucketName = UUID.randomUUID().toString();
    String keyTo = UUID.randomUUID().toString();

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();
    getObjectStore().createVolume(volumeName, createVolumeArgs);
    OzoneVolume ozoneVolume = getObjectStore().getVolume(volumeName);
    ozoneVolume.createBucket(bucketName);
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    String keyFrom = createKey(ozoneBucket);

    int callId = 10;
    ClientId clientId = ClientId.randomId();
    MiniOzoneHAClusterImpl cluster = getCluster();
    OzoneManager omLeader = cluster.getOMLeader();

    OzoneManagerProtocolProtos.KeyArgs keyArgs =
        OzoneManagerProtocolProtos.KeyArgs.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyFrom)
            .build();
    OzoneManagerProtocolProtos.RenameKeyRequest renameKeyRequest
        = OzoneManagerProtocolProtos.RenameKeyRequest.newBuilder()
        .setKeyArgs(keyArgs)
        .setToKeyName(keyTo)
        .build();
    OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.RenameKey)
            .setRenameKeyRequest(renameKeyRequest)
            .setClientId(clientId.toString())
            .build();
    // set up the current call so that OM Ratis Server doesn't complain.
    Server.getCurCall().set(new Server.Call(callId, 0, null, null,
        RPC.RpcKind.RPC_BUILTIN, clientId.toByteString().toByteArray()));
    // Submit rename request to OM
    OzoneManagerProtocolProtos.OMResponse omResponse =
        omLeader.getOmServerProtocol().processRequest(omRequest);
    assertTrue(omResponse.getSuccess());

    // Make one of the follower OM the leader, and shutdown the current leader.
    OzoneManager newLeader = cluster.getOzoneManagersList().stream().filter(
        om -> !om.getOMNodeId().equals(omLeader.getOMNodeId())).findFirst().get();
    transferLeader(omLeader, newLeader);
    cluster.shutdownOzoneManager(omLeader);

    // Once the rename completes, the source key should no longer exist
    // and the destination key should exist.
    OMException omException = assertThrows(OMException.class,
        () -> ozoneBucket.getKey(keyFrom));
    assertEquals(omException.getResult(), OMException.ResultCodes.KEY_NOT_FOUND);
    assertTrue(ozoneBucket.getKey(keyTo).isFile());

    // Submit rename request to OM again. The request is cached so it will succeed.
    omResponse = newLeader.getOmServerProtocol().processRequest(omRequest);
    assertTrue(omResponse.getSuccess());
  }

  private void transferLeader(OzoneManager omLeader, OzoneManager newLeader) throws IOException {
    LOG.info("Transfer leadership from {}(raft id {}) to {}(raft id {})",
        omLeader.getOMNodeId(), omLeader.getOmRatisServer().getRaftPeerId(),
        newLeader.getOMNodeId(), newLeader.getOmRatisServer().getRaftPeerId());

    final SupportedRpcType rpc = SupportedRpcType.GRPC;
    final RaftProperties properties = RatisHelper.newRaftProperties(rpc);

    // For now not making anything configurable, RaftClient  is only used
    // in SCM for DB updates of sub-ca certs go via Ratis.
    RaftClient.Builder builder = RaftClient.newBuilder()
        .setRaftGroup(omLeader.getOmRatisServer().getRaftGroup())
        .setLeaderId(null)
        .setProperties(properties)
        .setRetryPolicy(
            RetryPolicies.retryUpToMaximumCountWithFixedSleep(120,
                TimeDuration.valueOf(500, TimeUnit.MILLISECONDS)));
    try (RaftClient raftClient = builder.build()) {
      RaftClientReply reply = raftClient.admin().transferLeadership(newLeader.getOmRatisServer()
          .getRaftPeerId(), 10 * 1000);
      assertTrue(reply.isSuccess());
    }
  }

  private void validateVolumesList(Set<String> expectedVolumes,
      Iterator<? extends OzoneVolume> volumeIterator) {
    int expectedCount = 0;

    while (volumeIterator.hasNext()) {
      OzoneVolume next = volumeIterator.next();
      assertThat(expectedVolumes).contains(next.getName());
      expectedCount++;
    }

    assertEquals(expectedVolumes.size(), expectedCount);
  }

}
