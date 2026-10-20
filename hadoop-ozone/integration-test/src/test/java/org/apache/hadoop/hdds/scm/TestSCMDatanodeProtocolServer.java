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

package org.apache.hadoop.hdds.scm;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer;
import org.apache.hadoop.hdds.scm.server.SCMPolicyProvider;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.protocol.ReconDatanodeProtocol;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.recon.scm.ReconPolicyProvider;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Test for StorageContainerDatanodeProtocolProtos.
 */
public class TestSCMDatanodeProtocolServer {

  private static final String DATANODE_USER = "dn/host1@EXAMPLE.COM";

  @Test
  public void ensureTermAndDeadlineOnCommands()
      throws IOException, TimeoutException {
    OzoneStorageContainerManager scm =
        mock(OzoneStorageContainerManager.class);

    ReplicateContainerCommand command = ContainerTestUtils
        .getReplicateContainerCommand(1, randomDatanodeDetails());
    command.setTerm(5L);
    command.setDeadline(1234L);
    StorageContainerDatanodeProtocolProtos.SCMCommandProto proto =
        SCMDatanodeProtocolServer.getCommandResponse(command, scm);

    assertEquals(StorageContainerDatanodeProtocolProtos.SCMCommandProto
        .Type.replicateContainerCommand, proto.getCommandType());
    assertEquals(5L, proto.getTerm());
    assertEquals(1234L, proto.getDeadlineMsSinceEpoch());
  }

  @Test
  void registerAndHeartbeatAreDeniedForOtherUsers() throws Exception {
    final NodeManager nodeManager = mock(NodeManager.class);
    final SCMDatanodeProtocolServer server = newServer(nodeManager, true);
    final DatanodeDetails dn = randomDatanodeDetails();
    try {
      for (UserGroupInformation user : new UserGroupInformation[] {
          UserGroupInformation.createRemoteUser("alice"),
          UserGroupInformation.createRemoteUser("om/host1@EXAMPLE.COM"),
          UserGroupInformation.createRemoteUser("dn@EXAMPLE.COM"),
          UserGroupInformation.createRemoteUser("dn/host1@OTHER.COM"),
          UserGroupInformation.createProxyUser(DATANODE_USER, UserGroupInformation.createRemoteUser("alice"))}) {
        try (MockedStatic<Server> rpc = mockRpcCaller(user, dn.getIpAddress())) {
          assertThatThrownBy(() -> register(server, dn))
              .isInstanceOf(AccessControlException.class)
              .hasMessageContaining(HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY);
          assertThatThrownBy(() -> server.sendHeartbeat(heartbeat(dn)))
              .isInstanceOf(AccessControlException.class)
              .hasMessageContaining(HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY);
        }
      }
      verify(nodeManager, never()).register(any(), any(), any(), any());
      verify(nodeManager, never()).processHeartbeat(any(), any());
    } finally {
      server.stop();
    }
  }

  @Test
  void registerAndHeartbeatAreAllowedForDatanode() throws Exception {
    final NodeManager nodeManager = mock(NodeManager.class);
    final SCMDatanodeProtocolServer server = newServer(nodeManager, true);
    final DatanodeDetails dn = randomDatanodeDetails();
    when(nodeManager.register(any(), any(), any(), any())).thenReturn(registered(dn));
    try (MockedStatic<Server> rpc = mockRpcCaller(UserGroupInformation.createRemoteUser(DATANODE_USER), "10.0.0.1")) {
      register(server, dn);
      verify(nodeManager).register(any(), any(), any(), any());

      registeredAt(nodeManager, dn, "10.0.0.1", Time.monotonicNow());
      server.sendHeartbeat(heartbeat(dn));
      verify(nodeManager).processHeartbeat(any(), any());
    } finally {
      server.stop();
    }
  }

  @Test
  void registeredDatanodeIsBoundToItsAddress() throws Exception {
    final NodeManager nodeManager = mock(NodeManager.class);
    final SCMDatanodeProtocolServer server = newServer(nodeManager, true);
    final DatanodeDetails dn = randomDatanodeDetails();
    when(nodeManager.register(any(), any(), any(), any())).thenReturn(registered(dn));
    registeredAt(nodeManager, dn, "10.0.0.1", Time.monotonicNow());
    try (MockedStatic<Server> rpc = mockRpcCaller(UserGroupInformation.createRemoteUser(DATANODE_USER), "10.0.0.2")) {
      final SCMHeartbeatResponseProto response = server.sendHeartbeat(heartbeat(dn));
      assertThat(response.getCommandsList())
          .extracting(SCMCommandProto::getCommandType)
          .containsExactly(SCMCommandProto.Type.reregisterCommand);
      verify(nodeManager, never()).processHeartbeat(any(), any());

      assertThatThrownBy(() -> register(server, dn))
          .isInstanceOf(AccessControlException.class)
          .hasMessageContaining("10.0.0.1");
      verify(nodeManager, never()).register(any(), any(), any(), any());

      // once the registered address stopped sending heartbeats, heartbeats from the new address are still not
      // processed, but the Datanode may register from there
      registeredAt(nodeManager, dn, "10.0.0.1", Time.monotonicNow() - 10 * 60 * 1000);
      assertThat(server.sendHeartbeat(heartbeat(dn)).getCommandsList())
          .extracting(SCMCommandProto::getCommandType)
          .containsExactly(SCMCommandProto.Type.reregisterCommand);
      verify(nodeManager, never()).processHeartbeat(any(), any());
      register(server, dn);
      verify(nodeManager).register(any(), any(), any(), any());
    } finally {
      server.stop();
    }
  }

  @Test
  void registerAndHeartbeatAreNotCheckedWithoutSecurity() throws Exception {
    final NodeManager nodeManager = mock(NodeManager.class);
    final SCMDatanodeProtocolServer server = newServer(nodeManager, false);
    final DatanodeDetails dn = randomDatanodeDetails();
    when(nodeManager.register(any(), any(), any(), any())).thenReturn(registered(dn));
    registeredAt(nodeManager, dn, "10.0.0.1", Time.monotonicNow());
    try (MockedStatic<Server> rpc = mockRpcCaller(UserGroupInformation.createRemoteUser("alice"), "10.0.0.2")) {
      register(server, dn);
      server.sendHeartbeat(heartbeat(dn));
      verify(nodeManager).register(any(), any(), any(), any());
      verify(nodeManager).processHeartbeat(any(), any());
    } finally {
      server.stop();
    }
  }

  @Test
  void serviceAuthorizationRequiresDatanodePrincipal() throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY, "dn/_HOST@EXAMPLE.COM");
    final InetAddress address = InetAddress.getByName("127.0.0.1");
    final String host = address.getCanonicalHostName().toLowerCase();
    // ServiceAuthorizationManager checks the client principal only with Kerberos enabled
    try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class, CALLS_REAL_METHODS)) {
      ugi.when(UserGroupInformation::isSecurityEnabled).thenReturn(true);
      assertServiceAuthorization(conf, address, host, SCMPolicyProvider.getInstance(),
          StorageContainerDatanodeProtocol.class);
      assertServiceAuthorization(conf, address, host, ReconPolicyProvider.getInstance(), ReconDatanodeProtocol.class);
    }
  }

  private static void assertServiceAuthorization(OzoneConfiguration conf, InetAddress address, String host,
      PolicyProvider policyProvider, Class<?> protocol) {
    final ServiceAuthorizationManager manager = new ServiceAuthorizationManager();
    manager.refresh(conf, policyProvider);
    assertDoesNotThrow(() -> manager.authorize(
        UserGroupInformation.createRemoteUser("dn/" + host + "@EXAMPLE.COM"), protocol, conf, address));
    for (String user : new String[] {"om/" + host + "@EXAMPLE.COM", "dn/otherhost@EXAMPLE.COM"}) {
      assertThatThrownBy(() -> manager.authorize(UserGroupInformation.createRemoteUser(user), protocol, conf, address))
          .isInstanceOf(AuthorizationException.class)
          .hasMessageContaining("dn/" + host + "@EXAMPLE.COM");
    }
  }

  private static SCMDatanodeProtocolServer newServer(NodeManager nodeManager, boolean secure) throws IOException {
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, secure);
    conf.set(HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY, "dn/_HOST@EXAMPLE.COM");
    final SCMNodeDetails nodeDetails = mock(SCMNodeDetails.class);
    when(nodeDetails.getDatanodeProtocolServerAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 0));
    when(nodeDetails.getDatanodeAddressKey()).thenReturn("ozone.scm.datanode.address");
    final OzoneStorageContainerManager scm = mock(OzoneStorageContainerManager.class);
    when(scm.getScmNodeManager()).thenReturn(nodeManager);
    when(scm.getScmNodeDetails()).thenReturn(nodeDetails);
    return new SCMDatanodeProtocolServer(conf, scm, mock(EventPublisher.class), null);
  }

  private static MockedStatic<Server> mockRpcCaller(UserGroupInformation user, String ip) throws IOException {
    final MockedStatic<Server> rpc = mockStatic(Server.class, CALLS_REAL_METHODS);
    rpc.when(Server::getRemoteUser).thenReturn(user);
    rpc.when(Server::getRemoteIp).thenReturn(InetAddress.getByName(ip));
    return rpc;
  }

  private static void registeredAt(NodeManager nodeManager, DatanodeDetails dn, String ip, long lastHeartbeat) {
    final DatanodeInfo info = mock(DatanodeInfo.class);
    when(info.getIpAddress()).thenReturn(ip);
    when(info.getLastHeartbeatTime()).thenReturn(lastHeartbeat);
    when(nodeManager.isNodeRegistered(any())).thenReturn(true);
    when(nodeManager.getNode(dn.getID())).thenReturn(info);
  }

  private static RegisteredCommand registered(DatanodeDetails dn) {
    return RegisteredCommand.newBuilder()
        .setErrorCode(StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto.ErrorCode.success)
        .setDatanode(dn)
        .setClusterID("cluster")
        .build();
  }

  private static void register(SCMDatanodeProtocolServer server, DatanodeDetails dn) throws IOException {
    server.register(dn.getExtendedProtoBufMessage(), null, null, null, null);
  }

  private static SCMHeartbeatRequestProto heartbeat(DatanodeDetails dn) {
    return SCMHeartbeatRequestProto.newBuilder().setDatanodeDetails(dn.getProtoBufMessage()).build();
  }
}
