/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;

public final class MiniOzoneClusterLauncher {
  private MiniOzoneClusterLauncher() {
  }

  public static void main(String[] args) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OMConfigKeys.OZONE_OM_S3_GPRC_SERVER_ENABLED, true);
    conf.setInt(OMConfigKeys.OZONE_OM_GRPC_PORT_KEY, reservePort());

    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        cluster.shutdown();
      } catch (IOException ex) {
        System.err.println("Failed to shutdown MiniOzoneCluster: " + ex);
      }
    }));

    int grpcPort = conf.getInt(OMConfigKeys.OZONE_OM_GRPC_PORT_KEY, 0);
    System.out.println("OM_GRPC_ENDPOINT=127.0.0.1:" + grpcPort);
    System.out.flush();

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    reader.readLine();
    cluster.shutdown();
  }

  private static int reservePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
