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

package org.apache.hadoop.ozone.admin.om.lease;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.Callable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.admin.om.OmAddressOptions;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

/**
 * CLI to abort one open key of a writer that is confirmed dead.
 */
@CommandLine.Command(
    name = "abort",
    description = "Abort one open file (key) whose writer is confirmed dead. Data the writer has not committed is "
        + "discarded and cannot be recovered. Find the client ID with 'ozone admin om list-open-files'. "
        + "The path is /volume/bucket/key. Does not apply to hsync'ed files (use 'lease recover' instead) or append "
        + "sessions.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class OpenKeyAborter implements Callable<Void> {

  @CommandLine.Mixin
  private OmAddressOptions.OptionalServiceIdOrHostMixin omAddressOptions;

  @CommandLine.Option(names = {"--path"},
      required = true,
      description = "Path of the open file (key) in /volume/bucket/key format")
  private String path;

  @CommandLine.Option(names = {"--client-id"},
      required = true,
      description = "Client ID of the open file (key) to abort")
  private long clientId;

  @CommandLine.Option(names = {"-y", "--yes"},
      description = "Continue without interactive user confirmation")
  private boolean yes;

  @Override
  public Void call() throws Exception {
    try (OzoneManagerProtocol omClient = omAddressOptions.newClient()) {
      execute(omClient);
    }
    return null;
  }

  void execute(OzoneManagerProtocol omClient) throws IOException {
    final String[] address = StringUtils.removeStart(path, "/").split("/", 3);
    if (address.length < 3 || StringUtils.isAnyEmpty(address)) {
      throw new IllegalArgumentException("Invalid path: " + path + ". Expected /volume/bucket/key.");
    }

    final OzoneManagerVersion omVersion = RpcClient.getOmVersion(omClient.getServiceInfo());
    if (omVersion.compareTo(OzoneManagerVersion.APPEND) < 0) {
      throw new IOException("This command requires OzoneManager version " + OzoneManagerVersion.APPEND.name()
          + " or later.");
    }

    if (!yes) {
      // Ask for user confirmation
      System.out.printf("This command will abort open key %s with client ID %d."
          + "%nData its writer has not committed is discarded. There is no recovery option after using this command."
          + "%nEnter 'yes' to proceed: ", path, clientId);
      System.out.flush();
      Scanner scanner = new Scanner(new InputStreamReader(System.in, StandardCharsets.UTF_8));
      if (!scanner.hasNext() || !scanner.next().trim().equalsIgnoreCase("yes")) {
        System.out.println("Operation cancelled.");
        return;
      }
    }

    omClient.abortOpenKey(address[0], address[1], address[2], clientId);
    System.out.println("Aborted open key " + path + " with client ID " + clientId);
  }
}
