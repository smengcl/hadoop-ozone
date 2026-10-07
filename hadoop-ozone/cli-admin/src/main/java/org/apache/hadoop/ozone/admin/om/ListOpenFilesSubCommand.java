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

package org.apache.hadoop.ozone.admin.om;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.ListOpenFilesResult;
import org.apache.hadoop.ozone.om.helpers.OmAppendSession;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionPhase;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendWriterKind;
import picocli.CommandLine;

/**
 * Handler of ozone admin om list-open-files command.
 */
@CommandLine.Command(
    name = "list-open-files",
    aliases = {"list-open-keys", "lof", "lok"},
    description = "Lists open files (keys) in Ozone Manager.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class ListOpenFilesSubCommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Mixin
  private OmAddressOptions.OptionalServiceIdOrHostMixin omAddressOptions;

  @CommandLine.Option(names = { "--json" },
      defaultValue = "false",
      description = "Format output as JSON")
  private boolean json;

  @CommandLine.Option(names = { "--show-deleted" },
      defaultValue = "false",
      description = "Whether to show deleted open keys")
  private boolean showDeleted;

  @CommandLine.Option(names = { "--show-overwritten" },
      defaultValue = "false",
      description = "Whether to show overwritten open keys")
  private boolean showOverwritten;

  // Conforms to ListOptions, but not all in ListOptions applies here thus
  // not using that directly
  @CommandLine.Option(
      names = {"-p", "--prefix"},
      description = "Filter results by the specified path on the server side.",
      defaultValue = "/"
  )
  private String pathPrefix;

  @CommandLine.Option(
      names = {"-l", "--length"},
      description = "Maximum number of items to list",
      defaultValue = "100"
  )
  private int limit;

  @CommandLine.Option(
      names = {"-s", "--start"},
      description = "The item to start the listing from.%n" +
          "i.e. continuation token. " +
          "This will be excluded from the result.",
      defaultValue = ""
  )
  private String startItem;

  @Override
  public Void call() throws Exception {
    try (OzoneManagerProtocol omClient = omAddressOptions.newClient()) {
      execute(omClient);
    }

    return null;
  }

  void execute(OzoneManagerProtocol ozoneManagerClient) throws IOException {
    ServiceInfoEx serviceInfoEx = ozoneManagerClient.getServiceInfo();
    final OzoneManagerVersion omVersion = RpcClient.getOmVersion(serviceInfoEx);
    if (omVersion.compareTo(OzoneManagerVersion.HBASE_SUPPORT) < 0) {
      System.err.println("Error: This command requires OzoneManager version "
          + OzoneManagerVersion.HBASE_SUPPORT.name() + " or later.");
      return;
    }

    ListOpenFilesResult res =
        ozoneManagerClient.listOpenFiles(pathPrefix, limit, startItem);

    if (!showDeleted) {
      res.getOpenKeys().removeIf(o -> isDeleted(o.getKeyInfo()));
    }
    if (!showOverwritten) {
      res.getOpenKeys().removeIf(o -> o.getKeyInfo().getMetadata().containsKey(OzoneConsts.OVERWRITTEN_HSYNC_KEY));
    }
    if (json) {
      // Print detailed JSON
      printOpenKeysListAsJson(res);
    } else {
      // Human friendly output
      printOpenKeysList(res);
    }
  }

  private void printOpenKeysListAsJson(ListOpenFilesResult res)
      throws IOException {
    ObjectNode root = JsonUtils.createObjectNode(res);
    // The openKeys array has one node per open key, in the order of the result.
    for (int i = 0; i < res.getOpenKeys().size(); i++) {
      OmKeyInfo omKeyInfo = res.getOpenKeys().get(i).getKeyInfo();
      ObjectNode openKey = (ObjectNode) root.get("openKeys").get(i);
      openKey.put("writerKind", getWriterKind(omKeyInfo).name());
      if (omKeyInfo.getAppendSession() != null) {
        openKey.put("suffixBlockCount", getBlockCount(omKeyInfo));
      }
    }
    System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(root));
  }

  private void printOpenKeysList(ListOpenFilesResult res) {

    List<OpenKeySession> openFileList = res.getOpenKeys();

    String msg = getMessageString(res, openFileList);
    System.out.println(msg);

    for (OpenKeySession e : openFileList) {
      long clientId = e.getId();
      OmKeyInfo omKeyInfo = e.getKeyInfo();
      StringBuilder line = new StringBuilder()
          .append(clientId).append('\t')
          .append(Instant.ofEpochMilli(omKeyInfo.getCreationTime())).append('\t');

      if (omKeyInfo.isHsync()) {
        String hsyncClientIdStr =
            omKeyInfo.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
        long hsyncClientId = Long.parseLong(hsyncClientIdStr);
        if (clientId == hsyncClientId) {
          line.append("Yes\t\t");
        } else {
          // last hsync'ed with a different client ID than the client that
          // initially opens the file (!)
          line.append("Yes w/ cid ").append(hsyncClientIdStr).append('\t');
        }

        if (showDeleted) {
          if (omKeyInfo.getMetadata().containsKey(OzoneConsts.DELETED_HSYNC_KEY)) {
            line.append("Yes\t\t");
          } else {
            line.append("No\t\t");
          }
        }
        if (showOverwritten) {
          if (omKeyInfo.getMetadata().containsKey(OzoneConsts.OVERWRITTEN_HSYNC_KEY)) {
            line.append("Yes\t");
          } else {
            line.append("No\t");
          }
        }
      } else {
        line.append("No\t\t");
        if (showDeleted) {
          line.append(isDeleted(omKeyInfo) ? "Yes\t\t" : "No\t\t");
        }
        if (showOverwritten) {
          line.append("No\t");
        }
      }

      line.append(getWriterKind(omKeyInfo)).append('\t');
      line.append(getFullPathFromKeyInfo(omKeyInfo));

      OmAppendSession appendSession = omKeyInfo.getAppendSession();
      if (appendSession != null) {
        line.append("\tphase=").append(appendSession.getPhase())
            .append(" prefixLength=").append(appendSession.getPrefixLength())
            .append(" suffixBlocks=").append(getBlockCount(omKeyInfo))
            .append(" lastRenewed=").append(Instant.ofEpochMilli(appendSession.getLastRenewedAt()));
      }

      System.out.println(line);
    }

    System.out.println();

    // Compose next batch's command
    if (res.hasMore()) {
      String nextBatchCmd = getCmdForNextBatch(res.getContinuationToken());

      System.out.println("To get the next batch of open keys, run:");
      System.out.println("  " + nextBatchCmd);
    } else {
      System.out.println("Reached the end of the list.");
    }
  }

  /**
   * @return formatted output message for the command.
   */
  private String getMessageString(ListOpenFilesResult res, List<OpenKeySession> openFileList) {
    StringBuilder sb = new StringBuilder();
    sb.append(res.getTotalOpenKeyCount())
        .append(" total open files. Showing ")
        .append(openFileList.size())
        .append(" open files (limit ")
        .append(limit)
        .append(") under path prefix:")
        .append(System.lineSeparator())
        .append("  ")
        .append(pathPrefix);
    if (startItem != null && !startItem.isEmpty()) {
      sb.append(System.lineSeparator())
          .append("after continuation token:")
          .append(System.lineSeparator())
          .append("  ")
          .append(startItem);
    }
    sb.append(System.lineSeparator())
        .append(System.lineSeparator())
        .append("Client ID\t\t\tCreation time\t\tHsync'ed\t");
    if (showDeleted) {
      sb.append("Deleted\t");
    }
    if (showOverwritten) {
      sb.append("Overwritten\t");
    }
    sb.append("Writer\t\tOpen File Path");
    return sb.toString();
  }

  /**
   * @return the command to get the next batch of open keys
   */
  private String getCmdForNextBatch(String lastElementFullPath) {
    StringBuilder nextBatchCmd = new StringBuilder("ozone admin om lof ").append(omAddressOptions);
    if (json) {
      nextBatchCmd.append(" --json");
    }
    nextBatchCmd.append(" --length=").append(limit);
    if (pathPrefix != null && !pathPrefix.isEmpty()) {
      nextBatchCmd.append(" --prefix=").append(pathPrefix);
    }
    nextBatchCmd.append(" --start=").append(lastElementFullPath);
    return nextBatchCmd.toString();
  }

  /** An invalidated append session is what is left of a deleted file, like a deleted hsync'ed key. */
  private static boolean isDeleted(OmKeyInfo omKeyInfo) {
    OmAppendSession appendSession = omKeyInfo.getAppendSession();
    return omKeyInfo.getMetadata().containsKey(OzoneConsts.DELETED_HSYNC_KEY)
        || (appendSession != null && appendSession.getPhase() == AppendSessionPhase.APPEND_INVALIDATED);
  }

  private static AppendWriterKind getWriterKind(OmKeyInfo omKeyInfo) {
    if (omKeyInfo.getAppendSession() != null) {
      return AppendWriterKind.APPEND_WRITER;
    }
    return omKeyInfo.isHsync() ? AppendWriterKind.HSYNC_WRITER : AppendWriterKind.ORDINARY_WRITER;
  }

  /** For an append session, the open key holds only the blocks allocated by the session (the suffix). */
  private static long getBlockCount(OmKeyInfo omKeyInfo) {
    OmKeyLocationInfoGroup locations = omKeyInfo.getLatestVersionLocations();
    return locations == null ? 0 : locations.getLocationListCount();
  }

  private String getFullPathFromKeyInfo(OmKeyInfo oki) {
    return "/" + oki.getVolumeName() +
        "/" + oki.getBucketName() +
        "/" + oki.getPath();
  }

}
