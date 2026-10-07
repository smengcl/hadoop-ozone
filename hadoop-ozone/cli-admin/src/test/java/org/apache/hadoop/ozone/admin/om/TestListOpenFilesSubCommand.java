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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.ListOpenFilesResult;
import org.apache.hadoop.ozone.om.helpers.OmAppendSession;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionPhase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Tests how {@link ListOpenFilesSubCommand} shows ordinary writers, hsync'ed writers and append sessions.
 */
public class TestListOpenFilesSubCommand {

  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
  private static final long ORDINARY_ID = 1L;
  private static final long HSYNC_ID = 2L;
  private static final long APPEND_ID = 3L;
  private static final long INVALIDATED_ID = 4L;
  private static final long RENEWED_AT = 1_700_000_000_000L;

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private OzoneManagerProtocol omClient;

  @BeforeEach
  public void setup() throws Exception {
    OmAppendSession appendSession = OmAppendSession.newActive(1024, 1, RENEWED_AT - 1).withRenewal(RENEWED_AT);
    List<OpenKeySession> openKeys = new ArrayList<>(Arrays.asList(
        openKey(ORDINARY_ID, keyInfo("ordinary", 1)),
        openKey(HSYNC_ID, keyInfo("hsynced", 1).addMetadata(OzoneConsts.HSYNC_CLIENT_ID, String.valueOf(HSYNC_ID))),
        openKey(APPEND_ID, keyInfo("appended", 2).setAppendSession(appendSession)),
        openKey(INVALIDATED_ID, keyInfo("invalidated", 0)
            .setAppendSession(appendSession.withPhase(AppendSessionPhase.APPEND_INVALIDATED)))));

    omClient = mock(OzoneManagerProtocol.class);
    when(omClient.getServiceInfo()).thenReturn(new ServiceInfoEx(Collections.emptyList(), null, null));
    when(omClient.listOpenFiles(anyString(), anyInt(), anyString()))
        .thenReturn(new ListOpenFilesResult(openKeys.size(), false, null, openKeys));
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
  }

  @Test
  public void testPlainOutput() throws Exception {
    String output = execute();

    assertThat(output).contains("Hsync'ed\tWriter\t\tOpen File Path");
    // The path column is unchanged: /volume/bucket/parentObjectID/fileName.
    assertThat(lineOf(output, ORDINARY_ID)).endsWith("\tNo\t\tORDINARY_WRITER\t/vol1/bucket1/0/ordinary");
    assertThat(lineOf(output, HSYNC_ID)).endsWith("\tYes\t\tHSYNC_WRITER\t/vol1/bucket1/0/hsynced");
    assertThat(lineOf(output, APPEND_ID)).endsWith("\tNo\t\tAPPEND_WRITER\t/vol1/bucket1/0/appended"
        + "\tphase=APPEND_ACTIVE prefixLength=1024 suffixBlocks=2 lastRenewed=" + Instant.ofEpochMilli(RENEWED_AT));
    // Like a deleted hsync'ed key, an invalidated append session is hidden unless --show-deleted is given.
    assertThat(output).doesNotContain("invalidated");
  }

  @Test
  public void testPlainOutputShowDeleted() throws Exception {
    String output = execute("--show-deleted");

    assertThat(output).contains("Hsync'ed\tDeleted\tWriter\t\tOpen File Path");
    assertThat(lineOf(output, APPEND_ID)).contains("\tNo\t\tNo\t\tAPPEND_WRITER\t/vol1/bucket1/0/appended\t");
    assertThat(lineOf(output, INVALIDATED_ID)).endsWith("\tNo\t\tYes\t\tAPPEND_WRITER\t/vol1/bucket1/0/invalidated"
        + "\tphase=APPEND_INVALIDATED prefixLength=1024 suffixBlocks=0 lastRenewed="
        + Instant.ofEpochMilli(RENEWED_AT));
  }

  @Test
  public void testJsonOutput() throws Exception {
    JsonNode openKeys = JsonUtils.readTree(execute("--json")).get("openKeys");

    assertThat(openKeys).hasSize(3);
    assertThat(openKeys.get(0).get("clientId").asLong()).isEqualTo(ORDINARY_ID);
    assertThat(openKeys.get(0).get("writerKind").asText()).isEqualTo("ORDINARY_WRITER");
    assertThat(openKeys.get(0).has("suffixBlockCount")).isFalse();
    assertThat(openKeys.get(1).get("writerKind").asText()).isEqualTo("HSYNC_WRITER");

    JsonNode append = openKeys.get(2);
    assertThat(append.get("clientId").asLong()).isEqualTo(APPEND_ID);
    assertThat(append.get("writerKind").asText()).isEqualTo("APPEND_WRITER");
    assertThat(append.get("suffixBlockCount").asLong()).isEqualTo(2);
    JsonNode session = append.get("keyInfo").get("appendSession");
    assertThat(session.get("phase").asText()).isEqualTo("APPEND_ACTIVE");
    assertThat(session.get("prefixLength").asLong()).isEqualTo(1024);
    assertThat(session.get("lastRenewedAt").asLong()).isEqualTo(RENEWED_AT);
  }

  @Test
  public void testJsonOutputShowDeleted() throws Exception {
    JsonNode openKeys = JsonUtils.readTree(execute("--json", "--show-deleted")).get("openKeys");

    assertThat(openKeys).hasSize(4);
    assertThat(openKeys.get(3).get("keyInfo").get("appendSession").get("phase").asText())
        .isEqualTo("APPEND_INVALIDATED");
    assertThat(openKeys.get(3).get("suffixBlockCount").asLong()).isZero();
  }

  private String execute(String... args) throws Exception {
    ListOpenFilesSubCommand cmd = new ListOpenFilesSubCommand();
    new CommandLine(cmd).parseArgs(args);
    cmd.execute(omClient);
    return outContent.toString(DEFAULT_ENCODING);
  }

  private static String lineOf(String output, long clientId) {
    return Arrays.stream(output.split(System.lineSeparator()))
        .filter(line -> line.startsWith(clientId + "\t"))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No line for client ID " + clientId + " in:\n" + output));
  }

  private static OpenKeySession openKey(long clientId, OmKeyInfo.Builder keyInfo) {
    return new OpenKeySession(clientId, keyInfo.build(), 0);
  }

  private static OmKeyInfo.Builder keyInfo(String keyName, int blockCount) {
    List<OmKeyLocationInfo> blocks = new ArrayList<>();
    for (int i = 0; i < blockCount; i++) {
      blocks.add(new OmKeyLocationInfo.Builder().setBlockID(new BlockID(1L, i)).build());
    }
    return new OmKeyInfo.Builder()
        .setVolumeName("vol1")
        .setBucketName("bucket1")
        .setKeyName(keyName)
        .setReplicationConfig(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE))
        .setOmKeyLocationInfos(Collections.singletonList(new OmKeyLocationInfoGroup(0, blocks)));
  }
}
