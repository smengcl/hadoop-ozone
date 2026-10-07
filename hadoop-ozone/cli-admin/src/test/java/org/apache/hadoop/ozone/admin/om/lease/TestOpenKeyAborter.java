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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

/**
 * Tests argument handling and confirmation of {@link OpenKeyAborter} against a mock OM client.
 */
public class TestOpenKeyAborter {

  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private final InputStream originalIn = System.in;
  private OzoneManagerProtocol omClient;

  @BeforeEach
  public void setup() throws Exception {
    omClient = mock(OzoneManagerProtocol.class);
    when(omClient.getServiceInfo()).thenReturn(serviceInfo(OzoneManagerVersion.APPEND));
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
    System.setIn(originalIn);
  }

  @Test
  public void testAbortWithYesOption() throws Exception {
    execute("", "--path", "/vol1/bucket1/dir1/key1", "--client-id", "123", "--yes");

    verify(omClient).abortOpenKey("vol1", "bucket1", "dir1/key1", 123L);
    assertThat(outContent.toString(DEFAULT_ENCODING))
        .doesNotContain("Enter 'yes'")
        .contains("Aborted open key /vol1/bucket1/dir1/key1 with client ID 123");
  }

  @Test
  public void testAbortAfterConfirmation() throws Exception {
    execute("yes\n", "--path", "vol1/bucket1/key1", "--client-id", "123");

    verify(omClient).abortOpenKey("vol1", "bucket1", "key1", 123L);
    assertThat(outContent.toString(DEFAULT_ENCODING))
        .contains("vol1/bucket1/key1", "client ID 123", "no recovery option", "Enter 'yes' to proceed");
  }

  @ParameterizedTest
  @ValueSource(strings = {"no\n", "y\n", ""})
  public void testNoAbortWithoutConfirmation(String input) throws Exception {
    execute(input, "--path", "/vol1/bucket1/key1", "--client-id", "123");

    verify(omClient, never()).abortOpenKey(any(), any(), any(), anyLong());
    assertThat(outContent.toString(DEFAULT_ENCODING)).contains("Operation cancelled.");
  }

  @ParameterizedTest
  @ValueSource(strings = {"/vol1/bucket1", "/vol1/bucket1/", "/vol1//key1", "/"})
  public void testInvalidPath(String path) throws Exception {
    assertThatThrownBy(() -> execute("", "--path", path, "--client-id", "123", "--yes"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expected /volume/bucket/key");

    verify(omClient, never()).abortOpenKey(any(), any(), any(), anyLong());
  }

  @Test
  public void testRequiredOptions() {
    assertThatThrownBy(() -> new CommandLine(new OpenKeyAborter()).parseArgs("--path", "/vol1/bucket1/key1"))
        .isInstanceOf(CommandLine.MissingParameterException.class)
        .hasMessageContaining("--client-id");
  }

  @Test
  public void testOldOzoneManagerVersion() throws Exception {
    when(omClient.getServiceInfo()).thenReturn(serviceInfo(OzoneManagerVersion.HBASE_SUPPORT));

    assertThatThrownBy(() -> execute("", "--path", "/vol1/bucket1/key1", "--client-id", "123", "--yes"))
        .hasMessageContaining("requires OzoneManager version APPEND");

    verify(omClient, never()).abortOpenKey(any(), any(), any(), anyLong());
  }

  private void execute(String input, String... args) throws Exception {
    System.setIn(new ByteArrayInputStream(input.getBytes(DEFAULT_ENCODING)));
    OpenKeyAborter cmd = new OpenKeyAborter();
    new CommandLine(cmd).parseArgs(args);
    cmd.execute(omClient);
  }

  private static ServiceInfoEx serviceInfo(OzoneManagerVersion omVersion) {
    return new ServiceInfoEx(Collections.singletonList(ServiceInfo.newBuilder()
        .setNodeType(HddsProtos.NodeType.OM)
        .setHostname("om1")
        .setOmVersion(omVersion)
        .build()), null, null);
  }
}
