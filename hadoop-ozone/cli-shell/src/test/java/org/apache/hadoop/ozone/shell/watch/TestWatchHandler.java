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

package org.apache.hadoop.ozone.shell.watch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.InotifyOpType;
import org.apache.hadoop.ozone.om.helpers.InotifyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InotifyRequest;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import picocli.CommandLine;

class TestWatchHandler {

  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  private static class TestableWatchHandler extends WatchHandler {
    OzoneAddress exposeAddress() {
      return super.getAddress();
    }

    void publicExecute(OzoneClient client, OzoneAddress address)
        throws java.io.IOException {
      execute(client, address);
    }
  }

  @BeforeEach
  void setup() throws UnsupportedEncodingException {
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  void tearDown() {
    System.setErr(originalErr);
  }

  @Test
  void testFetchOnceBuildsRequest() throws Exception {
    WatchHandler handler = new WatchHandler();
    new CommandLine(handler).parseArgs(
        "--recursive=false",
        "--limit=42",
        "--resolve-fso-paths",
        "--op-type=read",
        "vol1/bucket1/prefix");

    OzoneClient client = mock(OzoneClient.class);
    ClientProtocol protocol = mock(ClientProtocol.class);
    when(client.getProxy()).thenReturn(protocol);
    when(protocol.getInotifyEvents(org.mockito.ArgumentMatchers.any()))
        .thenReturn(new InotifyResponse());

    InotifyResponse response = handler.fetchOnce(
        client, "vol1/bucket1/prefix", 0L, 0L, InotifyOpType.READ);

    ArgumentCaptor<InotifyRequest> captor =
        ArgumentCaptor.forClass(InotifyRequest.class);
    verify(protocol).getInotifyEvents(captor.capture());
    InotifyRequest request = captor.getValue();
    assertThat(request.getWriteSequenceNumber()).isEqualTo(0L);
    assertThat(request.getAccessSequenceNumber()).isEqualTo(0L);
    assertThat(request.getLimitCount()).isEqualTo(42L);
    assertThat(request.getRecursive()).isFalse();
    assertThat(request.getResolveFsoPaths()).isTrue();
    assertThat(request.getPathPrefix()).isEqualTo("vol1/bucket1/prefix");
    assertThat(request.getOpType().name()).isEqualTo("READ");
    assertThat(response).isNotNull();
  }

  @Test
  void testParseRootPrefix() throws Exception {
    TestableWatchHandler handler = new TestableWatchHandler();
    new CommandLine(handler).parseArgs("/");
    OzoneAddress address = handler.exposeAddress();
    assertThat(address.getVolumeName()).isEmpty();
    assertThat(address.getBucketName()).isEmpty();
    assertThat(address.getKeyName()).isEmpty();
  }

  @Test
  void testParseDefaultRootPrefix() throws Exception {
    TestableWatchHandler handler = new TestableWatchHandler();
    new CommandLine(handler).parseArgs();
    OzoneAddress address = handler.exposeAddress();
    assertThat(address.getVolumeName()).isEmpty();
    assertThat(address.getBucketName()).isEmpty();
    assertThat(address.getKeyName()).isEmpty();
  }

  @Test
  void testParseVolumeBucketPrefix() throws Exception {
    TestableWatchHandler handler = new TestableWatchHandler();
    new CommandLine(handler).parseArgs("/vol1/bucket1");
    OzoneAddress address = handler.exposeAddress();
    assertThat(address.getVolumeName()).isEqualTo("vol1");
    assertThat(address.getBucketName()).isEqualTo("bucket1");
    assertThat(address.getKeyName()).isEmpty();
  }

  @Test
  void testInotifyDisabledShowsFriendlyError() throws Exception {
    TestableWatchHandler handler = new TestableWatchHandler();
    new CommandLine(handler).parseArgs("/");
    OzoneAddress address = handler.exposeAddress();

    OzoneClient client = mock(OzoneClient.class);
    ClientProtocol protocol = mock(ClientProtocol.class);
    when(client.getProxy()).thenReturn(protocol);
    when(protocol.getInotifyEvents(org.mockito.ArgumentMatchers.any()))
        .thenThrow(new OMException("Inotify API is disabled.",
            OMException.ResultCodes.FEATURE_NOT_ENABLED));

    handler.publicExecute(client, address);

    assertThat(errContent.toString(DEFAULT_ENCODING))
        .contains("Inotify API is disabled on the OM");
  }
}
