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

import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.InotifyOpType;
import org.apache.hadoop.ozone.om.helpers.InotifyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InotifyRequest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import picocli.CommandLine;

class TestWatchHandler {

  @Test
  void testFetchOnceBuildsRequest() throws Exception {
    WatchHandler handler = new WatchHandler();
    new CommandLine(handler).parseArgs(
        "--recursive=false",
        "--limit=42",
        "--op-type=read",
        "o3:///vol1/bucket1/prefix");

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
    assertThat(request.getPathPrefix()).isEqualTo("vol1/bucket1/prefix");
    assertThat(request.getOpType().name()).isEqualTo("READ");
    assertThat(response).isNotNull();
  }
}
