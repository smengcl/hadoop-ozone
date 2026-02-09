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

package org.apache.hadoop.hdds.scm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.ratis.client.api.DataStreamApi;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.protocol.DataStreamReply;
import org.junit.jupiter.api.Test;

class TestBlockDataStreamOutput {

  @Test
  void testFlushWindowPrecomputed() throws Exception {
    OzoneClientConfig config = new OzoneClientConfig();
    config.setDataStreamMinPacketSize(256);
    config.setDataStreamBufferFlushSize(1024);
    config.setStreamWindowSize(2048);

    BlockDataStreamOutput subject = newOutput(config, new ArrayList<>());
    try {
      Field flushBoundaryField = BlockDataStreamOutput.class.getDeclaredField("flushBoundary");
      Field streamWindowField = BlockDataStreamOutput.class.getDeclaredField("streamWindow");
      flushBoundaryField.setAccessible(true);
      streamWindowField.setAccessible(true);

      long flushBoundary = (long) flushBoundaryField.get(subject);
      long streamWindow = (long) streamWindowField.get(subject);

      assertEquals(4L, flushBoundary);
      assertEquals(8L, streamWindow);
    } finally {
      subject.cleanup(false);
    }
  }

  @Test
  void testWriteOnRetryPopulatesBuffersForPutBlock() throws Exception {
    OzoneClientConfig config = new OzoneClientConfig();
    config.setDataStreamMinPacketSize(256);
    config.setDataStreamBufferFlushSize(1024);
    config.setStreamWindowSize(2048);
    config.setBytesPerChecksum(2);
    config.setChecksumType(ContainerProtos.ChecksumType.CRC32);

    List<StreamBuffer> bufferList = new ArrayList<>();
    bufferList.add(streamBufferWithSize(4));
    bufferList.add(streamBufferWithSize(4));

    BlockDataStreamOutput subject = newOutput(config, bufferList);
    try {
      subject.writeOnRetry(8);

      Field buffersForPutBlockField =
          BlockDataStreamOutput.class.getDeclaredField("buffersForPutBlock");
      buffersForPutBlockField.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<StreamBuffer> buffersForPutBlock =
          (List<StreamBuffer>) buffersForPutBlockField.get(subject);

      assertNotNull(buffersForPutBlock);
      assertEquals(2, buffersForPutBlock.size());
    } finally {
      subject.cleanup(false);
    }
  }

  private static StreamBuffer streamBufferWithSize(int size) {
    ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.position(size);
    return new StreamBuffer(buffer);
  }

  private static BlockDataStreamOutput newOutput(OzoneClientConfig config,
      List<StreamBuffer> bufferList) throws IOException {
    Pipeline pipeline = MockPipeline.createRatisPipeline();
    BlockID blockID = new BlockID(1, 1);

    DataStreamReply reply = mock(DataStreamReply.class);
    when(reply.isSuccess()).thenReturn(true);
    when(reply.getCommitInfos()).thenReturn(Collections.emptyList());

    DataStreamOutput out = mock(DataStreamOutput.class);
    when(out.writeAsync(any(ByteBuffer.class)))
        .thenReturn(CompletableFuture.completedFuture(reply));
    when(out.writeAsync(any(ByteBuffer.class), eq(StandardWriteOption.SYNC)))
        .thenReturn(CompletableFuture.completedFuture(reply));
    when(out.writeAsync(any(ByteBuffer.class), eq(StandardWriteOption.CLOSE)))
        .thenReturn(CompletableFuture.completedFuture(reply));

    DataStreamApi dataStreamApi = mock(DataStreamApi.class);
    when(dataStreamApi.stream(any(ByteBuffer.class))).thenReturn(out);
    when(dataStreamApi.stream(any(ByteBuffer.class), any())).thenReturn(out);

    XceiverClientRatis xceiverClient = mock(XceiverClientRatis.class);
    when(xceiverClient.getPipeline()).thenReturn(pipeline);
    when(xceiverClient.getDataStreamApi()).thenReturn(dataStreamApi);
    when(xceiverClient.updateCommitInfosMap(any(Collection.class))).thenReturn(0L);

    XceiverClientFactory factory = mock(XceiverClientFactory.class);
    when(factory.acquireClient(eq(pipeline), eq(true))).thenReturn(xceiverClient);

    return new BlockDataStreamOutput(
        blockID, factory, pipeline, config, null, bufferList);
  }
}
