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

package org.apache.hadoop.ozone.client.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.MockDatanodePipeline;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmAppendSession;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionKey;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link KeyDataStreamOutput} exercised through the
 * {@link org.apache.hadoop.hdds.scm.storage.ByteBufferStreamOutput} interface with mocked datanode pipeline and OM
 * client.
 *
 * <p>These tests verify the key-level stream behavior: block allocation, hsync→OM integration, retry on container
 * close, and atomic key commit.
 *
 */
class TestKeyDataStreamOutput {

  private static final int CHUNK_SIZE = 100;
  private static final long DS_FLUSH_SIZE = 400;
  private static final long STREAM_WINDOW = 500;
  private static final long BLOCK_SIZE = 800;
  private static final long APPEND_PREFIX_LENGTH = 1234;
  private static final long APPEND_SESSION_ID = 7L;

  private static OzoneClientConfig createConfig() {
    OzoneClientConfig config = new OzoneClientConfig();
    config.setDataStreamMinPacketSize(CHUNK_SIZE);
    config.setDataStreamBufferFlushSize(DS_FLUSH_SIZE);
    config.setStreamWindowSize(STREAM_WINDOW);
    config.setStreamBufferSize(CHUNK_SIZE);
    config.setStreamBufferFlushSize(DS_FLUSH_SIZE);
    config.setStreamBufferMaxSize(2 * DS_FLUSH_SIZE);
    config.setStreamBufferFlushDelay(false);
    config.setChecksumType(ContainerProtos.ChecksumType.NONE);
    config.setBytesPerChecksum(CHUNK_SIZE);
    return config;
  }

  /**
   * Creates a shared XceiverClientFactory that routes acquireClient calls
   * to the correct MockDatanodePipeline based on pipeline ID.
   */
  private XceiverClientFactory createSharedClientFactory(MockDatanodePipeline... pipelines) throws IOException {
    XceiverClientFactory factory = mock(XceiverClientFactory.class);
    doAnswer(invocation -> {
      Pipeline p = invocation.getArgument(0);
      for (MockDatanodePipeline pipeline : pipelines) {
        if (pipeline.getPipeline().getId().equals(p.getId())) {
          return pipeline.getXceiverClient();
        }
      }
      throw new IOException("Unknown pipeline: " + p.getId());
    }).when(factory).acquireClient(any(Pipeline.class), anyBoolean());

    doAnswer(invocation -> {
      Pipeline p = invocation.getArgument(0);
      for (MockDatanodePipeline pipeline : pipelines) {
        if (pipeline.getPipeline().getId().equals(p.getId())) {
          return pipeline.getXceiverClient();
        }
      }
      throw new IOException("Unknown pipeline: " + p.getId());
    }).when(factory).acquireClient(any(Pipeline.class));

    return factory;
  }

  /**
   * Creates a KeyDataStreamOutput with a mocked OM client that allocates blocks from the given mocked pipelines.
   * Each call to allocateBlock returns a block on the next pipeline in the list.
   */
  private KeyDataStreamOutput createKeyStream(OzoneManagerProtocol omClient, MockDatanodePipeline... pipelines)
      throws Exception {

    OzoneClientConfig config = createConfig();
    ReplicationConfig replicationConfig = RatisReplicationConfig.getInstance(ReplicationFactor.THREE);

    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setKeyName("testkey")
        .setDataSize(BLOCK_SIZE)
        .setReplicationConfig(replicationConfig)
        .build();

    OpenKeySession session = new OpenKeySession(1L, keyInfo, 0L);

    XceiverClientFactory sharedFactory = createSharedClientFactory(pipelines);

    KeyDataStreamOutput keyStream = new KeyDataStreamOutput(
        config,
        session,
        sharedFactory,
        omClient,
        CHUNK_SIZE,
        "test-request-id",
        replicationConfig,
        null,  // uploadID
        0,     // partNumber
        false, // isMultipart
        false // unsafeByteBufferConversion
    );

    // Pre-allocate the first block on mocked pipelines[0]
    OmKeyLocationInfo firstBlock = new OmKeyLocationInfo.Builder()
        .setBlockID(pipelines[0].getBlockID())
        .setPipeline(pipelines[0].getPipeline())
        .setLength(BLOCK_SIZE)
        .build();
    OmKeyLocationInfoGroup version = new OmKeyLocationInfoGroup(0, Collections.singletonList(firstBlock));
    keyStream.addPreallocateBlocks(version, 0);

    return keyStream;
  }

  /**
   * Creates a mock OM client that allocates blocks from mocked pipelines, starting from the given index.
   */
  private OzoneManagerProtocol createOmClient(MockDatanodePipeline... pipelines) throws IOException {
    OzoneManagerProtocol omClient = mock(OzoneManagerProtocol.class);
    AtomicInteger allocIndex = new AtomicInteger(0);
    doAnswer(invocation -> {
      int idx = allocIndex.getAndIncrement();
      if (idx >= pipelines.length) {
        throw new IOException("No more blocks to allocate");
      }
      MockDatanodePipeline pipeline = pipelines[idx];
      return new OmKeyLocationInfo.Builder()
          .setBlockID(pipeline.getBlockID())
          .setPipeline(pipeline.getPipeline())
          .setLength(BLOCK_SIZE)
          .build();
    }).when(omClient).allocateBlock(any(OmKeyArgs.class), anyLong(), any(ExcludeList.class));
    return omClient;
  }

  @Test
  void writeAndCloseCommitsKey() throws Exception {
    MockDatanodePipeline pipeline = new MockDatanodePipeline();
    OzoneManagerProtocol omClient = createOmClient(pipeline);

    try (KeyDataStreamOutput stream = createKeyStream(omClient, pipeline)) {
      writeRandom(stream, 300);
    }

    verify(omClient, times(1)).commitKey(any(OmKeyArgs.class), anyLong());
  }

  @Test
  void writeCrossBlockBoundary() throws Exception {
    MockDatanodePipeline pipeline1 = new MockDatanodePipeline(new BlockID(1, 1));
    MockDatanodePipeline pipeline2 = new MockDatanodePipeline(new BlockID(2, 2));

    // OM returns pipeline2 when allocateBlock is called
    OzoneManagerProtocol omClient = createOmClient(pipeline2);

    // The first block (pipeline1) has BLOCK_SIZE=800 capacity. Both mocks must be known to the shared client factory.
    try (KeyDataStreamOutput stream = createKeyStream(omClient, pipeline1, pipeline2)) {
      writeRandom(stream, 850);
    }

    // allocateBlock should have been called for the second block
    verify(omClient, times(1)).allocateBlock(any(OmKeyArgs.class), anyLong(), any(ExcludeList.class));
    verify(omClient, times(1)).commitKey(any(OmKeyArgs.class), anyLong());

    // pipeline1 should have received 800 bytes, pipeline2 should have received 50
    assertEquals(800, totalReceived(pipeline1));
    assertEquals(50, totalReceived(pipeline2));
  }

  @Test
  void hsyncCallsOmHsyncKey() throws Exception {
    MockDatanodePipeline pipeline = new MockDatanodePipeline();
    OzoneManagerProtocol omClient = createOmClient(pipeline);

    try (KeyDataStreamOutput stream = createKeyStream(omClient, pipeline)) {
      writeRandom(stream, 200);
      stream.hsync();

      verify(omClient, times(1)).hsyncKey(any(OmKeyArgs.class), anyLong());
    }
  }

  @Test
  void hsyncWithBlockErrorDoesNotCallOmHsync() throws Exception {
    MockDatanodePipeline pipeline = new MockDatanodePipeline();
    // First putBlock will fail
    pipeline.failPutBlockAfter(0, () -> new IOException("putBlock failed"));

    OzoneManagerProtocol omClient = createOmClient(pipeline);

    KeyDataStreamOutput stream = createKeyStream(omClient, pipeline);
    writeRandom(stream, 200);

    // hsync should throw because the block-level flush failed
    assertThrows(IOException.class, stream::hsync, "hsync() must throw when block-level flush fails");

    // OM hsyncKey must NOT have been called — data was not committed
    verify(omClient, never()).hsyncKey(any(OmKeyArgs.class), anyLong());

    stream.close();
  }

  @Test
  void containerCloseTriggersRetryOnNewBlock() throws Exception {
    MockDatanodePipeline pipeline1 = new MockDatanodePipeline(new BlockID(1, 1));
    MockDatanodePipeline pipeline2 = new MockDatanodePipeline(new BlockID(2, 2));

    // First pipeline: putBlock fails with ContainerNotOpen
    pipeline1.failPutBlockAfter(0,
        () -> new StorageContainerException("Container closed", ContainerProtos.Result.CLOSED_CONTAINER_IO));

    OzoneManagerProtocol omClient = createOmClient(pipeline2);

    try (KeyDataStreamOutput stream = createKeyStream(omClient, pipeline1, pipeline2)) {
      writeRandom(stream, 200);
      // The flush on close will hit the container closed error, trigger exception handling, allocate a new block on
      // pipeline2, and retry to write there.
    }

    // allocateBlock should have been called (for the retry block)
    verify(omClient).allocateBlock(any(OmKeyArgs.class), anyLong(), any(ExcludeList.class));
    verify(omClient).commitKey(any(OmKeyArgs.class), anyLong());
  }

  @Test
  void multipleHsyncsCallOmAtLeastOnce() throws Exception {
    MockDatanodePipeline pipeline = new MockDatanodePipeline();
    OzoneManagerProtocol omClient = createOmClient(pipeline);

    try (KeyDataStreamOutput stream = createKeyStream(omClient, pipeline)) {
      writeRandom(stream, 200);
      stream.hsync();

      writeRandom(stream, 200);
      stream.hsync();

      // hsyncKey is called at least once; the second call is skipped because the block ID hasn't changed
      // (OM optimization at BlockDataStreamOutputEntryPool.hsyncKey line 172).
      verify(omClient, times(1)).hsyncKey(any(OmKeyArgs.class), anyLong());

      // But both hsyncs should have flushed data to the datanode
      assertEquals(400, totalReceived(pipeline));
    }
  }

  @Test
  void writeAfterCloseThrows() throws Exception {
    MockDatanodePipeline pipeline = new MockDatanodePipeline();
    KeyDataStreamOutput stream = createKeyStream(createOmClient(pipeline), pipeline);

    writeRandom(stream, 100);
    stream.close();

    assertThrows(IOException.class, () -> writeRandom(stream, 100), "write() after close() should throw");
  }

  @Test
  void appendPublishesSuffixAndTotalLength() throws Exception {
    MockDatanodePipeline pipeline1 = new MockDatanodePipeline(new BlockID(1, 1));
    MockDatanodePipeline pipeline2 = new MockDatanodePipeline(new BlockID(2, 2));
    OzoneManagerProtocol omClient = createOmClient(pipeline1, pipeline2);

    try (KeyDataStreamOutput stream = createAppendStream(omClient, APPEND_SESSION_ID, pipeline1, pipeline2)) {
      assertThat(stream.getAppendPrefixLength()).isEqualTo(APPEND_PREFIX_LENGTH);
      // Nothing to publish before the first write, and no block before it either. OM is asked all the same: only
      // its answer tells a writer that the session is gone.
      stream.hsync();
      assertAppendPublication(lastHsync(omClient, 1));
      verify(omClient, never()).allocateBlock(any(OmKeyArgs.class), anyLong(), any(ExcludeList.class));

      // A datanode holds the tail of an open block in memory: an hsync does not publish the open block.
      writeRandom(stream, 300);
      stream.hsync();
      assertAppendPublication(lastHsync(omClient, 2));

      // Crosses the block boundary: the closed block is published with the total length up to its end.
      writeRandom(stream, 700);
      stream.hsync();
      assertAppendPublication(lastHsync(omClient, 3), BLOCK_SIZE);

      // Nothing more was closed since the last publication: the same publication again.
      writeRandom(stream, 50);
      stream.hsync();
      assertAppendPublication(lastHsync(omClient, 4), BLOCK_SIZE);
    }

    ArgumentCaptor<OmKeyArgs> commit = ArgumentCaptor.forClass(OmKeyArgs.class);
    verify(omClient).commitKey(commit.capture(), eq(APPEND_SESSION_ID));
    assertAppendPublication(commit.getValue(), BLOCK_SIZE, 250);
    verify(omClient, times(2)).allocateBlock(any(OmKeyArgs.class), eq(APPEND_SESSION_ID), any(ExcludeList.class));
    assertEquals(BLOCK_SIZE, totalReceived(pipeline1));
    assertEquals(250, totalReceived(pipeline2));
  }

  @Test
  void hsyncOfFencedAppendFails() throws Exception {
    MockDatanodePipeline pipeline = new MockDatanodePipeline();
    OzoneManagerProtocol omClient = createOmClient(pipeline);
    KeyDataStreamOutput stream = createAppendStream(omClient, APPEND_SESSION_ID, pipeline);
    writeRandom(stream, 100);
    stream.hsync();

    // The session was recovered or the file deleted. No block is closed, so the publication does not change.
    doThrow(new OMException("not active", OMException.ResultCodes.APPEND_SESSION_NOT_FOUND))
        .when(omClient).hsyncKey(any(OmKeyArgs.class), anyLong());
    writeRandom(stream, 100);
    assertThatThrownBy(stream::hsync).isInstanceOf(OMException.class);
  }

  @Test
  void appendHsyncFailsAfterFailedBlock() throws Exception {
    MockDatanodePipeline pipeline1 = new MockDatanodePipeline(new BlockID(1, 1));
    MockDatanodePipeline pipeline2 = new MockDatanodePipeline(new BlockID(2, 2));
    MockDatanodePipeline pipeline3 = new MockDatanodePipeline(new BlockID(3, 3));
    // The first block acknowledges 200 bytes, then its container closes under the writer.
    pipeline1.failPutBlockAfter(1,
        () -> new StorageContainerException("Container closed", ContainerProtos.Result.CLOSED_CONTAINER_IO));
    OzoneManagerProtocol omClient = createOmClient(pipeline1, pipeline2, pipeline3);

    try (KeyDataStreamOutput stream =
        createAppendStream(omClient, APPEND_SESSION_ID, pipeline1, pipeline2, pipeline3)) {
      writeRandom(stream, 200);
      stream.hsync();
      writeRandom(stream, 100);
      // The failed block was never closed by its datanodes, so neither it nor a block after it can be published.
      assertThatThrownBy(stream::hsync).isInstanceOf(IOException.class).hasMessageContaining("Only close()");
      List<OmKeyLocationInfo> blocks = stream.getLocationInfoList();
      assertThat(blocks).extracting(OmKeyLocationInfo::getLength).containsExactly(200L, 100L);

      // Nor once the block after the failed one is closed.
      writeRandom(stream, BLOCK_SIZE);
      assertThatThrownBy(stream::hsync).isInstanceOf(IOException.class).hasMessageContaining("Only close()");
      assertThat(stream.getLocationInfoList()).hasSize(3);
      assertAppendPublication(lastHsync(omClient, 1));
    }
    // The stream stays usable after the failed hsync, and close publishes everything in order.
    ArgumentCaptor<OmKeyArgs> commit = ArgumentCaptor.forClass(OmKeyArgs.class);
    verify(omClient).commitKey(commit.capture(), eq(APPEND_SESSION_ID));
    assertAppendPublication(commit.getValue(), 200, BLOCK_SIZE, 100);
  }

  @Test
  void emptyAppendCloseEndsSession() throws Exception {
    OzoneManagerProtocol omClient = createOmClient();

    createAppendStream(omClient, APPEND_SESSION_ID).close();

    ArgumentCaptor<OmKeyArgs> commit = ArgumentCaptor.forClass(OmKeyArgs.class);
    verify(omClient).commitKey(commit.capture(), eq(APPEND_SESSION_ID));
    assertAppendPublication(commit.getValue());
    verify(omClient, never()).allocateBlock(any(OmKeyArgs.class), anyLong(), any(ExcludeList.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void appendLeaseRenewerFencesStream() throws Exception {
    MockDatanodePipeline pipeline = new MockDatanodePipeline();
    OzoneManagerProtocol omClient = createOmClient(pipeline);
    AtomicBoolean renewable = new AtomicBoolean(true);
    doAnswer(invocation -> Collections.nCopies(invocation.<List<AppendSessionKey>>getArgument(0).size(),
        renewable.get())).when(omClient).renewAppendLeases(any());
    // The interval is long: this test runs the renewals itself.
    try (AppendLeaseRenewer renewer = new AppendLeaseRenewer(omClient, Duration.ofHours(1))) {
      KeyDataStreamOutput stream = createAppendStream(omClient, APPEND_SESSION_ID, pipeline);
      KeyDataStreamOutput other = createAppendStream(omClient, APPEND_SESSION_ID + 1);
      KeyDataStreamOutput closed = createAppendStream(omClient, APPEND_SESSION_ID + 2);
      renewer.register(stream);
      renewer.register(other);
      renewer.register(closed);
      // A closed stream is not renewed any more.
      closed.close();
      renewer.renew();
      ArgumentCaptor<List<AppendSessionKey>> renewed = ArgumentCaptor.forClass(List.class);
      verify(omClient).renewAppendLeases(renewed.capture());
      assertThat(renewed.getValue()).containsExactlyInAnyOrder(
          AppendSessionKey.newBuilder().setVolumeName("vol").setBucketName("bucket")
              .setSessionId(APPEND_SESSION_ID).build(),
          AppendSessionKey.newBuilder().setVolumeName("vol").setBucketName("bucket")
              .setSessionId(APPEND_SESSION_ID + 1).build());
      writeRandom(stream, 100);
      stream.hsync();

      // OM answers false: the next write, hsync or close fails and nothing more is published.
      renewable.set(false);
      renewer.renew();
      clearInvocations(omClient);
      assertThatThrownBy(() -> writeRandom(other, 100)).hasMessageContaining("append lease was lost");
      assertThatThrownBy(stream::hsync).hasMessageContaining("append lease was lost");
      assertThatThrownBy(stream::close).hasMessageContaining("append lease was lost");
      verify(omClient, never()).hsyncKey(any(OmKeyArgs.class), anyLong());
      verify(omClient, never()).commitKey(any(OmKeyArgs.class), anyLong());
      renewer.renew();
      verify(omClient, never()).renewAppendLeases(any());
    }
  }

  // --- Helpers ---

  /** An append stream has no preallocated block: every block of the suffix comes from allocateBlock. */
  private KeyDataStreamOutput createAppendStream(OzoneManagerProtocol omClient, long sessionId,
      MockDatanodePipeline... pipelines) throws IOException {
    ReplicationConfig replicationConfig = RatisReplicationConfig.getInstance(ReplicationFactor.THREE);
    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setKeyName("testkey")
        .setDataSize(APPEND_PREFIX_LENGTH)
        .setReplicationConfig(replicationConfig)
        .setAppendSession(OmAppendSession.newActive(APPEND_PREFIX_LENGTH, 1, 0))
        .build();
    return new KeyDataStreamOutput.Builder()
        .setConfig(createConfig())
        .setHandler(new OpenKeySession(sessionId, keyInfo, 0L))
        .setXceiverClientManager(createSharedClientFactory(pipelines))
        .setOmClient(omClient)
        .setReplicationConfig(replicationConfig)
        .build();
  }

  private static OmKeyArgs lastHsync(OzoneManagerProtocol omClient, int count) throws IOException {
    ArgumentCaptor<OmKeyArgs> hsync = ArgumentCaptor.forClass(OmKeyArgs.class);
    verify(omClient, times(count)).hsyncKey(hsync.capture(), eq(APPEND_SESSION_ID));
    return hsync.getValue();
  }

  /** An hsync or commit of an append carries the blocks of the suffix only and the total file length. */
  private static void assertAppendPublication(OmKeyArgs keyArgs, long... suffixBlockLengths) {
    assertThat(keyArgs.getLocationInfoList()).extracting(OmKeyLocationInfo::getLength)
        .containsExactly(Arrays.stream(suffixBlockLengths).boxed().toArray(Long[]::new));
    assertThat(keyArgs.getDataSize()).isEqualTo(APPEND_PREFIX_LENGTH + Arrays.stream(suffixBlockLengths).sum());
  }

  private static int totalReceived(MockDatanodePipeline pipeline) {
    return pipeline.getReceivedChunks().stream().mapToInt(c -> c.length).sum();
  }

  private static void writeRandom(KeyDataStreamOutput stream, long length) throws IOException {
    stream.write(ByteBuffer.wrap(RandomUtils.secure().randomBytes((int) length)), 0, (int) length);
  }
}
