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

package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_NOT_FOUND;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.NO_SUCH_BLOCK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.om.helpers.LeaseKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmAppendSession;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tests which blocks {@link LeaseRecoveryClientDNHandler} finalizes and returns, with a mocked adapter in the place
 * of the datanodes. Blocks 1 and 2 are the prefix of an appended file, blocks 11 and up are its suffix.
 */
public class TestLeaseRecoveryClientDNHandler {

  private static final ReplicationConfig RATIS = RatisReplicationConfig.getInstance(ReplicationFactor.THREE);
  private static final long ALLOCATED = 256;
  private static final long PREFIX_LENGTH = 150;
  private static final OmAppendSession SESSION = OmAppendSession.newActive(PREFIX_LENGTH, 2, 0);

  private OzoneClientAdapter adapter;
  /** Answer of the datanodes per block local ID: the finalized length, or the exception to throw. */
  private final Map<Long, Object> datanodeBlocks = new HashMap<>();

  @BeforeEach
  public void setUp() throws IOException {
    adapter = mock(OzoneClientAdapter.class);
    when(adapter.finalizeBlock(any())).thenAnswer(invocation -> {
      Object answer = datanodeBlocks.get(invocation.<OmKeyLocationInfo>getArgument(0).getLocalID());
      if (answer instanceof IOException) {
        throw (IOException) answer;
      }
      return answer;
    });
  }

  @Test
  public void appendWithoutAllocationRecoversNothing() throws IOException {
    LeaseKeyInfo leaseKeyInfo = new LeaseKeyInfo(keyInfo(RATIS, null, block(1, 100), block(2, 50)),
        keyInfo(RATIS, SESSION));

    List<OmKeyLocationInfo> recovered = recover(leaseKeyInfo);

    assertThat(recovered).isEmpty();
    verify(adapter, never()).finalizeBlock(any());
    assertEquals(PREFIX_LENGTH, LeaseRecoveryClientDNHandler.getRecoveredLength(leaseKeyInfo, recovered));
  }

  @Test
  public void unpublishedSuffixIsFinalized() throws IOException {
    datanodeBlocks.put(11L, 200L);
    datanodeBlocks.put(12L, 30L);
    LeaseKeyInfo leaseKeyInfo = new LeaseKeyInfo(keyInfo(RATIS, null, block(1, 100), block(2, 50)),
        keyInfo(RATIS, SESSION, block(11, ALLOCATED), block(12, ALLOCATED)));

    List<OmKeyLocationInfo> recovered = recover(leaseKeyInfo);

    assertThat(recovered).extracting(OmKeyLocationInfo::getLocalID).containsExactly(11L, 12L);
    assertThat(recovered).extracting(OmKeyLocationInfo::getLength).containsExactly(200L, 30L);
    assertThat(finalizedBlocks()).containsExactly(11L, 12L);
    assertEquals(PREFIX_LENGTH + 230, LeaseRecoveryClientDNHandler.getRecoveredLength(leaseKeyInfo, recovered));
  }

  @Test
  public void unpublishedSuffixEndsAtFirstEmptyBlock() throws IOException {
    for (Object firstUnusedBlock : Arrays.asList(0L, new StorageContainerException(NO_SUCH_BLOCK),
        new StorageContainerException(CONTAINER_NOT_FOUND))) {
      setUp();
      datanodeBlocks.put(11L, firstUnusedBlock);
      datanodeBlocks.put(12L, 5L);
      LeaseKeyInfo leaseKeyInfo = new LeaseKeyInfo(keyInfo(RATIS, null, block(1, 100), block(2, 50)),
          keyInfo(RATIS, SESSION, block(11, ALLOCATED), block(12, ALLOCATED)));

      List<OmKeyLocationInfo> recovered = recover(leaseKeyInfo);

      assertThat(recovered).isEmpty();
      assertThat(finalizedBlocks()).containsExactly(11L);
      assertEquals(PREFIX_LENGTH, LeaseRecoveryClientDNHandler.getRecoveredLength(leaseKeyInfo, recovered));
    }
  }

  @Test
  public void unpublishedSuffixEndsAtBlockWithoutPipeline() throws IOException {
    // OM returns the pipeline of the last two open blocks only.
    datanodeBlocks.put(12L, 30L);
    datanodeBlocks.put(13L, 5L);
    LeaseKeyInfo leaseKeyInfo = new LeaseKeyInfo(keyInfo(RATIS, null, block(1, 100), block(2, 50)),
        keyInfo(RATIS, SESSION, block(11, ALLOCATED, null), block(12, ALLOCATED), block(13, ALLOCATED)));

    List<OmKeyLocationInfo> recovered = recover(leaseKeyInfo);

    assertThat(recovered).isEmpty();
    verify(adapter, never()).finalizeBlock(any());
    assertEquals(PREFIX_LENGTH, LeaseRecoveryClientDNHandler.getRecoveredLength(leaseKeyInfo, recovered));
  }

  @Test
  public void unpublishedSuffixFailsOnDatanodeErrorUnlessForced() throws IOException {
    datanodeBlocks.put(11L, 200L);
    datanodeBlocks.put(12L, new IOException("datanodes are not reachable"));
    LeaseKeyInfo leaseKeyInfo = new LeaseKeyInfo(keyInfo(RATIS, null, block(1, 100), block(2, 50)),
        keyInfo(RATIS, SESSION, block(11, ALLOCATED), block(12, ALLOCATED)));

    assertThatThrownBy(() -> LeaseRecoveryClientDNHandler.getOmKeyLocationInfos(leaseKeyInfo, adapter, false))
        .isInstanceOf(IOException.class).hasMessageContaining("not reachable");

    assertThat(LeaseRecoveryClientDNHandler.getOmKeyLocationInfos(leaseKeyInfo, adapter, true))
        .extracting(OmKeyLocationInfo::getLocalID).containsExactly(11L);
  }

  @Test
  public void publishedSuffixIsRecoveredWithoutThePrefix() throws IOException {
    // Block 11 is full and closed, the writer hsync'ed 10 bytes of block 12 and wrote 30.
    datanodeBlocks.put(12L, 30L);
    LeaseKeyInfo leaseKeyInfo = new LeaseKeyInfo(
        keyInfo(RATIS, null, block(1, 100), block(2, 50), block(11, ALLOCATED), block(12, 10)),
        keyInfo(RATIS, SESSION, block(11, ALLOCATED), block(12, ALLOCATED)));

    List<OmKeyLocationInfo> recovered = recover(leaseKeyInfo);

    assertThat(recovered).extracting(OmKeyLocationInfo::getLocalID).containsExactly(11L, 12L);
    assertThat(recovered).extracting(OmKeyLocationInfo::getLength).containsExactly(ALLOCATED, 30L);
    assertThat(finalizedBlocks()).containsExactly(12L);
    assertEquals(PREFIX_LENGTH + ALLOCATED + 30,
        LeaseRecoveryClientDNHandler.getRecoveredLength(leaseKeyInfo, recovered));
  }

  @Test
  public void blockAllocatedAfterLastPublicationIsRecovered() throws IOException {
    // The writer hsync'ed 200 bytes of block 11, filled it, and wrote 30 bytes to block 12 without hsync.
    datanodeBlocks.put(11L, ALLOCATED);
    datanodeBlocks.put(12L, 30L);
    // The published block has the block commit sequence ID of the hsync, the open record the one of the allocation.
    OmKeyLocationInfo published = block(11, 200);
    published.getBlockID().setBlockCommitSequenceId(5);
    LeaseKeyInfo leaseKeyInfo = new LeaseKeyInfo(
        keyInfo(RATIS, null, block(1, 100), block(2, 50), published),
        keyInfo(RATIS, SESSION, block(11, ALLOCATED), block(12, ALLOCATED)));

    List<OmKeyLocationInfo> recovered = recover(leaseKeyInfo);

    assertThat(recovered).extracting(OmKeyLocationInfo::getLocalID).containsExactly(11L, 12L);
    assertThat(recovered).extracting(OmKeyLocationInfo::getLength).containsExactly(ALLOCATED, 30L);
    assertThat(finalizedBlocks()).containsExactly(11L, 12L);
  }

  @Test
  public void abandonedBlockBeforeLastPublishedBlockIsNotRecovered() throws IOException {
    // Block 11 failed before any byte was acknowledged, the writer moved on to block 12 and hsync'ed 10 of 30 bytes.
    datanodeBlocks.put(12L, 30L);
    LeaseKeyInfo leaseKeyInfo = new LeaseKeyInfo(
        keyInfo(RATIS, null, block(1, 100), block(2, 50), block(12, 10)),
        keyInfo(RATIS, SESSION, block(11, ALLOCATED), block(12, ALLOCATED)));

    List<OmKeyLocationInfo> recovered = recover(leaseKeyInfo);

    assertThat(recovered).extracting(OmKeyLocationInfo::getLocalID).containsExactly(12L);
    assertThat(recovered).extracting(OmKeyLocationInfo::getLength).containsExactly(30L);
    assertThat(finalizedBlocks()).containsExactly(12L);
    assertEquals(PREFIX_LENGTH + 30, LeaseRecoveryClientDNHandler.getRecoveredLength(leaseKeyInfo, recovered));
  }

  @Test
  public void ecSuffixIsNotFinalized() throws IOException {
    ReplicationConfig ec = new ECReplicationConfig(3, 2);
    LeaseKeyInfo leaseKeyInfo = new LeaseKeyInfo(keyInfo(ec, null, block(1, 100), block(2, 50)),
        keyInfo(ec, SESSION, block(11, ALLOCATED)));

    List<OmKeyLocationInfo> recovered = recover(leaseKeyInfo);

    assertThat(recovered).isEmpty();
    verify(adapter, never()).finalizeBlock(any());
    assertEquals(PREFIX_LENGTH, LeaseRecoveryClientDNHandler.getRecoveredLength(leaseKeyInfo, recovered));
  }

  @Test
  public void fileWithoutAppendSessionKeepsAllBlocks() throws IOException {
    datanodeBlocks.put(2L, 80L);
    LeaseKeyInfo leaseKeyInfo = new LeaseKeyInfo(keyInfo(RATIS, null, block(1, 100), block(2, 50)),
        keyInfo(RATIS, null, block(1, ALLOCATED), block(2, ALLOCATED)));

    List<OmKeyLocationInfo> recovered = recover(leaseKeyInfo);

    assertThat(recovered).extracting(OmKeyLocationInfo::getLocalID).containsExactly(1L, 2L);
    assertThat(recovered).extracting(OmKeyLocationInfo::getLength).containsExactly(100L, 80L);
    assertEquals(180, LeaseRecoveryClientDNHandler.getRecoveredLength(leaseKeyInfo, recovered));
  }

  private List<OmKeyLocationInfo> recover(LeaseKeyInfo leaseKeyInfo) throws IOException {
    return LeaseRecoveryClientDNHandler.getOmKeyLocationInfos(leaseKeyInfo, adapter, false);
  }

  /** @return local IDs of the blocks the handler asked the datanodes to finalize, in order. */
  private List<Long> finalizedBlocks() throws IOException {
    ArgumentCaptor<OmKeyLocationInfo> blocks = ArgumentCaptor.forClass(OmKeyLocationInfo.class);
    verify(adapter, atLeast(1)).finalizeBlock(blocks.capture());
    return blocks.getAllValues().stream().map(OmKeyLocationInfo::getLocalID).collect(Collectors.toList());
  }

  private static OmKeyLocationInfo block(long localId, long length) {
    return block(localId, length, mock(Pipeline.class));
  }

  private static OmKeyLocationInfo block(long localId, long length, Pipeline pipeline) {
    return new OmKeyLocationInfo.Builder().setBlockID(new BlockID(1, localId)).setLength(length)
        .setPipeline(pipeline).build();
  }

  private static OmKeyInfo keyInfo(ReplicationConfig replication, OmAppendSession session,
      OmKeyLocationInfo... blocks) {
    return new OmKeyInfo.Builder().setVolumeName("vol").setBucketName("bucket").setKeyName("key")
        .setReplicationConfig(replication)
        .setOmKeyLocationInfos(Collections.singletonList(new OmKeyLocationInfoGroup(0, Arrays.asList(blocks))))
        .setAppendSession(session)
        .build();
  }
}
