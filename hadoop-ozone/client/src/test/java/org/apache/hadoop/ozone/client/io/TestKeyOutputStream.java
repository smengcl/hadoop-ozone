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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamBufferArgs;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.om.helpers.OmAppendSession;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionKey;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.slf4j.event.Level;

/**
 * Tests KeyOutputStream.
 * This is a unit test meant to verify specific behaviors of KeyOutputStream.
 */
public class TestKeyOutputStream {

  @BeforeAll
  static void init() {
    GenericTestUtils.setLogLevel(KeyOutputStreamSemaphore.class, Level.DEBUG);
  }

  @Test
  void testConcurrentWriteLimitOne() throws Exception {
    // Verify the semaphore is working to limit the number of concurrent writes allowed.
    KeyOutputStreamSemaphore sema1 = new KeyOutputStreamSemaphore(1);
    KeyOutputStream keyOutputStream = spy(KeyOutputStream.class);
    when(keyOutputStream.getRequestSemaphore()).thenReturn(sema1);

    final AtomicInteger countWrite = new AtomicInteger(0);
    // mock write()
    doAnswer(invocation -> {
      countWrite.getAndIncrement();
      return invocation.callRealMethod();
    }).when(keyOutputStream).write(any(), anyInt(), anyInt());

    final ConcurrentHashMap<Long, CountDownLatch> mapNotifiers = new ConcurrentHashMap<>();

    final AtomicInteger countHandleWrite = new AtomicInteger(0);
    // mock handleWrite()
    doAnswer(invocation -> {
      final long tid = Thread.currentThread().getId();
      System.out.println("handleWrite() called from tid " + tid);
      final CountDownLatch latch = mapNotifiers.compute(tid, (k, v) ->
          v != null ? v : new CountDownLatch(1));
      countHandleWrite.getAndIncrement();
      // doing some "work"
      latch.await();
      return null;
    }).when(keyOutputStream).handleWrite(any(), anyInt(), anyLong(), anyBoolean());

    final Runnable writeRunnable = () -> {
      try {
        keyOutputStream.write(new byte[4], 0, 4);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };

    final Thread thread1 = new Thread(writeRunnable);
    thread1.start();

    final Thread thread2 = new Thread(writeRunnable);
    thread2.start();

    // Wait for both threads to enter write()
    GenericTestUtils.waitFor(() -> countWrite.get() == 2, 100, 3000);
    // One thread should enter handleWrite()
    GenericTestUtils.waitFor(() -> countHandleWrite.get() == 1, 100, 3000);
    // The other thread is waiting on the semaphore
    GenericTestUtils.waitFor(() -> sema1.getQueueLength() == 1, 100, 3000);

    // handleWrite is triggered only once because of the semaphore and the synchronized block
    verify(keyOutputStream, times(1)).handleWrite(any(), anyInt(), anyLong(), anyBoolean());

    // Now, allow the current thread to finish handleWrite
    // There is only one thread in handleWrite() so mapNotifiers should have only one entry.
    assertEquals(1, mapNotifiers.size());
    Entry<Long, CountDownLatch> entry = mapNotifiers.entrySet().stream().findFirst().get();
    mapNotifiers.remove(entry.getKey());
    entry.getValue().countDown();

    // Wait for the other thread to proceed
    GenericTestUtils.waitFor(() -> countHandleWrite.get() == 2, 100, 3000);
    verify(keyOutputStream, times(2)).handleWrite(any(), anyInt(), anyLong(), anyBoolean());

    // Allow the other thread to finish handleWrite
    entry = mapNotifiers.entrySet().stream().findFirst().get();
    mapNotifiers.remove(entry.getKey());
    entry.getValue().countDown();

    // Let threads finish
    thread2.join();
    thread1.join();
  }
  
  @Test
  void testGetCurrentStreamEntryDuringBlockAllocation() throws Exception {
    CountDownLatch allocationStarted = new CountDownLatch(1);
    CountDownLatch finishAllocation = new CountDownLatch(1);
    OzoneManagerProtocol om = mock(OzoneManagerProtocol.class);
    OmKeyLocationInfo next = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(1, 2)).setLength(1024)
        .setPipeline(mock(Pipeline.class)).build();
    // Pause inside allocateBlock() to keep the pool in the intermediate state.
    when(om.allocateBlock(any(), anyLong(), any())).thenAnswer(invocation -> {
      allocationStarted.countDown();
      assertTrue(finishAllocation.await(10, TimeUnit.SECONDS));
      return next;
    });
    BlockOutputStreamEntryPool pool = new BlockOutputStreamEntryPool(new KeyOutputStream.Builder()
        .setConfig(new OzoneClientConfig()).setOmClient(om)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setHandler(new OpenKeySession(1, new OmKeyInfo.Builder()
            .setVolumeName("v").setBucketName("b").setKeyName("k").build(), 0))
        .setStreamBufferArgs(StreamBufferArgs.Builder.getNewBuilder()
            .setBufferSize(1024).setBufferFlushSize(1024).setBufferMaxSize(2048).build()));
    // Add a closed entry so allocateBlockIfNeeded() must advance the index and allocate.
    BlockOutputStreamEntry closed = mock(BlockOutputStreamEntry.class);
    when(closed.isClosed()).thenReturn(true);
    pool.getStreamEntries().add(closed);

    // writer allocates (holds the pool monitor) and the reader does the concurrent lookup (as hsync).
    FutureTask<BlockOutputStreamEntry> allocation = new FutureTask<>(() -> pool.allocateBlockIfNeeded(false));
    FutureTask<BlockOutputStreamEntry> read = new FutureTask<>(pool::getCurrentStreamEntry);
    Thread writer = new Thread(allocation, "block-allocator");
    Thread reader = new Thread(read, "entry-reader");
    ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    writer.start();
    try {
      // Wait until the writer is paused inside allocateBlock().
      assertTrue(allocationStarted.await(5, TimeUnit.SECONDS));
      reader.start();
      // Deterministic sync proceed once reader is done or BLOCKED on the pool monitor.
      GenericTestUtils.waitFor(() -> {
        ThreadInfo info = threads.getThreadInfo(reader.getId());
        return read.isDone() || (info != null && info.getThreadState() == Thread.State.BLOCKED
            && info.getLockOwnerId() == writer.getId()
            && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(pool));
      }, 10, 5000);
    } finally {
      // Release allocation (appends the entry) and wait for both threads.
      finishAllocation.countDown();
      writer.join(5000);
      reader.join(5000);
    }
    assertFalse(writer.isAlive(), "Allocation should complete");
    assertFalse(reader.isAlive(), "Lookup should complete");
    // Reader must see the allocated entry, not the intermediate null (fails without the fix).
    assertSame(allocation.get(5, TimeUnit.SECONDS), read.get(5, TimeUnit.SECONDS),
        "Reader must not observe the intermediate index before the new entry is added");
  }

  @Test
  void testAppendLeaseRenewer() throws Exception {
    OzoneManagerProtocol om = mock(OzoneManagerProtocol.class);
    Set<Long> lostSessions = new HashSet<>();
    Answer<List<Boolean>> renewUnlessLost = invocation -> invocation.<List<AppendSessionKey>>getArgument(0).stream()
        .map(session -> !lostSessions.contains(session.getSessionId())).collect(Collectors.toList());
    doAnswer(renewUnlessLost).when(om).renewAppendLeases(any());
    // The interval is long: this test runs the renewals itself.
    try (AppendLeaseRenewer renewer = new AppendLeaseRenewer(om, Duration.ofHours(1))) {
      List<KeyOutputStream> streams = new ArrayList<>();
      for (long sessionId = 0; sessionId <= AppendLeaseRenewer.BATCH_SIZE; sessionId++) {
        streams.add(newAppendStream(om, sessionId));
        renewer.register(streams.get((int) sessionId));
      }

      renewer.renew();
      List<List<AppendSessionKey>> batches = renewAppendLeasesRequests(om, 2);
      assertThat(batches).extracting(List::size).containsExactlyInAnyOrder(AppendLeaseRenewer.BATCH_SIZE, 1);
      assertThat(batches.stream().flatMap(List::stream)).extracting(AppendSessionKey::getSessionId)
          .containsExactlyInAnyOrderElementsOf(LongStream.rangeClosed(0, AppendLeaseRenewer.BATCH_SIZE).boxed()
              .collect(Collectors.toList()));
      assertThat(batches.get(0).get(0).getVolumeName()).isEqualTo("v");
      assertThat(batches.get(0).get(0).getBucketName()).isEqualTo("b");

      // A failed renewal request fences nothing.
      doThrow(new IOException("OM is not reachable")).when(om).renewAppendLeases(any());
      renewer.renew();
      streams.get(1).getBlockOutputStreamEntryPool().hsyncKey(0);

      // OM answers false for two sessions: their next write, hsync or close fails.
      doAnswer(renewUnlessLost).when(om).renewAppendLeases(any());
      lostSessions.add(1L);
      lostSessions.add(2L);
      renewer.renew();
      assertThatThrownBy(() -> streams.get(1).write(1))
          .isInstanceOf(IOException.class).hasMessageContaining("append lease was lost");
      assertThatThrownBy(() -> streams.get(2).getBlockOutputStreamEntryPool().hsyncKey(0))
          .isInstanceOf(IOException.class).hasMessageContaining("append lease was lost");
      assertThatThrownBy(() -> streams.get(2).close())
          .isInstanceOf(IOException.class).hasMessageContaining("append lease was lost");
      verify(om, never()).commitKey(any(), anyLong());

      // Closed and fenced streams are not renewed any more.
      streams.get(3).close();
      verify(om).commitKey(any(), eq(3L));
      clearInvocations(om);
      renewer.renew();
      assertThat(renewAppendLeasesRequests(om, 1).get(0)).extracting(AppendSessionKey::getSessionId)
          .hasSize(AppendLeaseRenewer.BATCH_SIZE - 2).doesNotContain(1L, 2L, 3L);
    }
  }

  @Test
  void testAppendLeaseRenewerRunsFromFirstStreamUntilClose() throws Exception {
    OzoneManagerProtocol om = mock(OzoneManagerProtocol.class);
    AppendLeaseRenewer renewer = new AppendLeaseRenewer(om, Duration.ofMillis(10));
    renewer.register(newAppendStream(om, 1));
    verify(om, timeout(10_000).atLeast(2)).renewAppendLeases(any());

    renewer.close();
    clearInvocations(om);
    Thread.sleep(100);
    verify(om, never()).renewAppendLeases(any());
  }

  @SuppressWarnings("unchecked")
  private static List<List<AppendSessionKey>> renewAppendLeasesRequests(OzoneManagerProtocol om, int count)
      throws IOException {
    ArgumentCaptor<List<AppendSessionKey>> captor = ArgumentCaptor.forClass(List.class);
    verify(om, times(count)).renewAppendLeases(captor.capture());
    return captor.getAllValues();
  }

  private static KeyOutputStream newAppendStream(OzoneManagerProtocol om, long sessionId) {
    return new KeyOutputStream.Builder()
        .setConfig(new OzoneClientConfig()).setOmClient(om)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setHandler(new OpenKeySession(sessionId, new OmKeyInfo.Builder()
            .setVolumeName("v").setBucketName("b").setKeyName("k" + sessionId)
            .setAppendSession(OmAppendSession.newActive(10, 1, 0)).build(), 0))
        .setStreamBufferArgs(StreamBufferArgs.Builder.getNewBuilder()
            .setBufferSize(1024).setBufferFlushSize(1024).setBufferMaxSize(2048).build())
        .build();
  }
}
