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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_ALREADY_CLOSED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.ozone.om.exceptions.AppendConflictException;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.LeaseKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmAppendSession;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendConflictInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionPhase;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendWriterKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests the wait, recover and retry decisions of {@link AppendWithRecovery} with a fake clock.
 */
class TestAppendWithRecovery {
  private static final String KEY = "dir/file";
  private static final long MAX_WAIT_MS = 10_000;

  private final OzoneClientAdapter adapter = mock(OzoneClientAdapter.class);
  private final OzoneFSOutputStream stream = mock(OzoneFSOutputStream.class);
  private final List<Long> sleeps = new ArrayList<>();
  private long nowMs;

  @BeforeEach
  void setUp() throws IOException {
    // A recovery that finds the file closed needs no datanode or commit call.
    when(adapter.recoverFilePrepare(anyString(), anyBoolean())).thenThrow(new OMException(KEY_ALREADY_CLOSED));
  }

  private OzoneFSOutputStream append() throws IOException {
    return AppendWithRecovery.append(adapter, KEY, MAX_WAIT_MS, () -> TimeUnit.MILLISECONDS.toNanos(nowMs), ms -> {
      sleeps.add(ms);
      nowMs += ms;
    });
  }

  private static AppendConflictException appendWriter(AppendSessionPhase phase, long lastRenewedAt, long remainingMs) {
    return new AppendConflictException("conflict", AppendConflictInfo.newBuilder()
        .setWriterKind(AppendWriterKind.APPEND_WRITER).setSessionId(7).setPhase(phase).setLastRenewedAt(lastRenewedAt)
        .setRemainingMsUntilRecoverable(remainingMs).build());
  }

  private static AppendConflictException active(long lastRenewedAt, long remainingMs) {
    return appendWriter(AppendSessionPhase.APPEND_ACTIVE, lastRenewedAt, remainingMs);
  }

  @Test
  void admittedWithoutConflict() throws IOException {
    when(adapter.appendFile(KEY)).thenReturn(stream);

    assertThat(append()).isSameAs(stream);
    assertThat(sleeps).isEmpty();
    verify(adapter, never()).recoverFilePrepare(anyString(), anyBoolean());
  }

  @Test
  void waitsThenRecoversAbandonedAppendWriter() throws IOException {
    when(adapter.appendFile(KEY)).thenThrow(active(100, 3000)).thenThrow(active(100, 0)).thenReturn(stream);

    assertThat(append()).isSameAs(stream);
    assertThat(sleeps).containsExactly(3000L);
    verify(adapter).recoverFilePrepare(KEY, false);
    verify(adapter, times(3)).appendFile(KEY);
  }

  @Test
  void resumesRecoveryInProgressWithoutWaiting() throws IOException {
    when(adapter.appendFile(KEY)).thenThrow(appendWriter(AppendSessionPhase.APPEND_RECOVERING, 100, 0))
        .thenReturn(stream);

    assertThat(append()).isSameAs(stream);
    assertThat(sleeps).isEmpty();
    verify(adapter).recoverFilePrepare(KEY, false);
  }

  @Test
  void retriesAdmissionOnlyOnceAfterRecovery() throws IOException {
    AppendConflictException second = active(500, 3000);
    when(adapter.appendFile(KEY)).thenThrow(active(100, 0)).thenThrow(second);

    assertThatThrownBy(this::append).isSameAs(second);
    verify(adapter, times(2)).appendFile(KEY);
  }

  @Test
  void failsWhenWriterRenewsWhileWaiting() throws IOException {
    AppendConflictException renewed = active(2100, 3000);
    when(adapter.appendFile(KEY)).thenThrow(active(100, 3000)).thenThrow(renewed);

    assertThatThrownBy(this::append).isSameAs(renewed);
    assertThat(sleeps).containsExactly(3000L);
    verify(adapter, never()).recoverFilePrepare(anyString(), anyBoolean());
  }

  @Test
  void failsWithoutWaitingWhenRemainingExceedsDeadline() throws IOException {
    AppendConflictException conflict = active(100, MAX_WAIT_MS + 1);
    when(adapter.appendFile(KEY)).thenThrow(conflict);

    assertThatThrownBy(this::append).isSameAs(conflict);
    assertThat(sleeps).isEmpty();
    verify(adapter, never()).recoverFilePrepare(anyString(), anyBoolean());
  }

  @Test
  void deadlineIsNotResetByPolls() throws IOException {
    // OM keeps reporting the same remaining time, for example after its soft limit was raised.
    when(adapter.appendFile(KEY)).thenThrow(active(100, 6000));

    assertThatThrownBy(this::append).isInstanceOf(AppendConflictException.class);
    assertThat(sleeps).containsExactly(6000L);
    verify(adapter, never()).recoverFilePrepare(anyString(), anyBoolean());
  }

  @ParameterizedTest
  @EnumSource(value = AppendWriterKind.class, names = {"ORDINARY_WRITER", "HSYNC_WRITER"})
  void failsImmediatelyForOtherWriters(AppendWriterKind kind) throws IOException {
    AppendConflictException conflict = new AppendConflictException("conflict",
        AppendConflictInfo.newBuilder().setWriterKind(kind).build());
    when(adapter.appendFile(KEY)).thenThrow(conflict);

    assertThatThrownBy(this::append).isSameAs(conflict);
    assertThat(sleeps).isEmpty();
    verify(adapter).appendFile(KEY);
    verify(adapter, never()).recoverFilePrepare(anyString(), anyBoolean());
  }

  @Test
  void recoveryRefusalSurfacesAsConflictOfLiveWriter() throws IOException {
    // The writer renewed between the admission attempt and the recovery request: OM refuses, nothing is stolen.
    AppendConflictException renewed = active(2100, 3000);
    when(adapter.appendFile(KEY)).thenThrow(active(100, 0)).thenThrow(renewed);
    doThrow(new OMException(KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD)).when(adapter).recoverFilePrepare(KEY, false);

    assertThatThrownBy(this::append).isSameAs(renewed);
    verify(adapter, times(2)).appendFile(KEY);
  }

  @ParameterizedTest
  @EnumSource(value = OMException.ResultCodes.class,
      names = {"KEY_ALREADY_CLOSED", "APPEND_SESSION_NOT_FOUND", "KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD"})
  void appendsAfterLosingRecoveryRace(OMException.ResultCodes lostRace) throws IOException {
    // Another client finished the recovery (and possibly appended and closed) before our recovery commit.
    when(adapter.appendFile(KEY)).thenThrow(active(100, 0)).thenReturn(stream);
    doReturn(leaseKeyInfoOfEmptySession()).when(adapter).recoverFilePrepare(KEY, false);
    doThrow(new OMException(lostRace)).when(adapter).recoverFile(any());

    assertThat(append()).isSameAs(stream);
    verify(adapter).recoverFile(any());
  }

  @Test
  void propagatesOtherRecoveryFailures() throws IOException {
    OMException failure = new OMException(OMException.ResultCodes.PERMISSION_DENIED);
    when(adapter.appendFile(KEY)).thenThrow(active(100, 0));
    doThrow(failure).when(adapter).recoverFilePrepare(KEY, false);

    assertThatThrownBy(this::append).isSameAs(failure);
    verify(adapter).appendFile(KEY);
  }

  /** @return what OM returns for an append session that allocated nothing: no datanode call is needed. */
  private static LeaseKeyInfo leaseKeyInfoOfEmptySession() {
    OmKeyInfo.Builder keyInfo = new OmKeyInfo.Builder().setVolumeName("vol").setBucketName("bucket").setKeyName(KEY)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setOmKeyLocationInfos(Collections.singletonList(new OmKeyLocationInfoGroup(0, Collections.emptyList())));
    return new LeaseKeyInfo(keyInfo.build(), keyInfo.setAppendSession(OmAppendSession.newActive(0, 0, 0)).build());
  }

  @Test
  void interruptedWait() throws IOException {
    when(adapter.appendFile(KEY)).thenThrow(active(100, 3000));

    assertThatThrownBy(() -> AppendWithRecovery.append(adapter, KEY, MAX_WAIT_MS, System::nanoTime, ms -> {
      throw new InterruptedException();
    })).isInstanceOf(InterruptedIOException.class);
    assertThat(Thread.interrupted()).isTrue();
    verify(adapter, never()).recoverFilePrepare(anyString(), anyBoolean());
  }
}
