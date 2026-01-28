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

package org.apache.hadoop.ozone.container.common.kernel;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.junit.jupiter.api.Test;

class TestKernelLogMonitor {

  @Test
  void testHighConfidenceFailureFailsVolume() {
    MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);
    Map<String, MutableVolumeSet> rootMap = Map.of("/data/vol1", volumeSet);
    DeviceMapper mapper = device -> List.of("/data/vol1");
    KernelLogMonitor monitor = new KernelLogMonitor(
        new NoopReader(), mapper, rootMap, Clock.systemUTC());

    monitor.handleLine("sd 0:0:0:0: [sda] I/O error, dev sda, sector 123");

    verify(volumeSet, times(1)).failVolume("/data/vol1");
  }

  @Test
  void testWarningThresholdEscalates() {
    MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);
    Map<String, MutableVolumeSet> rootMap = Map.of("/data/vol1", volumeSet);
    DeviceMapper mapper = device -> List.of("/data/vol1");
    MutableClock clock = new MutableClock(0L);
    KernelLogMonitor monitor = new KernelLogMonitor(
        new NoopReader(), mapper, rootMap, clock);

    String line = "sd 0:0:0:0: [sdb] timed out on command";
    monitor.handleLine(line);
    monitor.handleLine(line);
    monitor.handleLine(line);

    verify(volumeSet, times(1)).failVolume("/data/vol1");
  }

  @Test
  void testAmbiguousMappingDoesNotFailVolume() {
    MutableVolumeSet volumeSet1 = mock(MutableVolumeSet.class);
    MutableVolumeSet volumeSet2 = mock(MutableVolumeSet.class);
    Map<String, MutableVolumeSet> rootMap = Map.of(
        "/data/vol1", volumeSet1,
        "/data/vol2", volumeSet2);
    DeviceMapper mapper = device -> List.of("/data/vol1", "/data/vol2");
    KernelLogMonitor monitor = new KernelLogMonitor(
        new NoopReader(), mapper, rootMap, Clock.systemUTC());

    monitor.handleLine("sd 0:0:0:0: [sdc] I/O error, dev sdc");

    verify(volumeSet1, never()).failVolume("/data/vol1");
    verify(volumeSet2, never()).failVolume("/data/vol2");
  }

  private static final class NoopReader implements KernelLogReader {
    @Override
    public String readLine() throws IOException {
      return null;
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }

  private static final class MutableClock extends Clock {
    private long millis;

    private MutableClock(long millis) {
      this.millis = millis;
    }

    @Override
    public ZoneId getZone() {
      return ZoneId.systemDefault();
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return Instant.ofEpochMilli(millis);
    }

    @Override
    public long millis() {
      return millis;
    }
  }
}
