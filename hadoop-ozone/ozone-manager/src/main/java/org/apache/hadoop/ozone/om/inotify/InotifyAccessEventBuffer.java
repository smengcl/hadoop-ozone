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

package org.apache.hadoop.ozone.om.inotify;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.ozone.om.helpers.InotifyEvent;

/**
 * Ring buffer for ACCESS inotify events.
 */
public class InotifyAccessEventBuffer {

  public static class AccessBatch {
    private final List<InotifyEvent> events;
    private final long latestSequence;
    private final boolean overflow;

    AccessBatch(List<InotifyEvent> events, long latestSequence,
        boolean overflow) {
      this.events = events;
      this.latestSequence = latestSequence;
      this.overflow = overflow;
    }

    public List<InotifyEvent> getEvents() {
      return events;
    }

    public long getLatestSequence() {
      return latestSequence;
    }

    public boolean isOverflow() {
      return overflow;
    }
  }

  private final InotifyEvent[] ring;
  private final int capacity;
  private final AtomicLong sequence = new AtomicLong(0L);

  public InotifyAccessEventBuffer(int capacity) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must be > 0");
    }
    this.capacity = capacity;
    this.ring = new InotifyEvent[capacity];
  }

  public long recordAccess(String path, boolean isDir, long timestamp) {
    long seq = sequence.incrementAndGet();
    int slot = (int) (seq % capacity);
    ring[slot] = new InotifyEvent(
        InotifyEvent.EventType.ACCESS,
        path,
        null,
        isDir,
        timestamp,
        seq);
    return seq;
  }

  public AccessBatch readSince(long sinceSeq, long limitCount) {
    long latestSeq = sequence.get();
    if (latestSeq == 0) {
      return new AccessBatch(new ArrayList<>(), 0L, false);
    }
    long oldestSeq = Math.max(1L, latestSeq - capacity + 1);
    boolean overflow = sinceSeq > 0 && sinceSeq < oldestSeq;
    long start = Math.max(sinceSeq + 1, oldestSeq);
    long end = limitCount <= 0 ? latestSeq
        : Math.min(latestSeq, start + limitCount - 1);

    List<InotifyEvent> events = new ArrayList<>();
    for (long seq = start; seq <= end; seq++) {
      int slot = (int) (seq % capacity);
      InotifyEvent event = ring[slot];
      if (event != null && event.getSequenceNumber() == seq) {
        events.add(event);
      }
    }
    return new AccessBatch(events, latestSeq, overflow);
  }
}
