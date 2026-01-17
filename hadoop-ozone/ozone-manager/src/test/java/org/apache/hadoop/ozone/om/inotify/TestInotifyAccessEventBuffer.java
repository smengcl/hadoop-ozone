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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.ozone.om.helpers.InotifyEvent;
import org.junit.jupiter.api.Test;

class TestInotifyAccessEventBuffer {

  @Test
  void testOverflowAndSequencing() {
    InotifyAccessEventBuffer buffer = new InotifyAccessEventBuffer(2);
    buffer.recordAccess("vol/bucket/a", false, 1L);
    buffer.recordAccess("vol/bucket/b", false, 2L);
    buffer.recordAccess("vol/bucket/c", false, 3L);

    InotifyAccessEventBuffer.AccessBatch batch = buffer.readSince(0L, 10L);
    assertThat(batch.isOverflow()).isTrue();
    assertThat(batch.getEvents()).hasSize(2);
    assertThat(batch.getEvents().get(0).getPath()).endsWith("b");
    assertThat(batch.getEvents().get(1).getPath()).endsWith("c");

    InotifyAccessEventBuffer.AccessBatch batch2 = buffer.readSince(2L, 10L);
    assertThat(batch2.isOverflow()).isFalse();
    assertThat(batch2.getEvents()).hasSize(1);
    assertThat(batch2.getEvents().get(0).getPath()).endsWith("c");
    assertThat(batch2.getLatestSequence()).isEqualTo(3L);
  }

  @Test
  void testLimitCount() {
    InotifyAccessEventBuffer buffer = new InotifyAccessEventBuffer(5);
    buffer.recordAccess("vol/bucket/a", false, 1L);
    buffer.recordAccess("vol/bucket/b", false, 2L);
    buffer.recordAccess("vol/bucket/c", false, 3L);

    InotifyAccessEventBuffer.AccessBatch batch = buffer.readSince(0L, 1L);
    assertThat(batch.getEvents()).hasSize(1);
    InotifyEvent event = batch.getEvents().get(0);
    assertThat(event.getSequenceNumber()).isEqualTo(1L);
  }
}
