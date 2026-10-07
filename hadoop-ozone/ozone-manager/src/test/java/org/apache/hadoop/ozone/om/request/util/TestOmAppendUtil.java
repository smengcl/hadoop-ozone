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

package org.apache.hadoop.ozone.om.request.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.ozone.om.helpers.OmAppendSession;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AppendSessionPhase;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OmAppendUtil}.
 */
public class TestOmAppendUtil {

  private static OmKeyLocationInfo block(long localId) {
    return new OmKeyLocationInfo.Builder().setBlockID(new BlockID(1L, localId)).setLength(100).build();
  }

  private static OmKeyInfo.Builder key(OmKeyLocationInfo... blocks) {
    return new OmKeyInfo.Builder().setVolumeName("vol").setBucketName("bucket").setKeyName("key")
        .setOmKeyLocationInfos(Collections.singletonList(new OmKeyLocationInfoGroup(0, Arrays.asList(blocks))));
  }

  @Test
  public void privateAllocationsExcludePublishedSuffix() {
    // Prefix A, session allocated B and C, hsync published B.
    OmKeyLocationInfo a = block(1);
    OmKeyLocationInfo b = block(2);
    OmKeyLocationInfo c = block(3);
    OmKeyInfo committed = key(a, b).setAppendOwnerSessionId(7L).build();
    OmKeyInfo open = key(b, c).setAppendSession(OmAppendSession.newActive(100, 1, 1000)).build();

    assertThat(OmAppendUtil.privateAllocations(open, committed)).containsExactly(c);
    assertThat(OmAppendUtil.privateAllocations(open, null)).containsExactly(b, c);
    assertThat(OmAppendUtil.privateAllocations(key().build(), committed)).isEmpty();

    OmKeyInfo invalidated = OmAppendUtil.invalidate(open, committed, 55L);
    assertEquals(AppendSessionPhase.APPEND_INVALIDATED, invalidated.getAppendSession().getPhase());
    assertThat(invalidated.getLatestVersionLocations().createLocationList()).containsExactly(c);
    assertEquals(55L, invalidated.getUpdateID());
    // The cached open record must not be modified in place.
    assertTrue(open.getAppendSession().isActive());
    assertThat(open.getLatestVersionLocations().createLocationList()).containsExactly(b, c);
  }

  @Test
  public void ownership() {
    assertTrue(OmAppendUtil.isOwnedBy(key().setAppendOwnerSessionId(7L).build(), 7L));
    assertFalse(OmAppendUtil.isOwnedBy(key().setAppendOwnerSessionId(7L).build(), 8L));
    assertFalse(OmAppendUtil.isOwnedBy(key().build(), 7L));
    assertFalse(OmAppendUtil.isOwnedBy(null, 7L));
  }
}
