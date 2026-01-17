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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import java.io.File;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InotifyRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestInotifyDisabled {

  @TempDir
  private File folder;

  @Test
  void testInotifyDisabledRejected() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, folder.getAbsolutePath());
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, folder.getAbsolutePath());
    conf.setBoolean(OzoneConfigKeys.OZONE_OM_INOTIFY_ENABLED, false);

    OmTestManagers testManagers = new OmTestManagers(conf);
    try {
      OzoneManager om = testManagers.getOzoneManager();
      InotifyRequest request = InotifyRequest.newBuilder()
          .setWriteSequenceNumber(0L)
          .setAccessSequenceNumber(0L)
          .setLimitCount(1L)
          .build();

      OMException ex = assertThrows(OMException.class,
          () -> om.getInotifyEvents(request));
      assertEquals(OMException.ResultCodes.FEATURE_NOT_ENABLED, ex.getResult());
    } finally {
      testManagers.stop();
    }
  }
}
