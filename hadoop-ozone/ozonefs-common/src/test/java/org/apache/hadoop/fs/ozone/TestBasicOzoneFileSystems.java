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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_INDICATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit test for Basic*OzoneFileSystem.
 */
public class TestBasicOzoneFileSystems {
  public static Collection<FileSystem> data() {
    return Arrays.asList(
        new BasicOzoneFileSystem(),
        new BasicRootedOzoneFileSystem()
    );
  }

  @ParameterizedTest(autoCloseArguments = false)
  @MethodSource("data")
  public void defaultBlockSize(FileSystem subject) {
    Configuration conf = new OzoneConfiguration();
    subject.setConf(conf);

    long expected = toBytes(OZONE_SCM_BLOCK_SIZE_DEFAULT);
    assertDefaultBlockSize(expected, subject);
  }

  @ParameterizedTest(autoCloseArguments = false)
  @MethodSource("data")
  public void defaultBlockSizeCustomized(FileSystem subject) {
    String customValue = "128MB";
    Configuration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_BLOCK_SIZE, customValue);
    subject.setConf(conf);

    assertDefaultBlockSize(toBytes(customValue), subject);
  }

  // test for filesystem pseduo-posix symlink support
  @ParameterizedTest(autoCloseArguments = false)
  @MethodSource("data")
  public void testFileSystemPosixSymlinkSupport(FileSystem subject) {
    if (subject instanceof BasicRootedOzoneFileSystem) {
      assertTrue(subject.supportsSymlinks());
    } else if (subject instanceof BasicOzoneFileSystem) {
      assertFalse(subject.supportsSymlinks());
    } else {
      fail("Test case not implemented for FileSystem: " +
          subject.getClass().getSimpleName());
    }
  }

  @ParameterizedTest(autoCloseArguments = false)
  @MethodSource("data")
  public void testCreateSnapshotReturnPath(
      FileSystem subject) throws IOException {
    final String snapshotName = "snap1";

    if (subject instanceof BasicRootedOzoneFileSystem) {
      BasicRootedOzoneClientAdapterImpl adapter =
          mock(BasicRootedOzoneClientAdapterImpl.class);
      doReturn(snapshotName).when(adapter).createSnapshot(any(), any());

      BasicRootedOzoneFileSystem ofs =
          spy((BasicRootedOzoneFileSystem) subject);
      when(ofs.getAdapter()).thenReturn(adapter);

      Path ofsBucketStr = new Path("ofs://om/vol1/buck1/");
      Path ofsDir1 = new Path(ofsBucketStr, "dir1");
      Path res = ofs.createSnapshot(new Path(ofsDir1, snapshotName));

      Path expectedSnapshotRoot = new Path(ofsBucketStr, OM_SNAPSHOT_INDICATOR);
      Path expectedSnapshotPath = new Path(expectedSnapshotRoot, snapshotName);

      // Return value path should be "ofs://om/vol1/buck1/.snapshot/snap1"
      // without the subdirectory "dir1" in the Path.
      assertEquals(expectedSnapshotPath, res);
    } else if (subject instanceof BasicOzoneFileSystem) {
      BasicOzoneClientAdapterImpl adapter =
          mock(BasicOzoneClientAdapterImpl.class);
      doReturn(snapshotName).when(adapter).createSnapshot(any(), any());

      BasicOzoneFileSystem o3fs = spy((BasicOzoneFileSystem) subject);
      when(o3fs.getAdapter()).thenReturn(adapter);

      Path o3fsBucketStr = new Path("o3fs://buck1.vol1.om/");
      Path o3fsDir1 = new Path(o3fsBucketStr, "dir1");
      Path res = o3fs.createSnapshot(new Path(o3fsDir1, snapshotName));

      Path expectedSnapshotRoot =
          new Path(o3fsBucketStr, OM_SNAPSHOT_INDICATOR);
      Path expectedSnapshotPath = new Path(expectedSnapshotRoot, snapshotName);

      // Return value path should be "o3fs://buck1.vol1.om/.snapshot/snap1"
      // without the subdirectory "dir1" in the Path.
      assertEquals(expectedSnapshotPath, res);
    } else {
      fail("Test case not implemented for FileSystem: " +
          subject.getClass().getSimpleName());
    }
  }

  @Test
  public void testOfsInitializeParsesIpv6Authority() throws IOException {
    String[] capturedHost = {null};
    int[] capturedPort = {-2};
    BasicRootedOzoneFileSystem ofs = new BasicRootedOzoneFileSystem() {
      @Override
      protected OzoneClientAdapter createAdapter(ConfigurationSource conf,
          String omHost, int omPort) {
        capturedHost[0] = omHost;
        capturedPort[0] = omPort;
        return mock(BasicRootedOzoneClientAdapterImpl.class);
      }
    };
    ofs.initialize(URI.create("ofs://[2001:db8::1]:9862"),
        new OzoneConfiguration());
    // Brackets are stripped so the host is usable for InetAddress lookups.
    assertEquals("2001:db8::1", capturedHost[0]);
    assertEquals(9862, capturedPort[0]);
  }

  @Test
  public void testO3fsInitializeParsesHostAndPort() throws IOException {
    // o3fs embeds the OM host after bucket.volume. A bracketed IPv6 literal is
    // not a legal java.net.URI authority mid-string, so the practical way to
    // reach an IPv6 OM over o3fs is an OM service id; this guards that the
    // HostAndPort-based host:port parse still handles a plain host and port.
    String[] capturedHost = {null};
    int[] capturedPort = {-2};
    BasicOzoneFileSystem o3fs = new BasicOzoneFileSystem() {
      @Override
      protected OzoneClientAdapter createAdapter(ConfigurationSource conf,
          String bucketStr, String volumeStr, String omHost, int omPort) {
        capturedHost[0] = omHost;
        capturedPort[0] = omPort;
        return mock(BasicOzoneClientAdapterImpl.class);
      }
    };
    o3fs.initialize(URI.create("o3fs://buck1.vol1.om1:9862"),
        new OzoneConfiguration());
    assertEquals("om1", capturedHost[0]);
    assertEquals(9862, capturedPort[0]);
  }

  private void assertDefaultBlockSize(long expected, FileSystem subject) {
    assertEquals(expected, subject.getDefaultBlockSize());

    Path anyPath = new Path("/");
    assertEquals(expected, subject.getDefaultBlockSize(anyPath));

    Path nonExistentFile = new Path("/no/such/file");
    assertEquals(expected, subject.getDefaultBlockSize(nonExistentFile));
  }

  private static long toBytes(String value) {
    StorageSize blockSize = StorageSize.parse(value);
    return (long) blockSize.getUnit().toBytes(blockSize.getValue());
  }

}
