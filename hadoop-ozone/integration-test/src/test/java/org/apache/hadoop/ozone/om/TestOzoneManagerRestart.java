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

package org.apache.hadoop.ozone.om;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PARTIAL_RENAME;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_ALREADY_EXISTS;
import static org.apache.ozone.test.OzoneTestBase.uniqueObjectName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Bytes;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LeaseRecoverable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.AppendConflictException;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.service.KeyDeletingService;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test some client operations after cluster starts. And perform restart and
 * then performs client operations and check the behavior is expected or not.
 */
public class TestOzoneManagerRestart {
  private static final String APPEND_LEASE_RENEW_INTERVAL = "ozone.client.append.lease.renew.interval";

  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static OzoneClient client;

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.setBoolean(OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
    conf.setBoolean("ozone.client.hbase.enhancements.allowed", true);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    conf.setBoolean(OMConfigKeys.OZONE_OM_APPEND_ENABLED, true);
    conf.set(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);
    // Use OBS layout for key rename testing.
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.OBJECT_STORE.name());
    cluster =  MiniOzoneCluster.newBuilder(conf)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
  }

  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testRestartOMWithVolumeOperation() throws Exception {
    String volumeName = uniqueObjectName("volume");

    ObjectStore objectStore = client.getObjectStore();

    objectStore.createVolume(volumeName);

    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    assertEquals(volumeName, ozoneVolume.getName());

    cluster.restartOzoneManager();
    cluster.restartStorageContainerManager(true);

    // After restart, try to create same volume again, it should fail.
    OMException ome = assertThrows(OMException.class, () -> objectStore.createVolume(volumeName));
    assertEquals(VOLUME_ALREADY_EXISTS, ome.getResult());

    // Get Volume.
    ozoneVolume = objectStore.getVolume(volumeName);
    assertEquals(volumeName, ozoneVolume.getName());

  }

  @Test
  public void testRestartOMWithBucketOperation() throws Exception {
    String volumeName = uniqueObjectName("volume");
    String bucketName = uniqueObjectName("bucket");

    ObjectStore objectStore = client.getObjectStore();

    objectStore.createVolume(volumeName);

    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    assertEquals(volumeName, ozoneVolume.getName());

    ozoneVolume.createBucket(bucketName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    assertEquals(bucketName, ozoneBucket.getName());

    cluster.restartOzoneManager();
    cluster.restartStorageContainerManager(true);

    // After restart, try to create same bucket again, it should fail.
    // After restart, try to create same volume again, it should fail.
    OMException ome = assertThrows(OMException.class, () -> ozoneVolume.createBucket(bucketName));
    assertEquals(BUCKET_ALREADY_EXISTS, ome.getResult());

    // Get bucket.
    ozoneBucket = ozoneVolume.getBucket(bucketName);
    assertEquals(bucketName, ozoneBucket.getName());
  }

  @Test
  public void testRestartOMWithKeyOperation() throws Exception {
    String volumeName = uniqueObjectName("volume");
    String bucketName = uniqueObjectName("bucket");
    String key1 = uniqueObjectName("key1");
    String key2 = uniqueObjectName("key2");

    String newKey1 = uniqueObjectName("key1new");
    String newKey2 = uniqueObjectName("key2new");

    ObjectStore objectStore = client.getObjectStore();

    objectStore.createVolume(volumeName);

    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    assertEquals(volumeName, ozoneVolume.getName());

    ozoneVolume.createBucket(bucketName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    assertEquals(bucketName, ozoneBucket.getName());

    String data = "random data";
    OzoneOutputStream ozoneOutputStream1 = ozoneBucket.createKey(key1,
        data.length(), ReplicationType.RATIS, ReplicationFactor.ONE,
        new HashMap<>());

    ozoneOutputStream1.write(data.getBytes(UTF_8), 0, data.length());
    ozoneOutputStream1.close();

    Map<String, String> keyMap = new HashMap();
    keyMap.put(key1, newKey1);
    keyMap.put(key2, newKey2);

    try {
      ozoneBucket.renameKeys(keyMap);
    } catch (OMException ex) {
      assertEquals(PARTIAL_RENAME, ex.getResult());
    }

    // Get original Key1, it should not exist
    try {
      ozoneBucket.getKey(key1);
    } catch (OMException ex) {
      assertEquals(KEY_NOT_FOUND, ex.getResult());
    }

    cluster.restartOzoneManager();
    cluster.restartStorageContainerManager(true);


    // As we allow override of keys, not testing re-create key. We shall see
    // after restart key exists or not.

    // Get newKey1.
    OzoneKey ozoneKey = ozoneBucket.getKey(newKey1);
    assertEquals(newKey1, ozoneKey.getName());
    assertEquals(ReplicationType.RATIS, ozoneKey.getReplicationType());

    // Get newKey2, it should not exist
    try {
      ozoneBucket.getKey(newKey2);
    } catch (OMException ex) {
      assertEquals(KEY_NOT_FOUND, ex.getResult());
    }
  }

  @Test
  public void testRestartOMWithOpenAppendStream() throws Exception {
    OzoneBucket bucket = createFsoBucket();
    try (FileSystem fs = newFs(APPEND_LEASE_RENEW_INTERVAL, "500ms")) {
      final Path file = new Path(bucketPath(bucket), "file");
      final byte[] prefix = createFile(fs, file);
      final byte[] beforeRestart = randomBytes();
      final byte[] afterRestart = randomBytes();
      final byte[] beforeClose = randomBytes();
      final long sessionId;

      try (FSDataOutputStream out = fs.append(file)) {
        out.write(beforeRestart);
        out.hsync();
        sessionId = bucket.getFileStatus(file.getName()).getKeyInfo().getAppendOwnerSessionId();
        final long renewedBeforeRestart = appendOpenRecord(bucket, sessionId).getAppendSession().getLastRenewedAt();

        cluster.restartOzoneManager();

        // The session index is rebuilt from the open file table, so renewals find the session again.
        GenericTestUtils.waitFor(() -> lastRenewedAt(bucket, sessionId) > renewedBeforeRestart, 100, 30000);
        assertThatThrownBy(() -> bucket.appendFile(file.getName())).isInstanceOf(AppendConflictException.class);
        assertThat(bucket.getFileStatus(file.getName()).getKeyInfo().getAppendOwnerSessionId()).isEqualTo(sessionId);

        out.write(afterRestart);
        out.hsync();
        assertThat(fs.getFileStatus(file).getLen())
            .isEqualTo(prefix.length + beforeRestart.length + afterRestart.length);
        out.write(beforeClose);
      }

      assertThat(readFile(fs, file)).isEqualTo(Bytes.concat(prefix, beforeRestart, afterRestart, beforeClose));
      assertThat(bucket.getFileStatus(file.getName()).getKeyInfo().getAppendOwnerSessionId()).isNull();
      assertThat(appendOpenRecord(bucket, sessionId)).isNull();
    }
  }

  @Test
  public void testRestartOMWithAbandonedAppendStream() throws Exception {
    OzoneBucket bucket = createFsoBucket();
    // The writer never renews its lease, like a client that crashed.
    try (FileSystem writerFs = newFs(APPEND_LEASE_RENEW_INTERVAL, "1h"); FileSystem fs = newFs()) {
      final Path file = new Path(bucketPath(bucket), "file");
      final byte[] prefix = createFile(fs, file);
      final byte[] synced = randomBytes();
      final byte[] more = randomBytes();
      final FSDataOutputStream abandoned = writerFs.append(file);
      try {
        abandoned.write(synced);
        abandoned.hsync();
        // Stays in the client buffer, so it is lost with the writer.
        abandoned.write(randomBytes());

        cluster.restartOzoneManager();
        cluster.getOzoneManager().getConfiguration().set(OZONE_OM_LEASE_SOFT_LIMIT, "0s");

        assertThat(((LeaseRecoverable) fs).isFileClosed(file)).isFalse();
        assertThat(((LeaseRecoverable) fs).recoverLease(file)).isTrue();
        assertThat(((LeaseRecoverable) fs).isFileClosed(file)).isTrue();
        assertThat(readFile(fs, file)).isEqualTo(Bytes.concat(prefix, synced));

        try (FSDataOutputStream out = fs.append(file)) {
          assertThat(out.getPos()).isEqualTo(prefix.length + synced.length);
          out.write(more);
        }
        // The fenced writer cannot publish any more.
        assertThatThrownBy(abandoned::hsync).isInstanceOf(IOException.class);
      } finally {
        IOUtils.closeQuietly(abandoned);
        cluster.getOzoneManager().getConfiguration().unset(OZONE_OM_LEASE_SOFT_LIMIT);
      }
      assertThat(readFile(fs, file)).isEqualTo(Bytes.concat(prefix, synced, more));
    }
  }

  /**
   * An append that runs into the space quota fails without changing the file or the used bytes of the bucket. The
   * client does not end the session of the failed stream. Lease recovery does: it closes the file at the published
   * length and releases the blocks that were not published, so the bucket never holds more than its quota.
   */
  @Test
  public void testAppendOverSpaceQuota() throws Exception {
    OzoneBucket bucket = createFsoBucket();
    cluster.getOzoneManager().getConfiguration().set(OZONE_OM_LEASE_SOFT_LIMIT, "0s");
    try (FileSystem fs = newFs()) {
      final Path file = new Path(bucketPath(bucket), "file");
      final byte[] prefix = createFile(fs, file);
      final byte[] synced = randomBytes();
      final byte[] overQuota = randomBytes();
      final long usedByPrefix = usedBytes(bucket);

      // No room for a block, so the first write fails. Closing the failed stream does not reach OM.
      bucket.setQuota(OzoneQuota.getOzoneQuota(usedByPrefix, OzoneConsts.QUOTA_RESET));
      try (FSDataOutputStream out = fs.append(file)) {
        assertThatThrownBy(() -> out.write(synced)).hasStackTraceContaining("QUOTA_EXCEEDED");
      }
      assertThat(appendOwner(bucket, file)).isNotNull();
      assertThat(((LeaseRecoverable) fs).recoverLease(file)).isTrue();
      assertThat(appendOwner(bucket, file)).isNull();
      assertThat(usedBytes(bucket)).isEqualTo(usedByPrefix);
      assertThat(readFile(fs, file)).isEqualTo(prefix);

      // The quota runs out after the block was allocated, so hsync and close fail.
      bucket.clearSpaceQuota();
      final FSDataOutputStream out = fs.append(file);
      try {
        out.write(synced);
        out.hsync();
        out.write(overQuota);
        bucket.setQuota(OzoneQuota.getOzoneQuota(2 * usedByPrefix, OzoneConsts.QUOTA_RESET));
        assertThatThrownBy(out::hsync).hasStackTraceContaining("QUOTA_EXCEEDED");
        assertThatThrownBy(out::close).hasStackTraceContaining("QUOTA_EXCEEDED");
      } finally {
        IOUtils.closeQuietly(out);
      }
      assertThat(appendOwner(bucket, file)).isNotNull();
      assertThat(usedBytes(bucket)).isEqualTo(2 * usedByPrefix);
      assertThat(readFile(fs, file)).isEqualTo(Bytes.concat(prefix, synced));

      // What was written after the last hsync was never acknowledged, and the bucket has no room for it.
      assertThat(((LeaseRecoverable) fs).recoverLease(file)).isTrue();
      assertThat(appendOwner(bucket, file)).isNull();
      assertThat(fs.getFileStatus(file).getLen()).isEqualTo(prefix.length + synced.length);
      assertThat(readFile(fs, file)).isEqualTo(Bytes.concat(prefix, synced));
      assertThat(usedBytes(bucket)).isEqualTo(2 * usedByPrefix);

      // The same with a block that was never published: recovery hands it to the key deleting service.
      final KeyDeletingService keyDeletingService = cluster.getOzoneManager().getKeyManager().getDeletingService();
      keyDeletingService.suspend();
      bucket.clearSpaceQuota();
      final FSDataOutputStream unpublished = fs.append(file);
      try {
        unpublished.write(overQuota);
        final OmKeyInfo openRecord = appendOpenRecord(bucket, appendOwner(bucket, file));
        final ContainerBlockID discarded = openRecord.getLatestVersionLocations().getBlocksLatestVersionOnly().get(0)
            .getBlockID().getContainerBlockID();
        bucket.setQuota(OzoneQuota.getOzoneQuota(2 * usedByPrefix, OzoneConsts.QUOTA_RESET));
        assertThatThrownBy(unpublished::close).hasStackTraceContaining("QUOTA_EXCEEDED");

        assertThat(((LeaseRecoverable) fs).recoverLease(file)).isTrue();
        assertThat(appendOwner(bucket, file)).isNull();
        assertThat(readFile(fs, file)).isEqualTo(Bytes.concat(prefix, synced));
        assertThat(usedBytes(bucket)).isEqualTo(2 * usedByPrefix);
        assertThat(deletedBlocks()).contains(discarded);
      } finally {
        IOUtils.closeQuietly(unpublished);
        keyDeletingService.resume();
      }

      bucket.clearSpaceQuota();
      try (FSDataOutputStream again = fs.append(file)) {
        again.write(overQuota);
      }
      assertThat(appendOwner(bucket, file)).isNull();
      assertThat(readFile(fs, file)).isEqualTo(Bytes.concat(prefix, synced, overQuota));
      assertThat(usedBytes(bucket)).isEqualTo(3 * usedByPrefix);
    } finally {
      cluster.getOzoneManager().getConfiguration().unset(OZONE_OM_LEASE_SOFT_LIMIT);
    }
  }

  /** @return the blocks of the keys that wait in the deleted table. */
  private static List<ContainerBlockID> deletedBlocks() throws Exception {
    final List<ContainerBlockID> blocks = new ArrayList<>();
    cluster.getOzoneManager().awaitDoubleBufferFlush();
    try (Table.KeyValueIterator<String, RepeatedOmKeyInfo> iterator =
        cluster.getOzoneManager().getMetadataManager().getDeletedTable().iterator()) {
      while (iterator.hasNext()) {
        for (OmKeyInfo key : iterator.next().getValue().getOmKeyInfoList()) {
          key.getLatestVersionLocations().getBlocksLatestVersionOnly()
              .forEach(location -> blocks.add(location.getBlockID().getContainerBlockID()));
        }
      }
    }
    return blocks;
  }

  private static Long appendOwner(OzoneBucket bucket, Path file) throws IOException {
    return bucket.getFileStatus(file.getName()).getKeyInfo().getAppendOwnerSessionId();
  }

  private static long usedBytes(OzoneBucket bucket) throws IOException {
    return client.getObjectStore().getVolume(bucket.getVolumeName()).getBucket(bucket.getName()).getUsedBytes();
  }

  private static OzoneBucket createFsoBucket() throws IOException {
    String volumeName = uniqueObjectName("volume");
    String bucketName = uniqueObjectName("bucket");
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(volumeName);
    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    ozoneVolume.createBucket(bucketName,
        BucketArgs.newBuilder().setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED).build());
    return ozoneVolume.getBucket(bucketName);
  }

  /** @return a new ofs instance for the cluster, not from the cache. */
  private static FileSystem newFs(String... overrides) throws IOException {
    final OzoneConfiguration fsConf = new OzoneConfiguration(conf);
    for (int i = 0; i < overrides.length; i += 2) {
      fsConf.set(overrides[i], overrides[i + 1]);
    }
    return FileSystem.newInstance(
        URI.create(String.format("%s://%s/", OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY))), fsConf);
  }

  private static String bucketPath(OzoneBucket bucket) {
    return "/" + bucket.getVolumeName() + "/" + bucket.getName();
  }

  private static byte[] randomBytes() {
    final byte[] data = new byte[100];
    ThreadLocalRandom.current().nextBytes(data);
    return data;
  }

  private static byte[] createFile(FileSystem fs, Path file) throws IOException {
    final byte[] data = randomBytes();
    try (FSDataOutputStream out = fs.create(file, true)) {
      out.write(data);
    }
    return data;
  }

  private static byte[] readFile(FileSystem fs, Path file) throws IOException {
    try (FSDataInputStream in = fs.open(file)) {
      return ByteStreams.toByteArray(in);
    }
  }

  /** @return the open record of an append session as the restarted OM resolves it, null if the session is gone. */
  private static OmKeyInfo appendOpenRecord(OzoneBucket bucket, long sessionId) throws IOException {
    OMMetadataManager metadataManager = cluster.getOzoneManager().getMetadataManager();
    String dbOpenKey = metadataManager.getAppendSessionOpenKey(bucket.getVolumeName(), bucket.getName(), sessionId);
    return dbOpenKey == null ? null
        : metadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).get(dbOpenKey);
  }

  private static long lastRenewedAt(OzoneBucket bucket, long sessionId) {
    try {
      return appendOpenRecord(bucket, sessionId).getAppendSession().getLastRenewedAt();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
