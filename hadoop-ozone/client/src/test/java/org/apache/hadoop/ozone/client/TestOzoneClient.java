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

package org.apache.hadoop.ozone.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.ozone.test.GenericTestUtils.getTestStartTime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.CryptoExtension;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfigValidator;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.io.ECKeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.ozone.test.LambdaTestUtils.VoidCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Real unit test for OzoneClient.
 * <p>
 * Used for testing Ozone client without external network calls.
 */
public class TestOzoneClient {

  private OzoneClient client;
  private ObjectStore store;
  private MockOmTransport omTransport;
  private KeyProvider keyProvider;

  public static <E extends Throwable> void expectOmException(
      OMException.ResultCodes code,
      VoidCallable eval)
      throws Exception {
    OMException ex = assertThrows(OMException.class, () -> eval.call());
    assertEquals(code, ex.getResult());
  }

  @BeforeEach
  public void init() throws IOException {
    OzoneConfiguration config = new OzoneConfiguration();
    createNewClient(config, new SinglePipelineBlockAllocator(config));
  }

  private void createNewClient(ConfigurationSource config,
      MockBlockAllocator blkAllocator) throws IOException {
    client = new OzoneClient(config, new RpcClient(config, null) {

      @Override
      protected OmTransport createOmTransport(String omServiceId) {
        omTransport = new MockOmTransport(blkAllocator);
        return omTransport;
      }

      @Override
      public KeyProvider getKeyProvider() {
        return keyProvider;
      }

      @Nonnull
      @Override
      protected XceiverClientFactory createXceiverClientFactory(
          ServiceInfoEx serviceInfo) {
        return new MockXceiverClientFactory();
      }
    });

    store = client.getObjectStore();
  }

  @AfterEach
  public void close() throws IOException {
    client.close();
  }

  @Test
  public void testDeleteVolume()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    assertNotNull(volume);
    store.deleteVolume(volumeName);
    expectOmException(ResultCodes.VOLUME_NOT_FOUND,
        () -> store.getVolume(volumeName));

  }

  @Test
  public void testCreateVolumeWithMetadata()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .addMetadata("key1", "val1")
        .build();
    store.createVolume(volumeName, volumeArgs);
    OzoneVolume volume = store.getVolume(volumeName);
    assertEquals(OzoneConsts.QUOTA_RESET,
        volume.getQuotaInNamespace());
    assertEquals(OzoneConsts.QUOTA_RESET, volume.getQuotaInBytes());
    assertEquals("val1", volume.getMetadata().get("key1"));
    assertEquals(volumeName, volume.getName());
  }

  @Test
  public void testCreateBucket()
      throws IOException {
    Instant testStartTime = getTestStartTime();
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertEquals(bucketName, bucket.getName());
    assertFalse(bucket.getCreationTime().isBefore(testStartTime));
    assertFalse(volume.getCreationTime().isBefore(testStartTime));
  }

  @Test
  public void testPutKeyRatisOneNode() throws IOException {
    Instant testStartTime = getTestStartTime();
    String value = "sample value";
    OzoneBucket bucket = getOzoneBucket();

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      OzoneOutputStream out = bucket.createKey(keyName,
          value.getBytes(UTF_8).length, ReplicationType.RATIS,
          ONE, new HashMap<>());
      out.write(value.getBytes(UTF_8));
      out.close();
      OzoneKey key = bucket.getKey(keyName);
      assertEquals(keyName, key.getName());
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] fileContent = new byte[value.getBytes(UTF_8).length];
      assertEquals(value.length(), is.read(fileContent));
      is.close();
      assertEquals(value, new String(fileContent, UTF_8));
      assertFalse(key.getCreationTime().isBefore(testStartTime));
      assertFalse(key.getModificationTime().isBefore(testStartTime));
    }
  }

  @Test
  public void testPutKeyAllocateBlock() throws IOException {
    String value = new String(new byte[1024], UTF_8);
    OzoneBucket bucket = getOzoneBucket();

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      try (OzoneOutputStream out = bucket
          .createKey(keyName, value.getBytes(UTF_8).length,
              ReplicationType.RATIS, ONE, new HashMap<>())) {
        out.write(value.getBytes(UTF_8));
        out.write(value.getBytes(UTF_8));
      }
    }
  }

  @Test
  public void testPutKeyWithECReplicationConfig() throws IOException {
    close();
    OzoneConfiguration config = new OzoneConfiguration();
    ReplicationConfigValidator validator =
        config.getObject(ReplicationConfigValidator.class);
    validator.disableValidation();
    config.setFromObject(validator);
    config.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 2,
        StorageUnit.KB);
    int data = 3;
    int parity = 2;
    int chunkSize = 1024;
    createNewClient(config,
        new MultiNodePipelineBlockAllocator(config, data + parity, 15));
    String value = new String(new byte[chunkSize], UTF_8);
    OzoneBucket bucket = getOzoneBucket();

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();
      try (OzoneOutputStream out = bucket
          .createKey(keyName, value.getBytes(UTF_8).length,
              new ECReplicationConfig(data, parity,
                  ECReplicationConfig.EcCodec.RS, chunkSize),
              new HashMap<>())) {
        out.write(value.getBytes(UTF_8));
        out.write(value.getBytes(UTF_8));
      }
      OzoneKey key = bucket.getKey(keyName);
      assertEquals(keyName, key.getName());
    }
  }

  @Test
  public void testAppendFilePublishesSuffixAndTotalLength() throws IOException {
    OzoneBucket bucket = getHsyncBucket();
    String keyName = UUID.randomUUID().toString();
    byte[] prefix = "prefix".getBytes(UTF_8);
    writeKey(bucket, keyName, prefix);
    KeyLocation prefixBlock = lastCommit().getKeyArgs().getKeyLocations(0);
    int allocations = omTransport.getRequests(Type.AllocateBlock).size();

    try (OzoneOutputStream out = bucket.appendFile(keyName)) {
      assertEquals(prefix.length, out.getKeyOutputStream().getAppendPrefixLength());
      out.write("12345".getBytes(UTF_8));
      out.hsync();
      // The mock OM returns the prefix blocks in the key info. They must not be used as preallocated blocks.
      assertThat(omTransport.getRequests(Type.AllocateBlock)).hasSize(allocations + 1);
      assertThat(omTransport.getRequests(Type.CommitKey)).hasSize(2);
      assertTrue(lastCommit().getHsync());
      assertAppendPublication(prefixBlock, prefix.length, 5);

      // Nothing changed since the last publication: no OM call.
      out.hsync();
      assertThat(omTransport.getRequests(Type.CommitKey)).hasSize(2);

      // An hsync that advances within the same block is published.
      out.write("678".getBytes(UTF_8));
      out.hsync();
      assertThat(omTransport.getRequests(Type.CommitKey)).hasSize(3);
      assertTrue(lastCommit().getHsync());
      assertAppendPublication(prefixBlock, prefix.length, 8);
    }
    assertThat(omTransport.getRequests(Type.CommitKey)).hasSize(4);
    assertFalse(lastCommit().getHsync());
    assertAppendPublication(prefixBlock, prefix.length, 8);
  }

  @Test
  public void testCreateStreamSkipsSameBlockHsync() throws IOException {
    OzoneBucket bucket = getHsyncBucket();
    try (OzoneOutputStream out = bucket.createKey(UUID.randomUUID().toString(), 0,
        RatisReplicationConfig.getInstance(THREE), new HashMap<>())) {
      assertEquals(0, out.getKeyOutputStream().getAppendPrefixLength());
      out.write("12345".getBytes(UTF_8));
      out.hsync();
      out.write("678".getBytes(UTF_8));
      out.hsync();
      assertThat(omTransport.getRequests(Type.CommitKey)).hasSize(1);
    }
    assertEquals(8, lastCommit().getKeyArgs().getDataSize());
  }

  @Test
  public void testAppendEncryptedFileResumesCipherStream() throws Exception {
    KeyProvider.KeyVersion dek = mock(KeyProvider.KeyVersion.class);
    when(dek.getMaterial()).thenReturn(RandomUtils.secure().randomBytes(16));
    KeyProvider provider = mock(KeyProvider.class, withSettings().extraInterfaces(CryptoExtension.class));
    when(provider.getConf()).thenReturn(new Configuration());
    when(((CryptoExtension) provider).decryptEncryptedKey(any())).thenReturn(dek);
    keyProvider = provider;
    omTransport.setFileEncryptionInfo(OMPBHelper.convert(new FileEncryptionInfo(CipherSuite.AES_CTR_NOPADDING,
        CryptoProtocolVersion.ENCRYPTION_ZONES, new byte[16], RandomUtils.secure().randomBytes(16), "key", "key@0")));
    OzoneBucket bucket = getOzoneBucket();
    String keyName = UUID.randomUUID().toString();
    // The prefix does not end on a cipher block boundary.
    byte[] prefix = RandomUtils.secure().randomBytes(37);
    byte[] suffix = RandomUtils.secure().randomBytes(100);
    writeKey(bucket, keyName, prefix);

    try (OzoneOutputStream out = bucket.appendFile(keyName)) {
      assertEquals(prefix.length, out.getKeyOutputStream().getAppendPrefixLength());
      out.write(suffix);
    }

    assertEquals(prefix.length + suffix.length, lastCommit().getKeyArgs().getDataSize());
    assertArrayEquals(ArrayUtils.addAll(prefix, suffix), readKey(bucket, keyName));
  }

  @Test
  public void testAppendGdprFileIsRejected() throws IOException {
    OzoneBucket bucket = getOzoneBucket();
    String keyName = UUID.randomUUID().toString();
    try (OzoneOutputStream out = bucket.createKey(keyName, 0, RatisReplicationConfig.getInstance(THREE),
        Collections.singletonMap(OzoneConsts.GDPR_FLAG, "true"))) {
      out.write("prefix".getBytes(UTF_8));
    }

    int commits = omTransport.getRequests(Type.CommitKey).size();
    long prefixLength = lastCommit().getKeyArgs().getDataSize();

    assertThatThrownBy(() -> bucket.appendFile(keyName))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("GDPR");

    // The session OM admitted is closed without adding data, the file is not left reserved.
    assertThat(omTransport.getRequests(Type.CommitKey)).hasSize(commits + 1);
    assertFalse(lastCommit().getHsync());
    assertEquals(prefixLength, lastCommit().getKeyArgs().getDataSize());
    assertThat(lastCommit().getKeyArgs().getKeyLocationsList()).isEmpty();
  }

  @Test
  public void testAppendFileKeepsECReplication() throws IOException {
    close();
    OzoneConfiguration config = new OzoneConfiguration();
    ReplicationConfigValidator validator = config.getObject(ReplicationConfigValidator.class);
    validator.disableValidation();
    config.setFromObject(validator);
    int chunkSize = 1024;
    ECReplicationConfig ecConfig = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, chunkSize);
    createNewClient(config, new MultiNodePipelineBlockAllocator(config, ecConfig.getRequiredNodes(), 15));
    // The bucket default stays RATIS, the stream must follow the file.
    OzoneBucket bucket = getOzoneBucket();
    String keyName = UUID.randomUUID().toString();
    byte[] prefix = RandomUtils.secure().randomBytes(chunkSize + 10);
    byte[] suffix = RandomUtils.secure().randomBytes(2 * chunkSize + 20);
    try (OzoneOutputStream out = bucket.createKey(keyName, 0, ecConfig, new HashMap<>())) {
      out.write(prefix);
    }

    try (OzoneOutputStream out = bucket.appendFile(keyName)) {
      assertThat(out.getOutputStream()).isInstanceOf(ECKeyOutputStream.class);
      assertEquals(prefix.length, out.getKeyOutputStream().getAppendPrefixLength());
      out.write(suffix);
    }

    assertEquals(prefix.length + suffix.length, lastCommit().getKeyArgs().getDataSize());
    assertThat(lastCommit().getKeyArgs().getKeyLocationsList()).hasSize(1);
    assertArrayEquals(ArrayUtils.addAll(prefix, suffix), readKey(bucket, keyName));
  }

  /** The last commit or hsync of an append must carry the suffix block only and the total file length. */
  private void assertAppendPublication(KeyLocation prefixBlock, long prefixLength, long suffixLength) {
    KeyArgs keyArgs = lastCommit().getKeyArgs();
    assertEquals(prefixLength + suffixLength, keyArgs.getDataSize());
    assertThat(keyArgs.getKeyLocationsList()).hasSize(1);
    assertNotEquals(prefixBlock.getBlockID(), keyArgs.getKeyLocations(0).getBlockID());
    assertEquals(suffixLength, keyArgs.getKeyLocations(0).getLength());
  }

  private CommitKeyRequest lastCommit() {
    List<OMRequest> commits = omTransport.getRequests(Type.CommitKey);
    return commits.get(commits.size() - 1).getCommitKeyRequest();
  }

  private static void writeKey(OzoneBucket bucket, String keyName, byte[] data) throws IOException {
    try (OzoneOutputStream out = bucket.createKey(keyName, 0, RatisReplicationConfig.getInstance(THREE),
        new HashMap<>())) {
      out.write(data);
    }
  }

  private static byte[] readKey(OzoneBucket bucket, String keyName) throws IOException {
    try (OzoneInputStream in = bucket.readKey(keyName)) {
      return IOUtils.toByteArray(in);
    }
  }

  /** Recreates the client with hsync enabled. Keys must use three replicas to be able to hsync. */
  private OzoneBucket getHsyncBucket() throws IOException {
    close();
    OzoneConfiguration config = new OzoneConfiguration();
    config.setBoolean(OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
    config.setBoolean("ozone.client.hbase.enhancements.allowed", true);
    config.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    createNewClient(config, new SinglePipelineBlockAllocator(config));
    return getOzoneBucket();
  }

  private OzoneBucket getOzoneBucket() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    return volume.getBucket(bucketName);
  }
}
