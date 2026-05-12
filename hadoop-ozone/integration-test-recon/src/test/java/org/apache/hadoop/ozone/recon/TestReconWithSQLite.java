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

package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl.OmSnapshotTaskName.OmDeltaRequest;
import static org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl.OmSnapshotTaskName.OmSnapshotRequest;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import javax.sql.DataSource;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.ozone.test.LambdaTestUtils;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration test covering Recon startup with SQLite as the SQL backend.
 */
public class TestReconWithSQLite {
  private static final int WAIT_TIMEOUT_MS = 30000;
  private static final int POLL_INTERVAL_MS = 200;

  @TempDir
  private Path temporaryFolder;

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private OMMetadataManager metadataManager;
  private ReconService recon;
  private Path sqliteDbPath;

  @BeforeEach
  public void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    Path metadataDir = Files.createDirectories(
        temporaryFolder.resolve("metadata"));
    conf.set(OZONE_METADATA_DIRS, metadataDir.toString());

    ReconSqlDbConfig dbConfig = conf.getObject(ReconSqlDbConfig.class);
    dbConfig.setDriverClass("org.sqlite.JDBC");
    dbConfig.setJdbcUrl(String.format("jdbc:sqlite:${%s}/ozone_recon_sqlite.db",
        OZONE_RECON_DB_DIR));
    dbConfig.setSqlDbDialect(SQLDialect.SQLITE.toString());
    conf.setFromObject(dbConfig);

    recon = new ReconService(conf);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .addService(recon)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.getStorageContainerManager().exitSafeMode();

    client = cluster.newClient();
    metadataManager = cluster.getOzoneManager().getMetadataManager();
    sqliteDbPath = Paths.get(conf.get(OZONE_METADATA_DIRS),
        "recon", "ozone_recon_sqlite.db");
  }

  @AfterEach
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testReconStartsWithSQLiteAndPersistsTaskStatus()
      throws Exception {
    writeTestData("sqlite-vol", "sqlite-bucket", "sqlite-key", "sqlite-data");

    OzoneManagerServiceProviderImpl omServiceProvider =
        (OzoneManagerServiceProviderImpl)
            recon.getReconServer().getOzoneManagerServiceProvider();
    omServiceProvider.syncDataFromOM();

    long omLatestSeqNumber = ((RDBStore) metadataManager.getStore())
        .getDb().getLatestSequenceNumber();
    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
            recon.getReconServer().getReconStorageContainerManager();
    DataSource dataSource = reconScm.getDataSource();

    assertTrue(Files.exists(sqliteDbPath));
    try (Connection connection = dataSource.getConnection()) {
      assertTrue(connection.getMetaData().getURL().contains("sqlite"));
    }

    LambdaTestUtils.await(WAIT_TIMEOUT_MS, POLL_INTERVAL_MS, () -> {
      Long snapshotSeq = getTaskSequenceNumber(
          dataSource, OmSnapshotRequest.name());
      Long deltaSeq = getTaskSequenceNumber(
          dataSource, OmDeltaRequest.name());
      return omLatestSeqNumber == valueOrMinusOne(snapshotSeq) ||
          omLatestSeqNumber == valueOrMinusOne(deltaSeq);
    });

    Long snapshotSeq = getTaskSequenceNumber(dataSource, OmSnapshotRequest.name());
    Long deltaSeq = getTaskSequenceNumber(dataSource, OmDeltaRequest.name());
    assertNotNull(snapshotSeq != null ? snapshotSeq : deltaSeq);
  }

  private void writeTestData(String volumeName, String bucketName,
                             String keyName, String data) throws Exception {
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(volumeName);
    OzoneVolume volume = objectStore.getVolume(volumeName);
    volume.createBucket(bucketName);
    try (OzoneOutputStream out = volume.getBucket(bucketName)
        .createKey(keyName, data.length(), ReplicationType.RATIS, ONE,
            Collections.emptyMap())) {
      out.write(data.getBytes(StandardCharsets.UTF_8));
    }
  }

  private Long getTaskSequenceNumber(DataSource dataSource, String taskName)
      throws Exception {
    try (Connection connection = dataSource.getConnection();
         PreparedStatement statement = connection.prepareStatement(
             "SELECT last_updated_seq_number FROM RECON_TASK_STATUS " +
                 "WHERE task_name = ?")) {
      statement.setString(1, taskName);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (resultSet.next()) {
          return resultSet.getLong(1);
        }
      }
    }
    return null;
  }

  private long valueOrMinusOne(Long value) {
    return value == null ? -1L : value;
  }
}
