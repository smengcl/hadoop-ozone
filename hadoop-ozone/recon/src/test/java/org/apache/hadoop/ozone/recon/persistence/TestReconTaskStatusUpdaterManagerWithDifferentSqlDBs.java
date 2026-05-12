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

package org.apache.hadoop.ozone.recon.persistence;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.inject.Provider;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.ozone.recon.schema.ReconTaskSchemaDefinition;
import org.apache.ozone.recon.schema.generated.tables.daos.ReconTaskStatusDao;
import org.jooq.DSLContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies task-status upgrade compatibility across supported embedded SQL DBs.
 */
public class TestReconTaskStatusUpdaterManagerWithDifferentSqlDBs {
  @TempDir
  private static Path temporaryFolder;

  public static Stream<Object> parametersSource() throws IOException {
    return Stream.of(
        new AbstractReconSqlDBTest.DerbyDataSourceConfigurationProvider(
            Files.createDirectory(temporaryFolder.resolve("UpdaterDerbyDB"))
                .toFile()),
        new TestReconWithDifferentSqlDBs.SqliteDataSourceConfigurationProvider(
            Files.createDirectory(temporaryFolder.resolve("UpdaterSqliteDB"))
                .toFile()));
  }

  @ParameterizedTest
  @MethodSource("parametersSource")
  public void testLoadLegacyTaskRowsWithoutUpgradeColumns(
      Provider<DataSourceConfiguration> provider) throws Exception {
    AbstractReconSqlDBTest reconSqlDB = new AbstractReconSqlDBTest(provider);
    reconSqlDB.createReconSchemaForTest(temporaryFolder);

    DSLContext dsl = reconSqlDB.getDslContext();
    dsl.alterTable(ReconTaskSchemaDefinition.RECON_TASK_STATUS_TABLE_NAME)
        .dropColumn("last_task_run_status")
        .execute();
    dsl.alterTable(ReconTaskSchemaDefinition.RECON_TASK_STATUS_TABLE_NAME)
        .dropColumn("is_current_task_running")
        .execute();
    dsl.insertInto(table(ReconTaskSchemaDefinition.RECON_TASK_STATUS_TABLE_NAME))
        .columns(field(name("task_name")),
            field(name("last_updated_timestamp")),
            field(name("last_updated_seq_number")))
        .values("legacy-task", 123L, 456L)
        .execute();

    ReconTaskStatusUpdaterManager manager =
        new ReconTaskStatusUpdaterManager(
            reconSqlDB.getDao(ReconTaskStatusDao.class));

    ReconTaskStatusUpdater existingTask =
        manager.getTaskStatusUpdater("legacy-task");
    assertEquals("legacy-task", existingTask.getTaskName());
    assertEquals(456L, existingTask.getLastUpdatedSeqNumber());

    ReconTaskStatusUpdater newTask = manager.getTaskStatusUpdater("new-task");
    assertEquals("new-task", newTask.getTaskName());
    assertEquals(0L, newTask.getLastUpdatedSeqNumber());
  }
}
