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

import com.google.inject.Inject;
import com.google.inject.Provider;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.sql.DataSource;
import org.sqlite.SQLiteDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide a {@link javax.sql.DataSource} for the application.
 */
public class SqliteDataSourceProvider implements Provider<DataSource> {
  private static final Logger LOG =
      LoggerFactory.getLogger(SqliteDataSourceProvider.class);
  private static final String SQLITE_URL_PREFIX = "jdbc:sqlite:";

  private DataSourceConfiguration configuration;

  @Inject
  public SqliteDataSourceProvider(DataSourceConfiguration configuration) {
    this.configuration = configuration;
  }

  /**
   * Create a pooled datasource for the application.
   * <p>
   * Default sqlite database does not work with a connection pool, actually
   * most embedded databases do not, hence returning native implementation for
   * default db.
   */
  @Override
  public DataSource get() {
    ensureParentDirectoryExists(configuration.getJdbcUrl());
    SQLiteDataSource ds = new SQLiteDataSource();
    ds.setUrl(configuration.getJdbcUrl());
    return ds;
  }

  private void ensureParentDirectoryExists(String jdbcUrl) {
    if (jdbcUrl == null || !jdbcUrl.startsWith(SQLITE_URL_PREFIX)) {
      return;
    }

    String dbPath = jdbcUrl.substring(SQLITE_URL_PREFIX.length());
    if (dbPath.isEmpty() || ":memory:".equals(dbPath) || dbPath.startsWith("file:")) {
      return;
    }

    Path parentDir = Paths.get(dbPath).getParent();
    if (parentDir == null) {
      return;
    }

    try {
      Files.createDirectories(parentDir);
    } catch (IOException e) {
      LOG.error("Failed to create parent directory for SQLite DB {}", dbPath, e);
      throw new IllegalStateException("Unable to create SQLite DB parent directory", e);
    }
  }
}
