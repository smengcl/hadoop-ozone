/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderTokenIssuer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.security.token.DelegationTokenIssuer;

import java.io.IOException;
import java.net.URI;

/**
 * The Rooted Ozone Filesystem (OFS) implementation.
 * <p>
 * This subclass is marked as private as code should not be creating it
 * directly; use {@link FileSystem#get(org.apache.hadoop.conf.Configuration)}
 * and variants to create one. If cast to {@link RootedOzoneFileSystem}, extra
 * methods and features may be accessed. Consider those private and unstable.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RootedOzoneFileSystem extends BasicRootedOzoneFileSystem
    implements KeyProviderTokenIssuer {

  private OzoneFSStorageStatistics storageStatistics;

  public RootedOzoneFileSystem() {
    this.storageStatistics = new OzoneFSStorageStatistics();
  }

  @Override
  public KeyProvider getKeyProvider() throws IOException {
    return getImpl().getKeyProvider();
  }

  @Override
  public URI getKeyProviderUri() throws IOException {
    return getImpl().getKeyProviderUri();
  }

  @Override
  public DelegationTokenIssuer[] getAdditionalTokenIssuers()
      throws IOException {
    KeyProvider keyProvider;
    try {
      keyProvider = getKeyProvider();
    } catch (IOException ioe) {
      LOG.debug("Error retrieving KeyProvider.", ioe);
      return null;
    }
    if (keyProvider instanceof DelegationTokenIssuer) {
      return new DelegationTokenIssuer[]{(DelegationTokenIssuer)keyProvider};
    }
    return null;
  }

  StorageStatistics getOzoneFSOpsCountStatistics() {
    return storageStatistics;
  }

  @Override
  protected void incrementCounter(Statistic statistic) {
    if (storageStatistics != null) {
      storageStatistics.incrementCounter(statistic, 1);
    }
  }
}
