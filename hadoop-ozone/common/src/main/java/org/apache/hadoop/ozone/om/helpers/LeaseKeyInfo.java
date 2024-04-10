/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.om.helpers;

/**
 * This class represents LeaseKeyInfo.
 */
public class LeaseKeyInfo {
  private final OmKeyInfo keyInfo;
  /**
   * isKeyInfo = true indicates keyInfo is from keyTable.
   * isKeyInfo = false indicates keyInfo is from openKeyTable.
   */
  private boolean isKeyInfo;

  public LeaseKeyInfo(OmKeyInfo info, boolean isKeyInfo) {
    this.keyInfo = info;
    this.isKeyInfo = isKeyInfo;
  }

  public boolean getIsKeyInfo() {
    return this.isKeyInfo;
  }

  public OmKeyInfo getKeyInfo() {
    return keyInfo;
  }
}
