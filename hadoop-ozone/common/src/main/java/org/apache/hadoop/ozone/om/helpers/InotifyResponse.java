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

package org.apache.hadoop.ozone.om.helpers;

import java.util.ArrayList;
import java.util.List;

/**
 * Response wrapper for inotify events.
 */
public class InotifyResponse {
  private final List<InotifyEvent> events = new ArrayList<>();
  private final List<InotifyEvent> accessEvents = new ArrayList<>();
  private long writeSequenceNumber = -1;
  private long latestWriteSequenceNumber = -1;
  private long accessSequenceNumber = -1;
  private long latestAccessSequenceNumber = -1;
  private boolean overflow = false;
  private boolean accessOverflow = false;
  private boolean dbUpdateSuccess = true;

  public List<InotifyEvent> getEvents() {
    return events;
  }

  public List<InotifyEvent> getAccessEvents() {
    return accessEvents;
  }

  public long getWriteSequenceNumber() {
    return writeSequenceNumber;
  }

  public void setWriteSequenceNumber(long writeSequenceNumber) {
    this.writeSequenceNumber = writeSequenceNumber;
  }

  public long getLatestWriteSequenceNumber() {
    return latestWriteSequenceNumber;
  }

  public void setLatestWriteSequenceNumber(long latestWriteSequenceNumber) {
    this.latestWriteSequenceNumber = latestWriteSequenceNumber;
  }

  public long getAccessSequenceNumber() {
    return accessSequenceNumber;
  }

  public void setAccessSequenceNumber(long accessSequenceNumber) {
    this.accessSequenceNumber = accessSequenceNumber;
  }

  public long getLatestAccessSequenceNumber() {
    return latestAccessSequenceNumber;
  }

  public void setLatestAccessSequenceNumber(long latestAccessSequenceNumber) {
    this.latestAccessSequenceNumber = latestAccessSequenceNumber;
  }

  public boolean isOverflow() {
    return overflow;
  }

  public void setOverflow(boolean overflow) {
    this.overflow = overflow;
  }

  public boolean isAccessOverflow() {
    return accessOverflow;
  }

  public void setAccessOverflow(boolean accessOverflow) {
    this.accessOverflow = accessOverflow;
  }

  public boolean isDbUpdateSuccess() {
    return dbUpdateSuccess;
  }

  public void setDbUpdateSuccess(boolean dbUpdateSuccess) {
    this.dbUpdateSuccess = dbUpdateSuccess;
  }
}
