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

/**
 * Inotify event for Ozone namespace changes and access events.
 */
public final class InotifyEvent {

  public enum EventType {
    CREATE,
    DELETE,
    MODIFY,
    MOVED_FROM,
    MOVED_TO,
    ATTRIB,
    CLOSE_WRITE,
    ACCESS
  }

  private final EventType eventType;
  private final String path;
  private final String srcPath;
  private final boolean isDir;
  private final long timestamp;
  private final long sequenceNumber;

  public InotifyEvent(EventType eventType, String path, String srcPath,
      boolean isDir, long timestamp, long sequenceNumber) {
    this.eventType = eventType;
    this.path = path;
    this.srcPath = srcPath;
    this.isDir = isDir;
    this.timestamp = timestamp;
    this.sequenceNumber = sequenceNumber;
  }

  public EventType getEventType() {
    return eventType;
  }

  public String getPath() {
    return path;
  }

  public String getSrcPath() {
    return srcPath;
  }

  public boolean isDir() {
    return isDir;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getSequenceNumber() {
    return sequenceNumber;
  }
}
