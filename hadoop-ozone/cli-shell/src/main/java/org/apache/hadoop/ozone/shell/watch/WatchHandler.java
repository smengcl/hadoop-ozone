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

package org.apache.hadoop.ozone.shell.watch;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.InotifyEvent;
import org.apache.hadoop.ozone.om.helpers.InotifyOpType;
import org.apache.hadoop.ozone.om.helpers.InotifyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InotifyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InotifyRequest.Builder;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.prefix.PrefixUri;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Watch inotify events for a given prefix.
 */
@Command(name = "watch",
    description = "Watch inotify events for a prefix")
public class WatchHandler extends Handler {

  @CommandLine.Mixin
  private PrefixUri uri;

  @CommandLine.Option(
      names = {"--recursive"},
      description = "Watch subtree under the prefix",
      defaultValue = "true")
  private boolean recursive;

  @CommandLine.Option(
      names = {"--op-type"},
      description = "Filter by op type: READ, WRITE, READ_AND_WRITE",
      defaultValue = "READ_AND_WRITE")
  private String opType;

  @CommandLine.Option(
      names = {"--since-write-seq"},
      description = "Start from write sequence number",
      defaultValue = "0")
  private long sinceWriteSeq;

  @CommandLine.Option(
      names = {"--since-access-seq"},
      description = "Start from access sequence number",
      defaultValue = "0")
  private long sinceAccessSeq;

  @CommandLine.Option(
      names = {"--limit"},
      description = "Max events per poll",
      defaultValue = "1000")
  private long limit;

  @CommandLine.Option(
      names = {"--interval-ms"},
      description = "Poll interval in milliseconds",
      defaultValue = "1000")
  private long intervalMs;

  @CommandLine.Option(
      names = {"--types"},
      split = ",",
      description = "Filter event types by name, comma-separated")
  private Set<InotifyEvent.EventType> eventTypes;

  @CommandLine.Option(
      names = {"--json"},
      description = "Output events as JSON",
      defaultValue = "false")
  private boolean json;

  @Override
  protected OzoneAddress getAddress() {
    return uri.getValue();
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {
    String prefix = buildPrefix(address);
    InotifyOpType opFilter = parseOpType(opType);
    Set<InotifyEvent.EventType> typeFilter = eventTypes == null
        ? EnumSet.allOf(InotifyEvent.EventType.class)
        : EnumSet.copyOf(eventTypes);

    long writeSeq = sinceWriteSeq;
    long accessSeq = sinceAccessSeq;
    while (true) {
      InotifyResponse response = fetchOnce(
          client, prefix, writeSeq, accessSeq, opFilter);
      if (response.isOverflow()) {
        err().println("Write event overflow detected; resetting cursor.");
      }
      if (response.isAccessOverflow()) {
        err().println("Access event overflow detected; resetting cursor.");
      }
      if (response.getWriteSequenceNumber() > 0) {
        writeSeq = response.getWriteSequenceNumber();
      }
      if (response.getAccessSequenceNumber() > 0) {
        accessSeq = response.getAccessSequenceNumber();
      }
      for (InotifyEvent event : response.getEvents()) {
        if (typeFilter.contains(event.getEventType())) {
          printEvent(event);
        }
      }
      for (InotifyEvent event : response.getAccessEvents()) {
        if (typeFilter.contains(event.getEventType())) {
          printEvent(event);
        }
      }
      sleepQuietly(intervalMs);
    }
  }

  InotifyResponse fetchOnce(OzoneClient client, String prefix,
      long writeSeq, long accessSeq, InotifyOpType opFilter)
      throws IOException {
    Builder request = InotifyRequest.newBuilder()
        .setWriteSequenceNumber(writeSeq)
        .setAccessSequenceNumber(accessSeq)
        .setLimitCount(limit)
        .setPathPrefix(prefix)
        .setRecursive(recursive)
        .setOpType(org.apache.hadoop.ozone.protocol.proto
            .OzoneManagerProtocolProtos.InotifyOpType.valueOf(opFilter.name()));
    return client.getProxy().getInotifyEvents(request.build());
  }

  private void printEvent(InotifyEvent event)
      throws IOException {
    if (json) {
      printObjectAsJson(event);
      return;
    }
    String path = "o3://" + event.getPath();
    if (event.getEventType() == InotifyEvent.EventType.MOVED_FROM &&
        event.getSrcPath() != null) {
      out().printf("[seq=%d] %s o3://%s -> %s%n",
          event.getSequenceNumber(),
          event.getEventType().name(),
          event.getSrcPath(),
          path);
    } else {
      out().printf("[seq=%d] %s %s%n",
          event.getSequenceNumber(),
          event.getEventType().name(),
          path);
    }
  }

  private static String buildPrefix(OzoneAddress address) {
    String base = address.getVolumeName() + "/" + address.getBucketName();
    String key = address.getKeyName();
    if (key == null || key.isEmpty()) {
      return base;
    }
    return base + "/" + key;
  }

  private static InotifyOpType parseOpType(String raw) {
    if (raw == null) {
      return InotifyOpType.READ_AND_WRITE;
    }
    String normalized = raw.trim().toUpperCase(Locale.ROOT);
    return InotifyOpType.valueOf(normalized);
  }

  private static void sleepQuietly(long intervalMs) {
    try {
      TimeUnit.MILLISECONDS.sleep(intervalMs);
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
    }
  }
}
