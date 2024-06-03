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

package org.apache.hadoop.hdds.conf;

import org.apache.ratis.server.RaftServerConfigKeys;

import java.time.Duration;

import static org.apache.hadoop.hdds.conf.ConfigTag.DATANODE;
import static org.apache.hadoop.hdds.conf.ConfigTag.DATASTREAM;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.PERFORMANCE;
import static org.apache.hadoop.hdds.conf.ConfigTag.RATIS;
import static org.apache.hadoop.hdds.ratis.RatisHelper.HDDS_DATANODE_RATIS_PREFIX_KEY;

/**
 * Datanode Ratis server Configuration.
 */
@ConfigGroup(prefix = HDDS_DATANODE_RATIS_PREFIX_KEY + "."
    + RaftServerConfigKeys.PREFIX)
public class DatanodeRatisServerConfig {

  @Config(key = "rpc.request.timeout",
      defaultValue = "60s",
      type = ConfigType.TIME,
      tags = {OZONE, DATANODE, RATIS},
      description = "The timeout duration of the ratis write request " +
          "on Ratis Server."
  )
  private long requestTimeOut = Duration.ofSeconds(60).toMillis();

  public long getRequestTimeOut() {
    return requestTimeOut;
  }

  public void setRequestTimeOut(Duration duration) {
    this.requestTimeOut = duration.toMillis();
  }

  // hdds.ratis.raft.server.watch.timeout
  @Config(key = "watch.timeout",
      defaultValue = "10s",
      type = ConfigType.TIME,
      tags = {OZONE, DATANODE, RATIS},
      description = "The timeout duration for watch request on Ratis Server. " +
          "Timeout for the watch request in Ratis server to acknowledge a " +
          "particular request is replayed to all servers."
  )
  private long watchTimeOut = Duration.ofSeconds(180).toMillis();

  public long getWatchTimeOut() {
    return watchTimeOut;
  }

  public void setWatchTimeOut(Duration duration) {
    this.watchTimeOut = duration.toMillis();
  }

  // hdds.ratis.raft.server.notification.no-leader.timeout
  @Config(key = "notification.no-leader.timeout",
      defaultValue = "300s",
      type = ConfigType.TIME,
      tags = {OZONE, DATANODE, RATIS},
      description = "Time out duration after which StateMachine gets notified" +
          " that leader has not been elected for a long time and leader " +
          "changes its role to Candidate."
  )
  private long noLeaderTimeout = Duration.ofSeconds(300).toMillis();

  public long getNoLeaderTimeout() {
    return noLeaderTimeout;
  }

  public void setNoLeaderTimeout(Duration duration) {
    this.noLeaderTimeout = duration.toMillis();
  }

  // hdds.ratis.raft.server.rpc.slowness.timeout
  @Config(key = "rpc.slowness.timeout",
      defaultValue = "300s",
      type = ConfigType.TIME,
      tags = {OZONE, DATANODE, RATIS},
      description = "Timeout duration after which stateMachine will be " +
          "notified that follower is slow. StateMachine will close down the " +
          "pipeline."
  )
  private long followerSlownessTimeout = Duration.ofSeconds(300).toMillis();

  public long getFollowerSlownessTimeout() {
    return followerSlownessTimeout;
  }

  public void setFollowerSlownessTimeout(Duration duration) {
    this.followerSlownessTimeout = duration.toMillis();
  }

  // hdds.ratis.raft.server.write.element-limit
  @Config(key = "write.element-limit",
      defaultValue = "1024",
      type = ConfigType.INT,
      tags = {OZONE, DATANODE, RATIS, PERFORMANCE},
      description = "Maximum number of pending requests after which the " +
          "leader starts rejecting requests from client."
  )
  private int leaderNumPendingRequests;

  public int getLeaderNumPendingRequests() {
    return leaderNumPendingRequests;
  }

  public void setLeaderNumPendingRequests(int leaderNumPendingRequests) {
    this.leaderNumPendingRequests = leaderNumPendingRequests;
  }

  // hdds.ratis.raft.server.datastream.request.threads
  @Config(key = "datastream.request.threads",
      defaultValue = "20",
      type = ConfigType.INT,
      tags = {OZONE, DATANODE, RATIS, DATASTREAM},
      description = "Maximum number of threads in the thread pool for " +
          "datastream request."
  )
  private int streamRequestThreads;

  public int getStreamRequestThreads() {
    return streamRequestThreads;
  }

  public void setStreamRequestThreads(int streamRequestThreads) {
    this.streamRequestThreads = streamRequestThreads;
  }

  @Config(key = "datastream.client.pool.size",
      defaultValue = "10",
      type = ConfigType.INT,
      tags = {OZONE, DATANODE, RATIS, DATASTREAM},
      description = "Maximum number of client proxy in NettyServerStreamRpc " +
          "for datastream write."
  )
  private int clientPoolSize;

  public int getClientPoolSize() {
    return clientPoolSize;
  }

  public void setClientPoolSize(int clientPoolSize) {
    this.clientPoolSize = clientPoolSize;
  }

  @Config(key = "delete.ratis.log.directory",
          defaultValue = "true",
          type = ConfigType.BOOLEAN,
          tags = {OZONE, DATANODE, RATIS},
          description = "Flag to indicate whether ratis log directory will be" +
                  "cleaned up during pipeline remove."
  )
  private boolean shouldDeleteRatisLogDirectory;

  public boolean shouldDeleteRatisLogDirectory() {
    return shouldDeleteRatisLogDirectory;
  }

  public void setLeaderNumPendingRequests(boolean delete) {
    this.shouldDeleteRatisLogDirectory = delete;
  }

  @Config(key = "leaderelection.pre-vote",
      defaultValue = "true",
      type = ConfigType.BOOLEAN,
      tags = {OZONE, DATANODE, RATIS},
      description = "Flag to enable/disable ratis election pre-vote."
  )
  private boolean preVoteEnabled = true;

  public boolean isPreVoteEnabled() {
    return preVoteEnabled;
  }

  public void setPreVote(boolean preVote) {
    this.preVoteEnabled = preVote;
  }

  /** @see RaftServerConfigKeys.Log.Appender#WAIT_TIME_MIN_KEY */
  @Config(key = "log.appender.wait-time.min",
      defaultValue = "0us",
      type = ConfigType.TIME,
      tags = {OZONE, DATANODE, RATIS, PERFORMANCE},
      description = "The minimum wait time between two appendEntries calls. " +
          "In some error conditions, the leader may keep retrying " +
          "appendEntries. If it happens, increasing this value to, say, " +
          "5us (microseconds) can help avoid the leader being too busy " +
          "retrying."
  )
  private long logAppenderWaitTimeMin;

  public long getLogAppenderWaitTimeMin() {
    return logAppenderWaitTimeMin;
  }

  public void setLogAppenderWaitTimeMin(long logAppenderWaitTimeMin) {
    this.logAppenderWaitTimeMin = logAppenderWaitTimeMin;
  }
}
