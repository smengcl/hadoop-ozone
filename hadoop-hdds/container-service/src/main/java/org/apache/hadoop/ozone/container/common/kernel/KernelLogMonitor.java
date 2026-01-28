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

package org.apache.hadoop.ozone.container.common.kernel;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitor Linux kernel logs for storage error signals and fail the mapped
 * Ozone volume when the signal is decisive.
 */
public class KernelLogMonitor implements Closeable, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(KernelLogMonitor.class);

  private static final Pattern DEVICE_PATTERN = Pattern.compile(
      "\\b((sd[a-z]+\\d*)"
          + "|(nvme\\d+n\\d+(p\\d+)?)"
          + "|(dm-\\d+)"
          + "|(md\\d+)"
          + "|(vd[a-z]+\\d*)"
          + "|(xvd[a-z]+\\d*)"
          + "|(mmcblk\\d+p?\\d*)"
          + "|(loop\\d+))\\b");

  private static final Pattern[] HIGH_CONFIDENCE_PATTERNS = new Pattern[] {
      Pattern.compile("I/O error", Pattern.CASE_INSENSITIVE),
      Pattern.compile("Buffer I/O error", Pattern.CASE_INSENSITIVE),
      Pattern.compile("blk_update_request: I/O error", Pattern.CASE_INSENSITIVE),
      Pattern.compile("end_request: I/O error", Pattern.CASE_INSENSITIVE),
      Pattern.compile("rejecting I/O to offline device", Pattern.CASE_INSENSITIVE),
      Pattern.compile("Device offlined", Pattern.CASE_INSENSITIVE),
      Pattern.compile("Sense Key\\s*:\\s*(Medium Error|Hardware Error)",
          Pattern.CASE_INSENSITIVE),
      Pattern.compile("nvme.*reset.*failed", Pattern.CASE_INSENSITIVE),
      Pattern.compile("nvme.*failed", Pattern.CASE_INSENSITIVE),
      Pattern.compile("Remounting filesystem read-only", Pattern.CASE_INSENSITIVE),
      Pattern.compile("EXT4-fs error", Pattern.CASE_INSENSITIVE),
      Pattern.compile("XFS.*(corruption|shutdown)", Pattern.CASE_INSENSITIVE),
      Pattern.compile("BTRFS.*(error|corruption)", Pattern.CASE_INSENSITIVE)
  };

  private static final Pattern[] WARNING_PATTERNS = new Pattern[] {
      Pattern.compile("timed out", Pattern.CASE_INSENSITIVE),
      Pattern.compile("timeout", Pattern.CASE_INSENSITIVE),
      Pattern.compile("link reset", Pattern.CASE_INSENSITIVE),
      Pattern.compile("resetting link", Pattern.CASE_INSENSITIVE),
      Pattern.compile("controller reset", Pattern.CASE_INSENSITIVE)
  };

  private static final int WARNING_THRESHOLD_COUNT = 3;
  private static final long WARNING_THRESHOLD_WINDOW_MS = 60_000L;

  private final KernelLogReader reader;
  private final DeviceMapper deviceMapper;
  private final Map<String, MutableVolumeSet> volumeRootToSet;
  private final Clock clock;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private Thread thread;

  private final Map<String, Deque<Long>> warningEvents = new HashMap<>();

  public KernelLogMonitor(KernelLogReader reader,
      DeviceMapper deviceMapper,
      Map<String, MutableVolumeSet> volumeRootToSet,
      Clock clock) {
    this.reader = Objects.requireNonNull(reader, "reader");
    this.deviceMapper = Objects.requireNonNull(deviceMapper, "deviceMapper");
    this.volumeRootToSet = Objects.requireNonNull(volumeRootToSet, "volumeRootToSet");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  public static KernelLogMonitor createDefault(
      List<MutableVolumeSet> volumeSets, String threadNamePrefix) {
    Map<String, MutableVolumeSet> rootMap = buildVolumeRootMap(volumeSets);
    List<String> volumeRoots = new ArrayList<>(rootMap.keySet());
    KernelLogReader reader = new KmsgKernelLogReader();
    DeviceMapper mapper = new SystemDeviceMapper(volumeRoots,
        Path.of("/sys/class/block"), Path.of("/proc/self/mountinfo"));
    KernelLogMonitor monitor = new KernelLogMonitor(reader, mapper, rootMap,
        Clock.systemUTC());
    monitor.thread = new Thread(monitor, threadNamePrefix + "KernelLogMonitor");
    monitor.thread.setDaemon(true);
    return monitor;
  }

  public void start() {
    if (thread == null) {
      throw new IllegalStateException("Monitor thread not initialized");
    }
    if (running.compareAndSet(false, true)) {
      thread.start();
    }
  }

  @Override
  public void run() {
    while (running.get()) {
      try {
        String line = reader.readLine();
        if (line == null) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          continue;
        }
        handleLine(line);
      } catch (IOException e) {
        if (running.get()) {
          LOG.warn("Kernel log monitor read failed, stopping.", e);
        }
        break;
      } catch (Exception e) {
        LOG.warn("Kernel log monitor error while processing line.", e);
      }
    }
  }

  @Override
  public void close() {
    running.set(false);
    try {
      reader.close();
    } catch (IOException e) {
      LOG.debug("Failed to close kernel log reader", e);
    }
    if (thread != null) {
      thread.interrupt();
    }
  }

  void handleLine(String rawLine) {
    String message = normalizeMessage(rawLine);
    if (message == null || message.isEmpty()) {
      return;
    }

    MatchResult match = classify(message);
    if (match == MatchResult.NONE) {
      return;
    }

    String device = extractDeviceName(message);
    if (device == null) {
      LOG.debug("Kernel log matched but no device could be extracted: {}", message);
      return;
    }

    if (match == MatchResult.HIGH_CONFIDENCE) {
      failDevice(device, message, "high-confidence");
    } else {
      if (shouldEscalateWarning(device, message)) {
        failDevice(device, message, "warning-threshold");
      }
    }
  }

  private void failDevice(String device, String message, String reason) {
    List<String> volumeRoots = deviceMapper.findVolumeRoots(device);
    if (volumeRoots.isEmpty()) {
      LOG.warn("Kernel disk error for device {} but no volume match found. msg={}", device, message);
      return;
    }
    if (volumeRoots.size() > 1) {
      LOG.warn("Kernel disk error for device {} maps to multiple volumes {}. msg={}",
          device, volumeRoots, message);
      return;
    }

    String volumeRoot = volumeRoots.get(0);
    MutableVolumeSet volumeSet = volumeRootToSet.get(volumeRoot);
    if (volumeSet == null) {
      LOG.warn("Kernel disk error mapped to unknown volume {}. msg={}", volumeRoot, message);
      return;
    }
    LOG.error("Failing volume {} due to kernel disk error ({}). device={} msg={}",
        volumeRoot, reason, device, message);
    volumeSet.failVolume(volumeRoot);
  }

  private boolean shouldEscalateWarning(String device, String message) {
    String key = device + "|" + warningPatternKey(message);
    long now = clock.millis();
    Deque<Long> events = warningEvents.computeIfAbsent(key, k -> new ArrayDeque<>());
    events.addLast(now);
    long cutoff = now - WARNING_THRESHOLD_WINDOW_MS;
    while (!events.isEmpty() && events.peekFirst() < cutoff) {
      events.removeFirst();
    }
    return events.size() >= WARNING_THRESHOLD_COUNT;
  }

  private static MatchResult classify(String message) {
    for (Pattern pattern : HIGH_CONFIDENCE_PATTERNS) {
      if (pattern.matcher(message).find()) {
        return MatchResult.HIGH_CONFIDENCE;
      }
    }
    for (Pattern pattern : WARNING_PATTERNS) {
      if (pattern.matcher(message).find()) {
        return MatchResult.WARNING;
      }
    }
    return MatchResult.NONE;
  }

  private static String warningPatternKey(String message) {
    for (int i = 0; i < WARNING_PATTERNS.length; i++) {
      if (WARNING_PATTERNS[i].matcher(message).find()) {
        return "warn-" + i;
      }
    }
    return "warn-unknown";
  }

  private static String normalizeMessage(String rawLine) {
    int idx = rawLine.indexOf(';');
    if (idx >= 0 && idx + 1 < rawLine.length()) {
      return rawLine.substring(idx + 1).trim();
    }
    return rawLine.trim();
  }

  private static String extractDeviceName(String message) {
    Matcher matcher = DEVICE_PATTERN.matcher(message);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }

  private static Map<String, MutableVolumeSet> buildVolumeRootMap(
      List<MutableVolumeSet> volumeSets) {
    Map<String, MutableVolumeSet> rootMap = new HashMap<>();
    for (MutableVolumeSet volumeSet : volumeSets) {
      if (volumeSet == null) {
        continue;
      }
      for (StorageVolume volume : volumeSet.getVolumesList()) {
        rootMap.put(volume.getStorageDir().getPath(), volumeSet);
      }
    }
    return rootMap;
  }

  enum MatchResult {
    NONE,
    WARNING,
    HIGH_CONFIDENCE
  }
}

interface KernelLogReader extends Closeable {
  String readLine() throws IOException;
}

final class KmsgKernelLogReader implements KernelLogReader {
  private final BufferedReader reader;

  KmsgKernelLogReader() {
    try {
      this.reader = new BufferedReader(new InputStreamReader(
          new FileInputStream("/dev/kmsg"), UTF_8));
    } catch (IOException e) {
      throw new IllegalStateException("Unable to open /dev/kmsg", e);
    }
  }

  @Override
  public String readLine() throws IOException {
    return reader.readLine();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}

interface DeviceMapper {
  List<String> findVolumeRoots(String deviceName);
}

final class SystemDeviceMapper implements DeviceMapper {

  private static final Logger LOG = LoggerFactory.getLogger(SystemDeviceMapper.class);

  private final Map<String, List<String>> majorMinorToRoots;
  private final Path sysClassBlockPath;

  SystemDeviceMapper(List<String> volumeRoots, Path sysClassBlockPath, Path mountInfoPath) {
    this.sysClassBlockPath = sysClassBlockPath;
    this.majorMinorToRoots = buildMajorMinorToRoots(volumeRoots, mountInfoPath);
  }

  @Override
  public List<String> findVolumeRoots(String deviceName) {
    Set<String> majors = resolveDeviceMajorMinors(deviceName);
    if (majors.isEmpty()) {
      return Collections.emptyList();
    }
    Set<String> roots = new HashSet<>();
    for (String majorMinor : majors) {
      List<String> mapped = majorMinorToRoots.get(majorMinor);
      if (mapped != null) {
        roots.addAll(mapped);
      }
    }
    return new ArrayList<>(roots);
  }

  private Set<String> resolveDeviceMajorMinors(String deviceName) {
    Path deviceDir = sysClassBlockPath.resolve(deviceName);
    if (!Files.exists(deviceDir)) {
      return Collections.emptySet();
    }
    Set<String> majors = new HashSet<>();
    addMajorMinor(deviceDir.resolve("dev"), majors);

    try {
      Files.list(deviceDir).forEach(path -> {
        if (Files.isDirectory(path)) {
          String name = path.getFileName().toString();
          if (name.startsWith(deviceName)) {
            addMajorMinor(path.resolve("dev"), majors);
          }
        }
      });
    } catch (IOException e) {
      LOG.debug("Failed to list sysfs entries for {}", deviceName, e);
    }
    return majors;
  }

  private static void addMajorMinor(Path devPath, Set<String> majors) {
    if (!Files.exists(devPath)) {
      return;
    }
    try {
      String content = Files.readString(devPath, UTF_8).trim();
      if (!content.isEmpty()) {
        majors.add(content);
      }
    } catch (IOException e) {
      LOG.debug("Failed to read {}", devPath, e);
    }
  }

  private static Map<String, List<String>> buildMajorMinorToRoots(
      List<String> volumeRoots, Path mountInfoPath) {
    List<MountInfo> mounts = parseMountInfo(mountInfoPath);
    Map<String, List<String>> mapping = new HashMap<>();
    for (String root : volumeRoots) {
      MountInfo mount = findBestMount(mounts, root);
      if (mount == null) {
        continue;
      }
      mapping.computeIfAbsent(mount.majorMinor, k -> new ArrayList<>()).add(root);
    }
    return mapping;
  }

  private static MountInfo findBestMount(List<MountInfo> mounts, String path) {
    MountInfo best = null;
    for (MountInfo mount : mounts) {
      if (path.startsWith(mount.mountPoint)) {
        if (best == null || mount.mountPoint.length() > best.mountPoint.length()) {
          best = mount;
        }
      }
    }
    return best;
  }

  private static List<MountInfo> parseMountInfo(Path mountInfoPath) {
    if (!Files.exists(mountInfoPath)) {
      return Collections.emptyList();
    }
    List<MountInfo> mounts = new ArrayList<>();
    try {
      List<String> lines = Files.readAllLines(mountInfoPath, UTF_8);
      for (String line : lines) {
        int sep = line.indexOf(" - ");
        String left = sep >= 0 ? line.substring(0, sep) : line;
        String[] parts = left.split(" ");
        if (parts.length < 5) {
          continue;
        }
        String majorMinor = parts[2];
        String mountPoint = parts[4].replace("\\040", " ");
        mounts.add(new MountInfo(majorMinor, mountPoint));
      }
    } catch (IOException e) {
      LOG.debug("Failed to parse mountinfo", e);
    }
    return mounts;
  }

  private static final class MountInfo {
    private final String majorMinor;
    private final String mountPoint;

    private MountInfo(String majorMinor, String mountPoint) {
      this.majorMinor = majorMinor;
      this.mountPoint = mountPoint;
    }
  }
}
