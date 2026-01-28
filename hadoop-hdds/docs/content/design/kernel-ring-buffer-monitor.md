---
title: Kernel Ring Buffer Monitor for Datanode Disk Health
summary: Use kernel log signals to detect disk issues early and mark volumes failed.
date: 2026-01-25
jira: HDDS-XXXX
status: draft
author: Ozone Contributors
---
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Abstract

This design adds a datanode feature that tails the Linux kernel ring buffer to detect early disk issues. The feature emits structured events and can mark the affected Ozone volume as failed, allowing existing volume failure handling, scanner/scrubber workflows, and SCM replication logic to react promptly.

# Goals

- Detect disk problems earlier than filesystem-level checks alone.
- Produce actionable, structured signals (device, subsystem, severity).
- Map kernel log signals to configured datanode volumes and mark them failed when justified.
- Keep the feature optional, disabled by default, and safe to deploy in secure environments.

# Non-goals

- Replace SMART, RAID, or vendor monitoring.
- Provide full kernel log aggregation.
- Automatically recover or re-enable failed volumes.

# Background

Kernel logs (ring buffer) capture storage errors such as I/O errors, resets, and link failures. These often appear before user-visible data loss. Ozone datanode already has volume scanning and failure handling; this feature integrates with those mechanisms to accelerate response.

# Feature Overview

The datanode runs a lightweight kernel log monitor that:

1. Reads kernel messages in real time.
2. Filters for storage-related errors.
3. Classifies signals into warning/error/critical.
4. Maps the device to an Ozone volume.
5. Optionally marks the volume as failed and triggers existing failure handling.

# Configuration

New configuration key:

- `hdds.datanode.kernel.log.monitor.enabled` (boolean, default `false`, defined in `ozone-default.xml`)

This enables the monitor inside the datanode process.

# Data Sources

Preferred sources, in order:

1. **/dev/kmsg** (best streaming source)
2. **journalctl -k -f** (systemd journal, if available)
3. **dmesg --follow** (fallback, less reliable)

# Permissions Model

Kernel messages are restricted on most modern distros.

- **/dev/kmsg or dmesg**: typically require `CAP_SYSLOG` when `kernel.dmesg_restrict=1`.
- **systemd journal**: requires `root` or membership in `systemd-journal` group; in hardened setups, additional policies may apply.

Recommended minimal permission model for Ubuntu/Debian/RHEL:

- Run datanode as user `om`.
- Grant `CAP_SYSLOG` only to the datanode service via systemd:
  - `AmbientCapabilities=CAP_SYSLOG`
  - `CapabilityBoundingSet=CAP_SYSLOG`
  - `NoNewPrivileges=no`

This allows kernel log access without full root privileges.

## Service startup and capabilities

Ozone services (including the datanode) are started through the shared Ozone shell launcher, which invokes the Java process using the `ozone` bash script (for example, `start-ozone.sh` uses `${OZONE_HOME}/bin/ozone --daemon start datanode`). Because this is a shell script:

- **`setcap` does not apply to scripts**. Linux file capabilities only apply to ELF binaries. Setting capabilities on the `ozone` script is ineffective (and in some cases refused by the kernel or ignored by the loader).
- **Do not setcap the shared launcher**. Even if it were possible, it would grant the capability to every Ozone service started through that script, which violates least privilege.

Preferred approach: grant `CAP_SYSLOG` at the *service* level (systemd unit or container capability), so only the datanode has it.

### Example: systemd unit capability override

Create an override for the datanode service (example uses a dedicated unit for datanode):

```
sudo systemctl edit ozone-datanode.service
```

Add:

```
[Service]
User=om
AmbientCapabilities=CAP_SYSLOG
CapabilityBoundingSet=CAP_SYSLOG
NoNewPrivileges=no
```

Reload and restart:

```
sudo systemctl daemon-reload
sudo systemctl restart ozone-datanode
```

### Example: container capability

If datanode runs in a container, add the capability and ensure `/dev/kmsg` is available:

```
docker run --cap-add=SYSLOG --device=/dev/kmsg ...
```

Kubernetes:

```
securityContext:
  capabilities:
    add: ["SYSLOG"]
```

# What Qualifies as a “Bad Disk” (Kernel Signals)

The monitor separates **signal detection** from **volume failure decision**.

## 1) Signals (raw kernel patterns)

Examples of high-confidence storage errors:

- **I/O errors**
  - `I/O error` / `Buffer I/O error`
  - `blk_update_request: I/O error`
  - `end_request: I/O error`
- **Device offline or removed**
  - `rejecting I/O to offline device`
  - `Device offlined` / `detached`
- **SCSI sense and medium errors**
  - `Sense Key : Medium Error` / `Hardware Error`
  - `ASC/ASCQ` codes indicating media or hardware failure
- **NVMe fatal errors**
  - `nvme.*failed` / `controller reset failed`
  - `I/O timeout` followed by `reset failed`
- **Filesystem becomes read-only**
  - `Remounting filesystem read-only`
  - `EXT4-fs error` / `XFS.*(corruption|shutdown)`

Examples of lower-confidence warnings:

- `timed out` / `link reset` without subsequent I/O errors
- Single transient `reset` that recovers

## 2) Decision Rules (mark volume failed)

A volume is marked failed when **any** of the following is true:

- A high-confidence error is observed for a mapped device.
- A warning pattern is observed **N** times within **T** seconds (configurable defaults: N=3, T=60s).
- The kernel reports the filesystem on the device remounted read-only.

The monitor emits events for all matched patterns, but only escalates to volume failure when the decision rules are satisfied.

# Device to Volume Mapping

To identify which Ozone volume is impacted:

1. Parse device identifiers from log line (e.g., `sda`, `nvme0n1`, `dm-2`).
2. Resolve to a block device path (`/dev/sda`, `/dev/nvme0n1`, `/dev/dm-2`).
3. Map device to mount points via `/proc/self/mountinfo`.
4. Match mount points against configured Ozone data, metadata, and DB volume roots:
   - `hdds.datanode.dir`
   - `hdds.datanode.container.db.dir`
   - Any other volume roots used by datanode

If mapping is ambiguous or not found:

- Emit the signal as an alert only.
- Do **not** mark any volume failed automatically.

# Interaction With Volume Scanner / Scrubber

Once a volume is marked failed, the existing volume failure handling should take effect:

- `MutableVolumeSet#failVolume` removes the volume from the active set.
- The background volume scanner and container scrubber will stop scheduling work on that volume.
- The datanode reports the failed volume in its storage reports.
- SCM’s replication logic proceeds based on reduced capacity / failed volume signals.

This feature does **not** replace scheduled scrubbing; it acts as an early warning input into the same failure path.

# Action on Bad Disk

When a kernel signal decisively indicates disk failure and the device maps to a specific Ozone volume:

- **Set volume state to failed** via the existing volume failure path (`failVolume`), rather than removing it from config or deleting data.
- **Log at ERROR** in the datanode log with the triggering kernel message and the mapped volume path.
- **Propagate to SCM implicitly** through the normal datanode volume report mechanism. No direct SCM RPC is required; SCM will learn about the failed volume via standard reports and replication flows will react accordingly.

If the device cannot be mapped unambiguously to an Ozone volume, the monitor should emit an alert event and **not** fail any volume automatically.

# Event Output

Structured event emitted by the monitor:

```json
{
  "type": "ozone.disk.kernel_signal",
  "timestamp": "2026-01-25T15:04:05Z",
  "host": "node-12",
  "severity": "error",
  "subsystem": "scsi",
  "device": "sda",
  "pattern_id": "scsi_io_error",
  "message": "sd 0:0:0:0: [sda] I/O error, dev sda, sector 123456",
  "raw": "sd 0:0:0:0: [sda] I/O error, dev sda, sector 123456",
  "count": 1
}
```

# Failure Modes & Safety

- **No permission**: disable monitor and log one warning with remediation steps.
- **Unsupported platform**: disable monitor gracefully.
- **High noise**: apply rate limiting and duplicate suppression per device.
- **False positive risk**: keep decision rules conservative and allow operator override.

# Testing Plan

- Unit tests for pattern matching and device extraction.
- Integration tests with injected log lines.
- Manual validation with loopback device and fault injection.

## Suggested test approaches

- **Unit tests**: feed synthetic kmsg/journald lines into the parser and assert:
  - correct pattern classification and severity,
  - device extraction,
  - decision thresholds (N within T),
  - deduplication behavior.
- **Integration tests**: use a fake/pipe reader that emits kernel log lines and verify the datanode:
  - logs ERROR on decisive failures,
  - marks the volume failed (via `failVolume`),
  - keeps volume active when mapping is ambiguous.
- **Manual/chaos tests** (non-prod):
  - inject I/O errors using loopback + device-mapper error targets,
  - simulate timeouts/resets where possible,
  - verify the failed volume appears in datanode volume reports and SCM reacts.

# Rollout Plan

1. Ship disabled by default.
2. Document permissions and recommended config.
3. Enable in staging with CAP_SYSLOG.
4. Enable in production with tuned rules.

# Open Questions

- Should warning thresholds be configurable per device type (NVMe vs SATA)?
- Should a recovery mechanism exist to reinstate a volume after operator verification?
- Should journald be first-class supported on systemd-only deployments?

# Udev Exploration (Supplemental Signal, Not Implemented)

Udev emits device lifecycle events (add/remove/change) via netlink. It can provide fast notification of device presence changes, but it does **not** carry the rich error context found in kernel logs (I/O errors, retries, sense data, filesystem remounts).

Key points:

- **Fidelity**: Udev events are coarse; they cannot replace dmesg for early error detection.
- **Permissions**: Subscribing to udev events typically requires elevated capabilities (often root or `CAP_NET_ADMIN`/`CAP_SYS_ADMIN`, depending on distro and kernel policy).
- **Best use**: Treat udev as a supplemental signal for device removal or path change, not as the primary error detector.

This design does **not** implement udev monitoring; it is noted for potential future exploration.
