// Copyright 2026 The Lynx Authors. All rights reserved.
// Licensed under the Apache License Version 2.0 that can be found in the
// LICENSE file in the root directory of this source tree.

import fs from "fs";
import path from "path";
import { driver_dir, lockDir } from "../../utils/file_lock";
import { defaultLogger } from "../../utils/logger";

const LATEST_DRIVER_PROCESS_FILE = "LatestDriverProcess";
const DEFAULT_LEGACY_OWNERSHIP_MONITOR_INTERVAL = 500;
const LEGACY_LOCK_RELEASE_CHECK_INTERVAL_MS = 10;
const LEGACY_LOCK_RELEASE_MAX_CHECKS = 5;

export type LegacyOwnershipStatus = "attached" | "unattached" | "unInit";

export type LegacyOwnershipReason =
  | "daemon-started"
  | "legacy-preempted"
  | "reacquire-requested"
  | "stale-owner"
  | "invalid-owner";

export type LegacyOwnershipChange = {
  status: Exclude<LegacyOwnershipStatus, "unInit">;
  ownerPid: number;
  previousOwnerPid?: number;
  reason: LegacyOwnershipReason;
};

export type LegacyOwnershipGuardOption = {
  legacyDriverDir?: string;
  monitorInterval?: number;
  onStatusChanged?: (change: LegacyOwnershipChange) => void;
};

/**
 * Daemon-side compatibility bridge for the legacy LatestDriverProcess owner.
 * Only the daemon should use this guard in multiplexer mode; connector
 * processes must stay on the shared control channel instead of competing here.
 */
export class LegacyOwnershipGuard {
  readonly ownerFilePath: string;
  currentStatus: LegacyOwnershipStatus = "unInit";

  private readonly driverDir: string;
  private readonly lockDir: string;
  private readonly monitorInterval: number;
  private readonly onStatusChanged?: (change: LegacyOwnershipChange) => void;
  private monitorTimer?: NodeJS.Timeout;
  private stopped = true;

  constructor(option: LegacyOwnershipGuardOption = {}) {
    this.driverDir = option.legacyDriverDir ?? driver_dir;
    this.lockDir = path.join(this.driverDir, path.basename(lockDir));
    this.monitorInterval =
      option.monitorInterval ?? DEFAULT_LEGACY_OWNERSHIP_MONITOR_INTERVAL;
    this.onStatusChanged = option.onStatusChanged;
    this.ownerFilePath = path.join(this.driverDir, LATEST_DRIVER_PROCESS_FILE);
  }

  async start(): Promise<void> {
    if (process.env.DriverCloseMultiOpen === "true") {
      defaultLogger.warn(
        "Legacy ownership guard disabled by DriverCloseMultiOpen",
      );
      this.currentStatus = "attached";
      return;
    }
    if (this.monitorTimer) {
      return;
    }

    this.stopped = false;
    this.ensureDriverDataDir();
    await this.waitForLegacyLockRelease();
    if (this.stopped) {
      return;
    }
    this.claim("daemon-started");
    this.monitorTimer = setInterval(() => {
      this.monitor();
    }, this.monitorInterval);
  }

  stop(): void {
    this.stopped = true;
    if (!this.monitorTimer) {
      return;
    }

    clearInterval(this.monitorTimer);
    this.monitorTimer = undefined;
  }

  async reacquire(): Promise<boolean> {
    if (process.env.DriverCloseMultiOpen === "true") {
      return true;
    }

    await this.waitForLegacyLockRelease();
    return this.claim("reacquire-requested");
  }

  private monitor(): void {
    if (this.stopped) {
      return;
    }
    this.withLegacyLock(() => {
      const previousOwnerPid = this.readOwnerPid();

      if (previousOwnerPid === process.pid) {
        if (this.currentStatus !== "attached") {
          this.currentStatus = "attached";
          this.emitStatusChanged("attached", "reacquire-requested");
        }
        return;
      }

      if (previousOwnerPid === null) {
        this.writeOwnerPid("invalid-owner");
        return;
      }

      if (!isProcessAlive(previousOwnerPid)) {
        this.writeOwnerPid("stale-owner", previousOwnerPid);
        return;
      }

      if (this.currentStatus === "attached") {
        this.currentStatus = "unattached";
        this.emitStatusChanged(
          "unattached",
          "legacy-preempted",
          previousOwnerPid,
        );
      }
    });
  }

  private claim(reason: LegacyOwnershipReason): boolean {
    return this.withLegacyLock(() => {
      const previousOwnerPid = this.readOwnerPid();
      this.writeOwnerPid(reason, previousOwnerPid ?? undefined);
    });
  }

  private ensureDriverDataDir(): void {
    try {
      fs.mkdirSync(this.driverDir, { recursive: true });
    } catch (error: any) {
      defaultLogger.warn(
        `Failed to prepare legacy driver data dir: ${error?.message}`,
      );
    }
  }

  private async waitForLegacyLockRelease(): Promise<void> {
    for (let check = 0; check < LEGACY_LOCK_RELEASE_MAX_CHECKS; check++) {
      if (!fs.existsSync(this.lockDir)) {
        return;
      }

      await new Promise<void>((resolve) =>
        setTimeout(resolve, LEGACY_LOCK_RELEASE_CHECK_INTERVAL_MS),
      );
    }
  }

  private writeOwnerPid(
    reason: LegacyOwnershipReason,
    previousOwnerPid?: number,
  ): void {
    fs.writeFileSync(this.ownerFilePath, `${process.pid}`, "utf-8");
    this.currentStatus = "attached";
    this.emitStatusChanged("attached", reason, previousOwnerPid);
  }

  private readOwnerPid(): number | null {
    try {
      const rawValue = fs.readFileSync(this.ownerFilePath, "utf-8").trim();
      if (!rawValue) {
        return null;
      }

      const ownerPid = Number(rawValue);
      return Number.isInteger(ownerPid) && ownerPid > 0 ? ownerPid : null;
    } catch (error: any) {
      if (error?.code === "ENOENT") {
        return null;
      }
      defaultLogger.warn(`Failed to read legacy owner pid: ${error?.message}`);
      return null;
    }
  }

  private emitStatusChanged(
    status: Exclude<LegacyOwnershipStatus, "unInit">,
    reason: LegacyOwnershipReason,
    previousOwnerPid?: number,
  ): void {
    this.onStatusChanged?.({
      status,
      ownerPid: process.pid,
      previousOwnerPid,
      reason,
    });
  }

  private withLegacyLock(work: () => void): boolean {
    let acquiredLock = false;
    try {
      fs.mkdirSync(this.lockDir);
      acquiredLock = true;
      work();
      return true;
    } catch (error: any) {
      if (error?.code !== "EEXIST") {
        defaultLogger.warn(`Legacy ownership lock failed: ${error?.message}`);
      }
      return false;
    } finally {
      if (acquiredLock) {
        try {
          fs.rmSync(this.lockDir, { recursive: true, force: true });
        } catch (error: any) {
          defaultLogger.warn(
            `Failed to release legacy ownership lock: ${error?.message}`,
          );
        }
      }
    }
  }
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error: any) {
    return error?.code === "EPERM";
  }
}
