// Copyright 2024 The Lynx Authors. All rights reserved.
// Licensed under the Apache License Version 2.0 that can be found in the
// LICENSE file in the root directory of this source tree.

const assert = require("assert");
const fs = require("fs");
const os = require("os");
const path = require("path");

require("../register_ts");

const {
  MultiplexerDaemonManager,
} = require("../../../../debug_router_connector/src/multiplexer/client/MultiplexerDaemonManager");
const {
  FileLock,
} = require("../../../../debug_router_connector/src/multiplexer/utils/FileLock");

function createTempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "debug-router-mux-manager-"));
}

function createInfo(overrides = {}) {
  return {
    pid: 200,
    protocolVersion: 1,
    controlPort: 0,
    heartbeat: 1000,
    startedAt: 900,
    ...overrides,
  };
}

function usable(info = createInfo()) {
  return {
    status: "usable",
    info,
    compatibility: {
      status: "compatible",
      reason: "same-version",
      daemonProtocolVersion: info.protocolVersion,
      connectorProtocolVersion: 1,
    },
  };
}

function replaceRequired(info = createInfo({ protocolVersion: 0 })) {
  return {
    status: "replace-required",
    info,
    compatibility: {
      status: "replace-required",
      reason: "daemon-older-than-connector",
      daemonProtocolVersion: info.protocolVersion,
      connectorProtocolVersion: 1,
    },
  };
}

function unusable(reason = "missing") {
  return {
    status: "unusable",
    reason,
  };
}

function connectorTooOld(
  info = createInfo({
    protocolVersion: 2,
    minSupportedProtocolVersion: 2,
  })
) {
  return {
    status: "unusable",
    reason: "connector-protocol-too-old",
    info,
    compatibility: {
      status: "incompatible",
      reason: "connector-older-than-daemon-min-supported",
      daemonProtocolVersion: info.protocolVersion,
      daemonMinSupportedProtocolVersion: info.minSupportedProtocolVersion,
      connectorProtocolVersion: 1,
    },
  };
}

function createSequenceDiscovery(discoveryPath, sequence) {
  let index = 0;
  return {
    discoveryPath,
    staleTimeout: 1000,
    validateDiscovery() {
      const value = sequence[Math.min(index, sequence.length - 1)];
      index++;
      return typeof value === "function" ? value() : value;
    },
    calls() {
      return index;
    },
  };
}

function createSpawnRecorder() {
  const calls = [];
  return {
    calls,
    spawn(command, args, options) {
      const call = {
        command,
        args,
        options,
        unrefCalled: false,
      };
      calls.push(call);
      return {
        pid: 300,
        unref() {
          call.unrefCalled = true;
        },
      };
    },
  };
}

function createManager(tempDir, overrides = {}) {
  const discoveryPath = path.join(tempDir, "daemon.json");
  const spawnLockPath = path.join(tempDir, "spawn.lock");
  const daemonLockPath = path.join(tempDir, "daemon.lock");
  const spawnRecorder = overrides.spawnRecorder ?? createSpawnRecorder();
  const sleepCalls = [];
  const manager = new (overrides.ManagerClass ?? MultiplexerDaemonManager)({
    discovery:
      overrides.discovery ??
      createSequenceDiscovery(discoveryPath, [unusable("missing")]),
    spawnLockPath,
    daemonLockPath,
    daemonEntry: "/tmp/multiplexer-entry.js",
    startupTimeout: overrides.startupTimeout ?? 1000,
    staleTimeout: overrides.staleTimeout ?? 1000,
    localProtocolVersion: 1,
    controlPort: overrides.controlPort ?? 0,
    heartbeatInterval: overrides.heartbeatInterval,
    minSupportedProtocolVersion: overrides.minSupportedProtocolVersion,
    daemonVersion: overrides.daemonVersion,
    capabilities: overrides.capabilities,
    readyPollInterval: overrides.readyPollInterval ?? 10,
    replacementTimeout: overrides.replacementTimeout ?? 20,
    spawn: spawnRecorder.spawn,
    kill: overrides.kill ?? (() => {}),
    sleep:
      overrides.sleep ??
      (async (duration) => {
        sleepCalls.push(duration);
      }),
    now: overrides.now,
  });

  return {
    manager,
    discoveryPath,
    spawnLockPath,
    daemonLockPath,
    spawnRecorder,
    sleepCalls,
  };
}

describe("MultiplexerDaemonManager", function () {
  let tempDir;

  beforeEach(function () {
    tempDir = createTempDir();
  });

  afterEach(function () {
    fs.rmSync(tempDir, { recursive: true, force: true });
  });

  it("reuses a usable discovery without acquiring spawn or spawning", async function () {
    const info = createInfo();
    const discovery = createSequenceDiscovery(
      path.join(tempDir, "daemon.json"),
      [usable(info)]
    );
    const { manager, spawnRecorder, spawnLockPath } = createManager(tempDir, {
      discovery,
    });

    assert.deepStrictEqual(await manager.ensureDaemon(), info);
    assert.deepStrictEqual(spawnRecorder.calls, []);
    assert.strictEqual(fs.existsSync(spawnLockPath), false);
  });

  it("spawns a daemon and waits until discovery becomes usable", async function () {
    const readyInfo = createInfo({ pid: 201 });
    const discovery = createSequenceDiscovery(
      path.join(tempDir, "daemon.json"),
      [
        unusable("missing"),
        unusable("missing"),
        unusable("missing"),
        usable(readyInfo),
      ]
    );
    const {
      manager,
      spawnRecorder,
      spawnLockPath,
      daemonLockPath,
    } = createManager(tempDir, {
      discovery,
      heartbeatInterval: 250,
      daemonVersion: "0.0.1",
      capabilities: ["daemon", "manager"],
    });

    assert.deepStrictEqual(await manager.ensureDaemon(), readyInfo);
    assert.strictEqual(spawnRecorder.calls.length, 1);
    assert.strictEqual(spawnRecorder.calls[0].command, process.execPath);
    assert.deepStrictEqual(spawnRecorder.calls[0].args, [
      "/tmp/multiplexer-entry.js",
      "--discovery-path",
      discovery.discoveryPath,
      "--daemon-lock-path",
      daemonLockPath,
      "--protocol-version",
      "1",
      "--min-supported-protocol-version",
      "1",
      "--control-port",
      "0",
      "--heartbeat-interval",
      "250",
      "--daemon-version",
      "0.0.1",
      "--capabilities",
      "daemon,manager",
    ]);
    assert.deepStrictEqual(spawnRecorder.calls[0].options, {
      detached: true,
      stdio: "ignore",
    });
    assert.strictEqual(spawnRecorder.calls[0].unrefCalled, true);
    assert.strictEqual(fs.existsSync(spawnLockPath), false);
  });

  it("waits for an in-flight spawn when spawn lock is held elsewhere", async function () {
    const readyInfo = createInfo({ pid: 202 });
    const discovery = createSequenceDiscovery(
      path.join(tempDir, "daemon.json"),
      [unusable("missing"), unusable("missing"), usable(readyInfo)]
    );
    const { manager, spawnRecorder, spawnLockPath } = createManager(tempDir, {
      discovery,
    });
    const externalLock = new FileLock(spawnLockPath);

    assert.strictEqual(externalLock.acquire(), true);
    try {
      assert.deepStrictEqual(await manager.ensureDaemon(), readyInfo);
      assert.deepStrictEqual(spawnRecorder.calls, []);
    } finally {
      externalLock.release();
    }
  });

  it("keeps a spawn lock that is still inside the stale timeout window", function () {
    const now = 2500;
    const { manager, spawnLockPath } = createManager(tempDir, {
      startupTimeout: 1000,
      replacementTimeout: 500,
      now: () => now,
    });
    fs.mkdirSync(spawnLockPath, { recursive: true });
    fs.writeFileSync(
      path.join(spawnLockPath, "owner.json"),
      JSON.stringify({ pid: 1, createdAt: 0 })
    );

    assert.strictEqual(manager.acquireSpawnLock(), false);
    assert.strictEqual(fs.existsSync(spawnLockPath), true);
  });

  it("cleans an expired spawn lock before acquiring it", function () {
    const now = 2501;
    const { manager, spawnLockPath } = createManager(tempDir, {
      startupTimeout: 1000,
      replacementTimeout: 500,
      now: () => now,
    });
    fs.mkdirSync(spawnLockPath, { recursive: true });
    fs.writeFileSync(
      path.join(spawnLockPath, "owner.json"),
      JSON.stringify({ pid: 1, createdAt: 0 })
    );

    assert.strictEqual(manager.acquireSpawnLock(), true);
    assert.strictEqual(manager.spawnLock.isLocked(), true);

    manager.releaseSpawnLock();
  });

  it("rejects a daemon that no longer supports this connector", async function () {
    const unsupportedInfo = createInfo({
      pid: 290,
      protocolVersion: 2,
      minSupportedProtocolVersion: 2,
    });
    const discovery = createSequenceDiscovery(
      path.join(tempDir, "daemon.json"),
      [connectorTooOld(unsupportedInfo)]
    );
    const { manager, spawnRecorder } = createManager(tempDir, {
      discovery,
    });

    await assert.rejects(
      () => manager.ensureDaemon(),
      /requires debug-router-connector protocol 2 or newer/
    );
    assert.deepStrictEqual(spawnRecorder.calls, []);
  });

  it("replaces an older daemon by forcing stop when yield is unavailable", async function () {
    class NonYieldingManager extends MultiplexerDaemonManager {
      async requestDaemonYield() {
        return false;
      }
    }

    const oldInfo = createInfo({ pid: 300, protocolVersion: 0 });
    const readyInfo = createInfo({ pid: 301 });
    const killCalls = [];
    const discovery = createSequenceDiscovery(
      path.join(tempDir, "daemon.json"),
      [replaceRequired(oldInfo), replaceRequired(oldInfo), usable(readyInfo)]
    );
    const { manager, spawnRecorder } = createManager(tempDir, {
      ManagerClass: NonYieldingManager,
      discovery,
      kill: (pid, signal) => killCalls.push([pid, signal]),
    });

    assert.deepStrictEqual(await manager.ensureDaemon(), readyInfo);
    assert.deepStrictEqual(killCalls, [[300, "SIGKILL"]]);
    assert.strictEqual(spawnRecorder.calls.length, 1);
  });

  it("requestDaemonYield sends SIGTERM and succeeds once discovery changes", async function () {
    const oldInfo = createInfo({ pid: 312 });
    const discovery = createSequenceDiscovery(
      path.join(tempDir, "daemon.json"),
      [unusable("missing")]
    );
    const killCalls = [];
    const { manager } = createManager(tempDir, {
      discovery,
      kill: (pid, signal) => killCalls.push([pid, signal]),
    });

    assert.strictEqual(
      await manager.requestDaemonYield(oldInfo, "stale-daemon"),
      true
    );
    assert.deepStrictEqual(killCalls, [[312, "SIGTERM"]]);
  });

  it("times out while waiting for discovery to become usable", async function () {
    let now = 0;
    const discovery = createSequenceDiscovery(
      path.join(tempDir, "daemon.json"),
      [unusable("missing")]
    );
    const { manager } = createManager(tempDir, {
      discovery,
      startupTimeout: 25,
      readyPollInterval: 10,
      now: () => now,
      sleep: async (duration) => {
        now += duration;
      },
    });

    await assert.rejects(
      () => manager.waitUntilReady(25),
      /Timed out waiting for multiplexer daemon: unusable\/missing/
    );
  });
});
