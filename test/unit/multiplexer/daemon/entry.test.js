// Copyright 2024 The Lynx Authors. All rights reserved.
// Licensed under the Apache License Version 2.0 that can be found in the
// LICENSE file in the root directory of this source tree.

const assert = require("assert");
const fs = require("fs");
const http = require("http");
const os = require("os");
const path = require("path");

require("../register_ts");

const {
  createMultiplexerDaemon,
  parseEntryOption,
  startMultiplexerDaemonEntry,
} = require("../../../../debug_router_connector/src/multiplexer/daemon/entry");
const {
  defaultLogger,
} = require("../../../../debug_router_connector/src/utils/logger");

function createTempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "debug-router-mux-entry-"));
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function readHealth(port) {
  return new Promise((resolve, reject) => {
    const request = http.get(
      {
        host: "127.0.0.1",
        port,
        path: "/health",
      },
      (response) => {
        let body = "";
        response.setEncoding("utf8");
        response.on("data", (chunk) => {
          body += chunk;
        });
        response.on("end", () => {
          try {
            resolve({
              statusCode: response.statusCode,
              body: JSON.parse(body),
            });
          } catch (error) {
            reject(error);
          }
        });
      }
    );
    request.on("error", reject);
  });
}

function stubProcessOnce() {
  const originalOnce = process.once;
  const registrations = [];

  process.once = (event, handler) => {
    registrations.push({ event, handler });
    return process;
  };

  return {
    registrations,
    restore() {
      process.once = originalOnce;
    },
  };
}

describe("multiplexer daemon entry", function () {
  let tempDir;

  beforeEach(function () {
    tempDir = createTempDir();
    defaultLogger.setOutput(() => {});
  });

  afterEach(function () {
    fs.rmSync(tempDir, { recursive: true, force: true });
  });

  it("parses hyphenated options, defaults, and capabilities", function () {
    assert.deepStrictEqual(
      parseEntryOption([
        "--discovery-path",
        "/tmp/daemon.json",
        "--daemon-lock-path",
        "/tmp/daemon.lock",
        "--capabilities",
        "daemon, discovery ,, heartbeat",
      ]),
      {
        discoveryPath: "/tmp/daemon.json",
        daemonLockPath: "/tmp/daemon.lock",
        protocolVersion: 1,
        minSupportedProtocolVersion: 1,
        controlPort: 0,
        heartbeatInterval: 1000,
        daemonVersion: undefined,
        capabilities: ["daemon", "discovery", "heartbeat"],
      }
    );
  });

  it("parses camelCase options and equals-style values", function () {
    assert.deepStrictEqual(
      parseEntryOption([
        "--discoveryPath=/tmp/daemon.json",
        "--daemonLockPath=/tmp/daemon.lock",
        "--protocolVersion=2",
        "--minSupportedProtocolVersion=2",
        "--controlPort=9222",
        "--heartbeatInterval=200",
        "--daemonVersion=1.2.3",
      ]),
      {
        discoveryPath: "/tmp/daemon.json",
        daemonLockPath: "/tmp/daemon.lock",
        protocolVersion: 2,
        minSupportedProtocolVersion: 2,
        controlPort: 9222,
        heartbeatInterval: 200,
        daemonVersion: "1.2.3",
        capabilities: undefined,
      }
    );
  });

  it("treats blank optional strings as omitted and trims empty capabilities", function () {
    assert.deepStrictEqual(
      parseEntryOption([
        "--discovery-path=/tmp/daemon.json",
        "--daemon-lock-path=/tmp/daemon.lock",
        "--daemon-version=",
        "--capabilities= ,, ",
      ]),
      {
        discoveryPath: "/tmp/daemon.json",
        daemonLockPath: "/tmp/daemon.lock",
        protocolVersion: 1,
        minSupportedProtocolVersion: 1,
        controlPort: 0,
        heartbeatInterval: 1000,
        daemonVersion: undefined,
        capabilities: [],
      }
    );
    assert.deepStrictEqual(
      parseEntryOption([
        "--discovery-path=/tmp/daemon.json",
        "--daemon-lock-path=/tmp/daemon.lock",
        "--capabilities",
      ]).capabilities,
      undefined
    );
  });

  it("rejects missing, unknown, positional, and invalid numeric options", function () {
    assert.throws(
      () => parseEntryOption(["--discovery-path", "/tmp/daemon.json"]),
      /Missing required multiplexer daemon option: daemonLockPath/
    );
    assert.throws(
      () => parseEntryOption(["--daemon-lock-path", "/tmp/daemon.lock"]),
      /Missing required multiplexer daemon option: discoveryPath/
    );
    assert.throws(
      () =>
        parseEntryOption([
          "--discovery-path",
          "/tmp/daemon.json",
          "--daemon-lock-path",
          "/tmp/daemon.lock",
          "--unknown",
          "x",
        ]),
      /Unknown multiplexer daemon option: unknown/
    );
    assert.throws(() => parseEntryOption(["positional"]), /Unexpected/);
    assert.throws(
      () =>
        parseEntryOption([
          "--discovery-path",
          "/tmp/daemon.json",
          "--daemon-lock-path",
          "/tmp/daemon.lock",
          "--control-port",
        ]),
      /Invalid multiplexer daemon option controlPort/
    );
    assert.throws(
      () =>
        parseEntryOption([
          "--discovery-path",
          "/tmp/daemon.json",
          "--daemon-lock-path",
          "/tmp/daemon.lock",
          "--heartbeat-interval",
          "NaN",
        ]),
      /Invalid multiplexer daemon option heartbeatInterval/
    );
  });

  it("creates a host-backed daemon with parsed discovery fields", function () {
    const discoveryPath = path.join(tempDir, "daemon.json");
    const daemonLockPath = path.join(tempDir, "daemon.lock");
    const daemon = createMultiplexerDaemon({
      discoveryPath,
      daemonLockPath,
      protocolVersion: 3,
      minSupportedProtocolVersion: 2,
      controlPort: 9333,
      heartbeatInterval: 100000,
      daemonVersion: "0.0.3",
      capabilities: ["daemon", "control"],
    });

    const info = daemon.createDiscoveryInfo();
    assert.strictEqual(info.controlPort, 9333);
    assert.strictEqual(info.protocolVersion, 3);
    assert.strictEqual(info.minSupportedProtocolVersion, 2);
    assert.strictEqual(info.daemonVersion, "0.0.3");
    assert.deepStrictEqual(info.capabilities, ["daemon", "control"]);
    assert.strictEqual(fs.existsSync(discoveryPath), false);
  });

  it("starts a daemon entry, writes discovery with health, and registers cleanup handlers", async function () {
    const processStub = stubProcessOnce();
    const discoveryPath = path.join(tempDir, "daemon.json");
    const daemonLockPath = path.join(tempDir, "daemon.lock");
    let daemon;

    try {
      daemon = await startMultiplexerDaemonEntry([
        "--discovery-path",
        discoveryPath,
        "--daemon-lock-path",
        daemonLockPath,
        "--control-port",
        "0",
        "--heartbeat-interval",
        "100000",
        "--daemon-version",
        "0.0.1",
        "--capabilities",
        "daemon,control",
      ]);

      const discovery = readJson(discoveryPath);
      assert.strictEqual(discovery.pid, process.pid);
      assert.strictEqual(discovery.protocolVersion, 1);
      assert.strictEqual(discovery.minSupportedProtocolVersion, 1);
      assert.strictEqual(Number.isInteger(discovery.controlPort), true);
      assert.notStrictEqual(discovery.controlPort, 0);
      assert.strictEqual(discovery.daemonVersion, "0.0.1");
      assert.deepStrictEqual(discovery.capabilities, ["daemon", "control"]);

      const health = await readHealth(discovery.controlPort);
      assert.strictEqual(health.statusCode, 200);
      assert.strictEqual(health.body.ok, true);
      assert.strictEqual(health.body.pid, process.pid);
      assert.strictEqual(health.body.protocolVersion, 1);
      assert.strictEqual(health.body.minSupportedProtocolVersion, 1);
      assert.strictEqual(health.body.daemonVersion, "0.0.1");
      assert.deepStrictEqual(health.body.capabilities, ["daemon", "control"]);
      assert.deepStrictEqual(
        processStub.registrations.map((registration) => registration.event),
        [
          "beforeExit",
          "SIGINT",
          "SIGTERM",
          "uncaughtException",
          "unhandledRejection",
        ]
      );
    } finally {
      processStub.restore();
      if (daemon) {
        await daemon.stop();
      }
    }

    assert.strictEqual(fs.existsSync(discoveryPath), false);
    assert.strictEqual(fs.existsSync(daemonLockPath), false);
  });
});
