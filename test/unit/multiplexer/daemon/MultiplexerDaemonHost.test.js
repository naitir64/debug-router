// Copyright 2026 The Lynx Authors. All rights reserved.
// Licensed under the Apache License Version 2.0 that can be found in the
// LICENSE file in the root directory of this source tree.

const assert = require("assert");
const fs = require("fs");
const os = require("os");
const path = require("path");

const {
  MultiplexerDaemonHost,
} = require("../../../../debug_router_connector/dist/cjs/src/multiplexer/daemon/MultiplexerDaemonHost");

function createDevice(overrides = {}) {
  return {
    info: {
      os: overrides.os ?? "Android",
      title: overrides.title ?? "Pixel",
      serial: overrides.serial ?? "device-1",
    },
    ports: overrides.ports ?? [8901, 8902],
    get serial() {
      return this.info.serial;
    },
    getHost() {
      if (overrides.hostError) throw overrides.hostError;
      return overrides.host ?? "127.0.0.1";
    },
    startWatchCalls: 0,
    stopWatchCalls: 0,
    disconnectCalls: 0,
    startWatchClient() {
      this.startWatchCalls++;
    },
    async stopWatchClient() {
      this.stopWatchCalls++;
    },
    disConnect() {
      this.disconnectCalls++;
    },
  };
}

function createClient(overrides = {}) {
  const state = { rawCalls: [], messageCalls: [], closeCalls: 0 };
  const client = {
    state,
    info: {
      port: overrides.port ?? 9001,
      id: overrides.id ?? 7,
      query: {
        app: overrides.app ?? "demo",
        os: overrides.os ?? "Android",
        device: overrides.device ?? "Pixel",
        device_model: overrides.deviceModel ?? "Pixel",
        device_id: overrides.deviceId ?? "device-1",
        ...(overrides.sdkVersion === undefined
          ? {}
          : { sdk_version: overrides.sdkVersion }),
        ...(overrides.rawInfo === undefined
          ? {}
          : { raw_info: overrides.rawInfo }),
      },
    },
    clientId() {
      return this.info.id;
    },
    deviceId() {
      return this.info.query.device_id;
    },
    async sendRawMessage(message) {
      state.rawCalls.push(message);
      return { event: "Customized", data: { data: { message: "ok" } } };
    },
    sendMessage(message) {
      state.messageCalls.push(message);
    },
    close() {
      state.closeCalls++;
    },
  };
  return client;
}

class FakePhysicalConnector {
  constructor(devices = [], clients = []) {
    this.devices = new Map(devices.map((device) => [device.serial, device]));
    this.usbClients = new Map(
      clients.map((client) => [client.clientId(), client])
    );
    this.listeners = new Map();
    this.connectDevicesCalls = 0;
    this.startWatchCalls = [];
    this.closeCalls = 0;
    this.disableAllClientsCalls = 0;
    this.nextClientId = 100;
  }

  on(event, listener) {
    const listeners = this.listeners.get(event) ?? new Set();
    listeners.add(listener);
    this.listeners.set(event, listeners);
  }

  off(event, listener) {
    this.listeners.get(event)?.delete(listener);
  }

  emit(event, payload) {
    for (const listener of this.listeners.get(event) ?? []) listener(payload);
  }

  async connectDevices(timeout, serial) {
    this.connectDevicesCalls++;
    return this.getDevices(timeout, serial);
  }

  async getDevices(_timeout, serial) {
    const devices = Array.from(this.devices.values());
    return serial == null
      ? devices
      : devices.filter((device) => device.serial === serial);
  }

  getAllUsbClients() {
    return Array.from(this.usbClients.values());
  }

  async getDeviceUsbClients(deviceId, _timeout, clientName) {
    return this.getAllUsbClients().filter(
      (client) =>
        client.deviceId() === deviceId &&
        (clientName == null || client.info.query.app === clientName)
    );
  }

  async waitDeviceUsbClients(deviceId) {
    return this.getAllUsbClients().filter(
      (client) => client.deviceId() === deviceId
    );
  }

  async startWatchClient(device, shouldStart) {
    this.startWatchCalls.push(device.serial);
    if (shouldStart()) device.startWatchClient();
  }

  createClientId() {
    return ++this.nextClientId;
  }

  closeClient(id) {
    this.usbClients.get(id)?.close();
  }

  disableAllClients() {
    this.disableAllClientsCalls++;
  }

  async close() {
    this.closeCalls++;
  }
}

function rpc(id, method, params) {
  return { kind: "rpc", id, method, params };
}

describe("MultiplexerDaemonHost mirror stage", function () {
  let tempDir;
  let physical;
  let device;
  let client;
  let host;

  beforeEach(function () {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "debug-router-host-"));
    device = createDevice();
    client = createClient({ rawInfo: { App: "Demo" } });
    physical = new FakePhysicalConnector([device], [client]);
    host = new MultiplexerDaemonHost({
      controlEndpoint: path.join(tempDir, "control.sock"),
      protocolVersion: 1,
      multiplexerDaemonIdleTimeout: -1,
      legacyDriverDir: path.join(tempDir, "legacy"),
      physicalConnector: physical,
      now: () => 1234,
    });
  });

  afterEach(async function () {
    await host.stop();
    fs.rmSync(tempDir, { recursive: true, force: true });
  });

  it("does not expose physical devices before legacy ownership is attached", async function () {
    assert.deepStrictEqual(host.createSnapshot().devices, []);
    assert.deepStrictEqual(await host.getDevices(), []);
  });

  it("discovers devices while unattached without reacquiring ownership", async function () {
    host.legacyOwnershipGuard.currentStatus = "unattached";
    const snapshots = await host.handleControlRpc(
      1,
      rpc(1, "connectDevices", {
        timeout: 20,
        serial: "device-1",
        isAutoListenClients: true,
      })
    );

    assert.strictEqual(physical.connectDevicesCalls, 1);
    assert.strictEqual(host.legacyOwnershipGuard.currentStatus, "unattached");
    assert.deepStrictEqual(physical.startWatchCalls, []);
    assert.deepStrictEqual(snapshots, [
      {
        os: "Android",
        title: "Pixel",
        serial: "device-1",
        ports: [8901, 8902],
        host: "127.0.0.1",
      },
    ]);
    const newDevice = createDevice({ serial: "device-2" });
    physical.devices.set(newDevice.serial, newDevice);
    host.handleDeviceConnected(newDevice);
    assert.deepStrictEqual(physical.startWatchCalls, []);
    assert.strictEqual(host.getDevices instanceof Function, true);
  });

  it("reacquires ownership and starts existing watchers explicitly", async function () {
    await host.handleControlRpc(
      1,
      rpc(1, "startAllDeviceClientWatchers", {})
    );

    assert.strictEqual(host.legacyOwnershipGuard.currentStatus, "attached");
    assert.strictEqual(physical.connectDevicesCalls, 0);
    assert.deepStrictEqual(physical.startWatchCalls, ["device-1"]);
  });

  it("automatically watches newly connected devices by default", async function () {
    host.legacyOwnershipGuard.reacquire();
    await host.handleControlRpc(
      1,
      rpc(1, "connectDevices", { isAutoListenClients: false })
    );
    const newDevice = createDevice({ serial: "device-2" });
    physical.devices.set(newDevice.serial, newDevice);

    host.handleDeviceConnected(newDevice);
    await new Promise((resolve) => setImmediate(resolve));

    assert.deepStrictEqual(physical.startWatchCalls, ["device-2"]);
  });

  it("does not automatically watch devices in manual connect mode", async function () {
    host = new MultiplexerDaemonHost({
      controlEndpoint: path.join(tempDir, "control.sock"),
      protocolVersion: 1,
      multiplexerDaemonIdleTimeout: -1,
      legacyDriverDir: path.join(tempDir, "legacy"),
      physicalConnector: physical,
      physicalConnectorOption: { manualConnect: true },
      now: () => 1234,
    });
    host.legacyOwnershipGuard.reacquire();
    await host.handleControlRpc(
      1,
      rpc(1, "connectDevices", { isAutoListenClients: true })
    );
    const newDevice = createDevice({ serial: "device-2" });
    physical.devices.set(newDevice.serial, newDevice);

    host.handleDeviceConnected(newDevice);
    await new Promise((resolve) => setImmediate(resolve));

    assert.deepStrictEqual(physical.startWatchCalls, []);
  });

  it("connects USB clients and serializes authoritative snapshots", async function () {
    host.legacyOwnershipGuard.reacquire();
    await host.handleControlRpc(
      1,
      rpc(1, "connectDevices", { isAutoListenClients: false })
    );
    const snapshots = await host.handleControlRpc(
      1,
      rpc(2, "connectUsbClients", {
        deviceId: "device-1",
        timeout: 10,
        waitTimeout: true,
        clientName: "demo",
      })
    );

    assert.strictEqual(physical.connectDevicesCalls, 1);
    assert.deepStrictEqual(snapshots, [
      {
        port: 9001,
        id: 7,
        query: {
          app: "demo",
          os: "Android",
          device: "Pixel",
          device_model: "Pixel",
          device_id: "device-1",
          raw_info: { App: "Demo" },
        },
      },
    ]);
    assert.deepStrictEqual(host.createSnapshot(), {
      protocolVersion: 1,
      generatedAt: 1234,
      devices: [
        {
          os: "Android",
          title: "Pixel",
          serial: "device-1",
          ports: [8901, 8902],
          host: "127.0.0.1",
        },
      ],
      clients: snapshots,
    });
  });

  it("returns no USB clients when the device does not exist", async function () {
    host.legacyOwnershipGuard.currentStatus = "unattached";

    const snapshots = await host.handleControlRpc(
      1,
      rpc(1, "connectUsbClients", {
        deviceId: "missing-device",
        timeout: 10,
        waitTimeout: true,
        clientName: null,
      })
    );

    await host.handleControlRpc(
      1,
      rpc(2, "startDeviceClientWatcher", { deviceId: "missing-device" })
    );

    assert.deepStrictEqual(snapshots, []);
    assert.strictEqual(physical.connectDevicesCalls, 0);
    assert.deepStrictEqual(physical.startWatchCalls, []);
  });

  it("forwards watcher, close, and disconnect RPCs", async function () {
    host.legacyOwnershipGuard.reacquire();
    await host.handleControlRpc(
      1,
      rpc(1, "connectDevices", { isAutoListenClients: false })
    );
    await host.handleControlRpc(
      1,
      rpc(2, "startDeviceClientWatcher", { deviceId: "device-1" })
    );
    await host.handleControlRpc(
      1,
      rpc(3, "stopDeviceClientWatcher", { deviceId: "device-1" })
    );
    await host.handleControlRpc(1, rpc(4, "closeClient", { clientId: 7 }));
    await host.handleControlRpc(
      1,
      rpc(5, "disconnectDevice", { deviceId: "device-1" })
    );

    assert.strictEqual(physical.connectDevicesCalls, 1);
    assert.strictEqual(client.state.closeCalls, 1);
    assert.strictEqual(device.stopWatchCalls, 1);
    assert.strictEqual(device.disconnectCalls, 1);
  });

  it("leaves later routing RPCs unimplemented in this stage", async function () {
    assert.strictEqual(
      await host.handleControlRpc(1, rpc(1, "startWSServer", {})),
      undefined
    );
    assert.strictEqual(
      await host.handleControlRpc(
        1,
        rpc(2, "sendMessageWithReply", {
          clientId: 7,
          message: { event: "Customized", data: { data: { message: {} } } },
        })
      ),
      undefined
    );
    assert.strictEqual(
      await host.handleControlRpc(
        1,
        rpc(3, "sendMessageWithoutReply", {
          target: "web",
          clientId: 8,
          message: "message",
        })
      ),
      undefined
    );
  });

  it("publishes snapshots through current control events", async function () {
    const events = [];
    host.broadcast = (event) => events.push(event);
    host.legacyOwnershipGuard.reacquire();
    await host.handleControlRpc(
      1,
      rpc(1, "connectDevices", { isAutoListenClients: false })
    );
    host.handleClientConnected(client);

    assert.strictEqual(events[0].event, "legacy-ownership-changed");
    assert.strictEqual(events[1].event, "snapshot");
  });

  it("clears physical mirrors when legacy ownership is lost", async function () {
    const traceNodes = [];
    host = new MultiplexerDaemonHost({
      controlEndpoint: path.join(tempDir, "control.sock"),
      protocolVersion: 1,
      multiplexerDaemonIdleTimeout: -1,
      legacyDriverDir: path.join(tempDir, "legacy"),
      physicalConnector: physical,
      connectionTrace: {
        enabled: true,
        output: {
          write(line) {
            traceNodes.push(JSON.parse(line));
          },
        },
      },
      now: () => 1234,
    });
    await host.handleControlRpc(1, rpc(1, "startAllDeviceClientWatchers", {}));
    host.legacyOwnershipGuard.currentStatus = "unattached";
    host.handleLegacyOwnershipChanged({
      status: "unattached",
      ownerPid: process.pid,
      previousOwnerPid: process.pid + 1,
      reason: "legacy-preempted",
    });

    assert.strictEqual(physical.disableAllClientsCalls, 1);
    assert.strictEqual(physical.usbClients.size, 0);
    assert.deepStrictEqual(host.createSnapshot().devices, []);
    assert.deepStrictEqual(host.createSnapshot().clients, []);
    assert.deepStrictEqual(
      traceNodes.find((node) => node.event === "legacy_ownership_attached")
        .metadata,
      {
        ownerPid: process.pid,
        reason: "reacquire-requested",
      }
    );
    assert.deepStrictEqual(
      traceNodes.find((node) => node.event === "legacy_ownership_lost")
        .metadata,
      {
        ownerPid: process.pid,
        previousOwnerPid: process.pid + 1,
        reason: "legacy-preempted",
      }
    );
  });

  it("tracks active controls and dispatches graceful shutdown once", async function () {
    const snapshots = [];
    host.sendToControl = (controlId, event) =>
      snapshots.push({ controlId, event });
    host.handleControlConnected(3);
    assert.strictEqual(host.isInUse(), true);
    assert.strictEqual(snapshots[0].event.event, "snapshot");
    host.handleControlDisconnected(3);
    assert.strictEqual(host.isInUse(), false);

    let shutdownCalls = 0;
    host.setShutdownHandler(() => shutdownCalls++);
    await host.handleControlRpc(
      3,
      rpc(1, "shutdownDaemon", { reason: "test" })
    );
    await host.handleControlRpc(
      3,
      rpc(2, "shutdownDaemon", { reason: "test" })
    );
    await new Promise((resolve) => setImmediate(resolve));
    assert.strictEqual(shutdownCalls, 1);
  });

  it("records daemon, control, and shutdown trace lifecycle", async function () {
    const traceNodes = [];
    host = new MultiplexerDaemonHost({
      controlEndpoint: path.join(tempDir, "control.sock"),
      protocolVersion: 1,
      multiplexerDaemonIdleTimeout: -1,
      legacyDriverDir: path.join(tempDir, "legacy"),
      physicalConnector: physical,
      connectionTrace: {
        enabled: true,
        output: {
          write(line) {
            traceNodes.push(JSON.parse(line));
          },
        },
      },
      now: () => 1234,
    });
    host.setShutdownHandler(() => {});

    await host.start();
    host.handleControlConnected(3);
    host.handleControlDisconnected(3);
    await host.handleControlRpc(
      3,
      rpc(1, "shutdownDaemon", { reason: "test" })
    );
    await new Promise((resolve) => setImmediate(resolve));
    await host.stop();

    assert.deepStrictEqual(
      traceNodes.find((node) => node.event === "daemon_started").metadata,
      {
        pid: process.pid,
        controlEndpoint: path.join(tempDir, "control.sock"),
        protocolVersion: 1,
      }
    );
    assert.deepStrictEqual(
      traceNodes.find((node) => node.event === "control_socket_connected")
        .metadata,
      { controlId: 3, activeControlCount: 1 }
    );
    assert.deepStrictEqual(
      traceNodes.find((node) => node.event === "control_socket_disconnected")
        .metadata,
      { controlId: 3, activeControlCount: 0 }
    );
    assert.deepStrictEqual(
      traceNodes.find((node) => node.event === "daemon_shutdown_requested")
        .metadata,
      { reason: "test" }
    );
    assert.deepStrictEqual(
      traceNodes.find((node) => node.event === "daemon_stopped").metadata,
      { pid: process.pid, reason: "test" }
    );
  });

  it("records idle timeout trace and stop reason", async function () {
    const traceNodes = [];
    host = new MultiplexerDaemonHost({
      controlEndpoint: path.join(tempDir, "control.sock"),
      protocolVersion: 1,
      legacyDriverDir: path.join(tempDir, "legacy"),
      physicalConnector: physical,
      multiplexerDaemonIdleTimeout: 0,
      connectionTrace: {
        enabled: true,
        output: {
          write(line) {
            traceNodes.push(JSON.parse(line));
          },
        },
      },
      now: () => 1234,
    });
    const idleReached = new Promise((resolve) => {
      host.setIdleTimeoutHandler(resolve);
    });

    await host.start();
    await idleReached;
    await host.stop();

    assert.deepStrictEqual(
      traceNodes.find(
        (node) => node.event === "daemon_idle_timeout_reached"
      ).metadata,
      { idleTimeout: 0 }
    );
    assert.deepStrictEqual(
      traceNodes.find((node) => node.event === "daemon_stopped").metadata,
      { pid: process.pid, reason: "idle_timeout" }
    );
  });

  it("starts and stops the fixed endpoint with injected physical resources", async function () {
    await host.start();
    await host.start();
    assert.strictEqual(fs.existsSync(path.join(tempDir, "control.sock")), true);
    await host.stop();
    assert.strictEqual(physical.closeCalls, 1);
    assert.strictEqual(
      fs.existsSync(path.join(tempDir, "control.sock")),
      false
    );
  });
});
