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
        sdk_version: overrides.sdkVersion,
        raw_info: overrides.rawInfo,
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
    this.connectDevicesCalls = [];
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
    this.connectDevicesCalls.push({ timeout, serial });
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

  it("reacquires ownership, discovers devices, and starts current watchers", async function () {
    const snapshots = await host.handleControlRpc(
      1,
      rpc(1, "connectDevices", {
        timeout: 20,
        serial: "device-1",
        isAutoListenClients: true,
      })
    );

    assert.deepStrictEqual(physical.connectDevicesCalls, [
      { timeout: -1, serial: null },
    ]);
    assert.deepStrictEqual(physical.startWatchCalls, ["device-1"]);
    assert.deepStrictEqual(snapshots, [
      {
        os: "Android",
        title: "Pixel",
        serial: "device-1",
        ports: [8901, 8902],
        host: "127.0.0.1",
      },
    ]);
    assert.strictEqual(host.getDevices instanceof Function, true);
  });

  it("connects USB clients and serializes authoritative snapshots", async function () {
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

  it("forwards watcher, message, close, and disconnect RPCs", async function () {
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
    const request = { event: "Customized", data: { data: { message: {} } } };
    const response = await host.handleControlRpc(
      1,
      rpc(4, "sendMessageWithReply", { clientId: 7, message: request })
    );
    await host.handleControlRpc(
      1,
      rpc(5, "sendMessageWithoutReply", {
        target: "app",
        clientId: 7,
        message: "fire-and-forget",
      })
    );
    await host.handleControlRpc(1, rpc(6, "closeClient", { clientId: 7 }));
    await host.handleControlRpc(
      1,
      rpc(7, "disconnectDevice", { deviceId: "device-1" })
    );

    assert.deepStrictEqual(response, {
      event: "Customized",
      data: { data: { message: "ok" } },
    });
    assert.deepStrictEqual(client.state.rawCalls, [request]);
    assert.deepStrictEqual(client.state.messageCalls, ["fire-and-forget"]);
    assert.strictEqual(client.state.closeCalls, 1);
    assert.strictEqual(device.stopWatchCalls, 1);
    assert.strictEqual(device.disconnectCalls, 1);
  });

  it("keeps MR6 WebSocket routing outside this stage", async function () {
    await assert.rejects(
      host.handleControlRpc(1, rpc(1, "startWSServer", {})),
      (error) => error.code === "websocket-disabled"
    );
    await assert.rejects(
      host.handleControlRpc(
        1,
        rpc(2, "sendMessageWithoutReply", {
          target: "web",
          clientId: 8,
          message: "message",
        })
      ),
      (error) => error.code === "websocket-disabled"
    );
  });

  it("publishes snapshots and USB messages through current control events", async function () {
    const events = [];
    host.broadcast = (event) => events.push(event);
    await host.handleControlRpc(
      1,
      rpc(1, "connectDevices", { isAutoListenClients: false })
    );
    host.handleClientConnected(client);
    host.handleUsbClientMessage({ id: 7, message: "runtime-event" });

    assert.strictEqual(events[0].event, "legacy-ownership-changed");
    assert.strictEqual(events[1].event, "snapshot");
    assert.deepStrictEqual(events[2], {
      kind: "event",
      event: "client-message",
      data: {
        source: "usb-runtime",
        id: 7,
        message: "runtime-event",
      },
    });
  });

  it("clears physical mirrors when legacy ownership is lost", async function () {
    await host.handleControlRpc(
      1,
      rpc(1, "connectDevices", { isAutoListenClients: false })
    );
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
