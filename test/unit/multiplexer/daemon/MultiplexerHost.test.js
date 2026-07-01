// Copyright 2024 The Lynx Authors. All rights reserved.
// Licensed under the Apache License Version 2.0 that can be found in the
// LICENSE file in the root directory of this source tree.

const assert = require("assert");
const { EventEmitter } = require("events");
const path = require("path");
const rewire = require(require.resolve("rewire", {
  paths: [path.join(__dirname, "../../../../debug_router_connector")],
}));

require("../register_ts");

const hostModule = rewire(
  path.join(
    __dirname,
    "../../../../debug_router_connector/src/multiplexer/daemon/MultiplexerHost"
  )
);
const { MultiplexerHost } = hostModule;
const {
  defaultLogger,
} = require("../../../../debug_router_connector/src/utils/logger");

function nextTick() {
  return new Promise((resolve) => setImmediate(resolve));
}

function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((innerResolve, innerReject) => {
    resolve = innerResolve;
    reject = innerReject;
  });

  return {
    promise,
    resolve,
    reject,
  };
}

function createRpcRequest(method, params, extra = {}) {
  return {
    kind: "rpc",
    id: 1,
    method,
    params,
    ...extra,
  };
}

function createDevice(serial, overrides = {}) {
  const state = {
    startWatchCalls: 0,
    stopWatchCalls: 0,
    disconnectCalls: 0,
    getHostCalls: 0,
  };

  return {
    info: {
      os: overrides.os ?? "Android",
      title: overrides.title ?? `Device ${serial}`,
      serial,
    },
    ports: overrides.ports ?? [8901, 8902],
    state,
    get serial() {
      return this.info.serial;
    },
    getHost() {
      state.getHostCalls++;
      if (overrides.throwHost) {
        throw new Error("host unavailable");
      }
      return overrides.host;
    },
    startWatchClient() {
      state.startWatchCalls++;
    },
    async stopWatchClient() {
      state.stopWatchCalls++;
    },
    disConnect() {
      state.disconnectCalls++;
    },
  };
}

function createClient(id, overrides = {}) {
  const state = {
    sendCustomizedCalls: [],
    sendRawCalls: [],
    sendMessageCalls: [],
    closeCalls: 0,
  };
  const deviceId = overrides.deviceId ?? "device-1";

  return {
    info: {
      port: overrides.port ?? 9000 + id,
      id,
      query: {
        app: overrides.app ?? `app-${id}`,
        os: overrides.os ?? "Android",
        device: overrides.device ?? "Pixel",
        device_model: overrides.deviceModel ?? "Pixel",
        device_id: deviceId,
        sdk_version: overrides.sdkVersion,
        raw_info: overrides.rawInfo,
      },
    },
    state,
    clientId() {
      return this.info.id;
    },
    deviceId() {
      return this.info.query.device_id;
    },
    async sendCustomizedMessage(method, params, sessionId, type) {
      state.sendCustomizedCalls.push({
        method,
        params,
        sessionId,
        type,
      });
      return overrides.sendCustomizedResult ?? "customized-result";
    },
    async sendRawMessage(message) {
      state.sendRawCalls.push(message);
      return (
        overrides.sendRawResult ?? {
          event: "Register",
          data: {
            id,
            info: {},
          },
        }
      );
    },
    sendMessage(message) {
      state.sendMessageCalls.push(message);
    },
    close() {
      state.closeCalls++;
    },
  };
}

class FakePhysicalConnector extends EventEmitter {
  constructor(option = {}) {
    super();
    this.option = option;
    this.devices = new Map();
    this.usbClients = new Map();
    this.connectDevicesCalls = [];
    this.getDevicesCalls = [];
    this.getAllUsbClientsCalls = 0;
    this.getDeviceUsbClientsCalls = [];
    this.waitDeviceUsbClientsCalls = [];
    this.startWatchClientCalls = [];
    this.startWatchAllClientsCalls = [];
    this.sendRawMessageCalls = [];
    this.sendMessageCalls = [];
    this.closeClientCalls = [];
    this.closeCalls = 0;
    this.disableAllClientsCalls = 0;
    this.nextClientId = option.nextClientId ?? 1;
    this.createClientIdCalls = 0;
    this.connectDevicesImpl = option.connectDevicesImpl;
    this.getDeviceUsbClientsResult = option.getDeviceUsbClientsResult;
    this.waitDeviceUsbClientsResult = option.waitDeviceUsbClientsResult;
  }

  async connectDevices(
    timeout = -1,
    serial = null,
    isAutoListenClients = true
  ) {
    this.connectDevicesCalls.push({
      timeout,
      serial,
      isAutoListenClients,
    });
    if (this.connectDevicesImpl) {
      return this.connectDevicesImpl(timeout, serial, isAutoListenClients);
    }
    return this.getDevices(timeout, serial);
  }

  async getDevices(timeout = -1, serial = null) {
    this.getDevicesCalls.push({
      timeout,
      serial,
    });
    const devices = Array.from(this.devices.values());
    if (serial === null) {
      return devices;
    }
    return devices.filter((device) => device.serial === serial);
  }

  async getDeviceUsbClients(deviceId, timeout = -1, clientName = null) {
    this.getDeviceUsbClientsCalls.push({
      deviceId,
      timeout,
      clientName,
    });
    if (this.getDeviceUsbClientsResult) {
      return this.getDeviceUsbClientsResult;
    }
    return Array.from(this.usbClients.values()).filter(
      (client) => client.deviceId() === deviceId
    );
  }

  async waitDeviceUsbClients(deviceId, timeout = -1) {
    this.waitDeviceUsbClientsCalls.push({
      deviceId,
      timeout,
    });
    if (this.waitDeviceUsbClientsResult) {
      return this.waitDeviceUsbClientsResult;
    }
    return Array.from(this.usbClients.values()).filter(
      (client) => client.deviceId() === deviceId
    );
  }

  getAllUsbClients() {
    this.getAllUsbClientsCalls++;
    return Array.from(this.usbClients.values());
  }

  createClientId() {
    this.createClientIdCalls++;
    return this.nextClientId++;
  }

  async startWatchClient(device, shouldStart = () => true) {
    if (!shouldStart()) {
      return;
    }
    this.startWatchClientCalls.push(device.serial);
    if (this.option.startWatchClientImpl) {
      await this.option.startWatchClientImpl(device, shouldStart);
      return;
    }
    if (!shouldStart()) {
      return;
    }
    device.startWatchClient();
  }

  startWatchAllClients(force = true) {
    this.startWatchAllClientsCalls.push(force);
  }

  async sendRawMessage(clientId, message) {
    this.sendRawMessageCalls.push({
      clientId,
      message,
    });
    const client = this.usbClients.get(clientId);
    if (!client) {
      throw new Error(`client not found:${clientId}`);
    }
    return client.sendRawMessage(message);
  }

  sendMessage(clientId, message) {
    this.sendMessageCalls.push({
      clientId,
      message,
    });
    this.usbClients.get(clientId)?.sendMessage(message);
  }

  closeClient(clientId) {
    this.closeClientCalls.push(clientId);
    this.usbClients.get(clientId)?.close();
  }

  async close() {
    this.closeCalls++;
  }

  disableAllClients() {
    this.disableAllClientsCalls++;
    this.devices.forEach((device) => device.stopWatchClient());
    this.getAllUsbClients().forEach((client) => client.close());
  }
}

class FakeControlServer {
  static instances = [];
  static startError = null;
  static stopError = null;

  constructor(option = {}) {
    this.option = option;
    this.controlPort = option.controlPort ?? 8899;
    this.startCalls = 0;
    this.stopCalls = 0;
    this.broadcasts = [];
    this.targeted = [];
    FakeControlServer.instances.push(this);
  }

  async start() {
    this.startCalls++;
    if (FakeControlServer.startError) {
      throw FakeControlServer.startError;
    }
  }

  async stop() {
    this.stopCalls++;
    if (FakeControlServer.stopError) {
      throw FakeControlServer.stopError;
    }
  }

  broadcast(event) {
    this.broadcasts.push(event);
  }

  sendToControl(controlId, event) {
    this.targeted.push({
      controlId,
      event,
    });
  }
}

function replaceControlServer() {
  const controlServerImport = hostModule.__get__("MultiplexerControlServer_1");
  const originalControlServer = controlServerImport.MultiplexerControlServer;
  controlServerImport.MultiplexerControlServer = FakeControlServer;

  return () => {
    controlServerImport.MultiplexerControlServer = originalControlServer;
  };
}

class FakeLegacyOwnershipGuard {
  static instances = [];

  constructor(option = {}) {
    this.option = option;
    this.startCalls = 0;
    this.stopCalls = 0;
    this.reacquireCalls = 0;
    FakeLegacyOwnershipGuard.instances.push(this);
  }

  start() {
    this.startCalls++;
    this.emitStatus("attached", "daemon-started");
  }

  stop() {
    this.stopCalls++;
  }

  reacquire() {
    this.reacquireCalls++;
    this.emitStatus("attached", "reacquire-requested");
    return true;
  }

  emitStatus(status, reason, previousOwnerPid) {
    this.option.onStatusChanged?.({
      status,
      ownerPid: 100,
      previousOwnerPid,
      reason,
    });
  }
}

function replaceLegacyOwnershipGuard() {
  const legacyOwnershipImport = hostModule.__get__("LegacyOwnershipGuard_1");
  const originalLegacyOwnershipGuard =
    legacyOwnershipImport.LegacyOwnershipGuard;
  legacyOwnershipImport.LegacyOwnershipGuard = FakeLegacyOwnershipGuard;

  return () => {
    legacyOwnershipImport.LegacyOwnershipGuard = originalLegacyOwnershipGuard;
  };
}

function createHost(options = {}) {
  const physical = options.physical ?? new FakePhysicalConnector(options);
  const host = new MultiplexerHost({
    physicalConnector: physical,
    protocolVersion: options.protocolVersion,
    minSupportedProtocolVersion: options.minSupportedProtocolVersion,
    daemonVersion: options.daemonVersion,
    capabilities: options.capabilities,
    controlPort: options.controlPort,
    manualConnect: options.manualConnect,
    enableWebSocket: options.enableWebSocket,
    now: options.now ?? (() => 1000),
  });
  if (options.legacyOwnershipAttached !== false) {
    host.legacyOwnershipAttached = true;
  }

  return {
    host,
    physical,
  };
}

describe("MultiplexerHost", function () {
  let restoreControlServer;
  let restoreLegacyOwnershipGuard;

  before(function () {
    restoreControlServer = replaceControlServer();
    restoreLegacyOwnershipGuard = replaceLegacyOwnershipGuard();
  });

  after(function () {
    restoreControlServer?.();
    restoreLegacyOwnershipGuard?.();
  });

  beforeEach(function () {
    FakeControlServer.instances = [];
    FakeControlServer.startError = null;
    FakeControlServer.stopError = null;
    FakeLegacyOwnershipGuard.instances = [];
  });

  afterEach(function () {
    defaultLogger.setOutput(() => {});
  });

  it("constructs a physical connector when one is not injected", function () {
    const calls = [];
    class PhysicalConnectorCtor extends FakePhysicalConnector {
      constructor(option) {
        calls.push(option);
        super(option);
      }
    }

    const host = new MultiplexerHost({
      PhysicalConnectorCtor,
      protocolVersion: 3,
      manualConnect: true,
    });
    const snapshot = host.createSnapshot();

    assert.strictEqual(calls.length, 1);
    assert.strictEqual(calls[0].protocolVersion, 3);
    assert.strictEqual(calls[0].manualConnect, true);
    assert.deepStrictEqual(snapshot.devices, []);
    assert.deepStrictEqual(snapshot.clients, []);
  });

  it("starts once, reports the listening port, and stops idempotently", async function () {
    const { host, physical } = createHost();

    assert.strictEqual(host.getControlPort(), 0);
    await host.start();
    const port = host.getControlPort();
    await host.start();

    assert.strictEqual(port, 8899);
    assert.strictEqual(FakeControlServer.instances.length, 1);
    assert.strictEqual(FakeControlServer.instances[0].startCalls, 1);
    assert.strictEqual(physical.listenerCount("device-connected"), 1);
    assert.strictEqual(physical.listenerCount("device-disconnected"), 1);
    assert.strictEqual(physical.listenerCount("client-connected"), 1);
    assert.strictEqual(physical.listenerCount("client-disconnected"), 1);
    assert.strictEqual(physical.listenerCount("usb-client-message"), 1);

    await host.stop();
    await host.stop();

    assert.strictEqual(physical.closeCalls, 1);
    assert.strictEqual(FakeControlServer.instances[0].stopCalls, 1);
    assert.strictEqual(physical.listenerCount("device-connected"), 0);
    assert.strictEqual(physical.listenerCount("device-disconnected"), 0);
    assert.strictEqual(physical.listenerCount("client-connected"), 0);
    assert.strictEqual(physical.listenerCount("client-disconnected"), 0);
    assert.strictEqual(physical.listenerCount("usb-client-message"), 0);
  });

  it("cleans physical listeners when control server start fails", async function () {
    FakeControlServer.startError = new Error("control start failed");
    const { host, physical } = createHost();

    await assert.rejects(() => host.start(), /control start failed/);

    assert.strictEqual(physical.closeCalls, 1);
    assert.strictEqual(physical.listenerCount("device-connected"), 0);
    assert.strictEqual(FakeControlServer.instances[0].stopCalls, 1);
  });

  it("sends the current snapshot to newly connected controls", function () {
    const { host, physical } = createHost({
      protocolVersion: 3,
      minSupportedProtocolVersion: 2,
      daemonVersion: "0.0.3",
      capabilities: ["control"],
    });
    const device = createDevice("device-1", { host: "10.0.0.1" });
    const client = createClient(1, {
      rawInfo: {
        App: "Demo",
      },
    });
    physical.devices.set(device.serial, device);
    physical.usbClients.set(client.clientId(), client);
    host.controlServer = new FakeControlServer();

    host.handleControlConnected(42);

    assert.deepStrictEqual(host.controlServer.targeted, [
      {
        controlId: 42,
        event: {
          kind: "event",
          event: "snapshot",
          data: {
            protocolVersion: 3,
            generatedAt: 1000,
            devices: [
              {
                os: "Android",
                title: "Device device-1",
                serial: "device-1",
                ports: [8901, 8902],
                host: "10.0.0.1",
              },
            ],
            clients: [
              {
                port: 9001,
                id: 1,
                query: {
                  app: "app-1",
                  os: "Android",
                  device: "Pixel",
                  device_model: "Pixel",
                  device_id: "device-1",
                  raw_info: {
                    App: "Demo",
                  },
                },
              },
            ],
            daemonVersion: "0.0.3",
            capabilities: ["control"],
          },
        },
      },
    ]);
  });

  it("connectDevices starts device discovery once and auto-starts client discovery", async function () {
    const { host, physical } = createHost();
    const device = createDevice("device-1");
    const nextDevice = createDevice("device-2");
    physical.devices.set(device.serial, device);
    host.controlServer = new FakeControlServer();
    host.bindPhysicalConnectorEvents();

    const first = await host.handleControlRpc(
      1,
      createRpcRequest("connectDevices", {
        timeout: 10,
        serial: "device-1",
        isAutoListenClients: true,
      })
    );
    const second = await host.handleControlRpc(
      1,
      createRpcRequest("connectDevices", {
        timeout: 20,
        serial: null,
        isAutoListenClients: true,
      })
    );

    physical.devices.set(nextDevice.serial, nextDevice);
    physical.emit("device-connected", nextDevice);
    await nextTick();

    assert.deepStrictEqual(physical.connectDevicesCalls, [
      {
        timeout: -1,
        serial: null,
        isAutoListenClients: false,
      },
    ]);
    assert.deepStrictEqual(physical.getDevicesCalls, [
      {
        timeout: -1,
        serial: null,
      },
      {
        timeout: 10,
        serial: "device-1",
      },
      {
        timeout: 20,
        serial: null,
      },
    ]);
    assert.deepStrictEqual(physical.startWatchClientCalls, [
      "device-1",
      "device-2",
    ]);
    assert.strictEqual(device.state.startWatchCalls, 1);
    assert.strictEqual(nextDevice.state.startWatchCalls, 1);
    assert.deepStrictEqual(
      first.map((item) => item.serial),
      ["device-1"]
    );
    assert.deepStrictEqual(
      second.map((item) => item.serial),
      ["device-1"]
    );
  });

  it("connectDevices skips auto client discovery for manualConnect and explicit false", async function () {
    const manual = createHost({
      manualConnect: true,
    });
    const autoDisabled = createHost();
    const manualDevice = createDevice("manual-device");
    const disabledDevice = createDevice("disabled-device");
    manual.physical.devices.set(manualDevice.serial, manualDevice);
    autoDisabled.physical.devices.set(disabledDevice.serial, disabledDevice);

    await manual.host.handleControlRpc(
      1,
      createRpcRequest("connectDevices", {
        isAutoListenClients: true,
      })
    );
    await autoDisabled.host.handleControlRpc(
      1,
      createRpcRequest("connectDevices", {
        isAutoListenClients: false,
      })
    );

    assert.deepStrictEqual(manual.physical.startWatchClientCalls, []);
    assert.deepStrictEqual(autoDisabled.physical.startWatchClientCalls, []);
  });

  it("shares an in-flight device discovery attempt", async function () {
    const deferred = createDeferred();
    const physical = new FakePhysicalConnector({
      connectDevicesImpl: () => deferred.promise,
    });
    const device = createDevice("device-1");
    physical.devices.set(device.serial, device);
    const { host } = createHost({
      physical,
    });

    const first = host.handleControlRpc(
      1,
      createRpcRequest("connectDevices", {
        isAutoListenClients: false,
      })
    );
    const second = host.handleControlRpc(
      1,
      createRpcRequest("connectDevices", {
        isAutoListenClients: true,
      })
    );
    deferred.resolve([device]);
    await Promise.all([first, second]);

    assert.strictEqual(physical.connectDevicesCalls.length, 1);
  });

  it("getDevices directly serializes the physical query result", async function () {
    const { host, physical } = createHost();
    const firstDevice = createDevice("device-1");
    const secondDevice = createDevice("device-2");
    physical.devices.set(firstDevice.serial, firstDevice);
    physical.devices.set(secondDevice.serial, secondDevice);

    const result = await host.handleControlRpc(
      1,
      createRpcRequest("getDevices", {
        timeout: 30,
        serial: "device-2",
      })
    );

    assert.deepStrictEqual(physical.connectDevicesCalls, []);
    assert.deepStrictEqual(physical.getDevicesCalls, [
      {
        timeout: 30,
        serial: "device-2",
      },
    ]);
    assert.deepStrictEqual(
      result.map((item) => item.serial),
      ["device-2"]
    );
  });

  it("connectUsbClients starts discovery, watches target device once, and uses getDeviceUsbClients by default", async function () {
    const { host, physical } = createHost();
    const device = createDevice("device-1");
    const client = createClient(8, {
      deviceId: "device-1",
      rawInfo: {
        App: "Demo",
      },
    });
    physical.devices.set(device.serial, device);
    physical.usbClients.set(client.clientId(), client);

    const first = await host.handleControlRpc(
      1,
      createRpcRequest("connectUsbClients", {
        deviceId: "device-1",
        timeout: 55,
        clientName: "Demo",
      })
    );
    const second = await host.handleControlRpc(
      1,
      createRpcRequest("connectUsbClients", {
        deviceId: "device-1",
        timeout: 66,
      })
    );

    assert.deepStrictEqual(physical.connectDevicesCalls, [
      {
        timeout: -1,
        serial: null,
        isAutoListenClients: false,
      },
    ]);
    assert.deepStrictEqual(physical.startWatchClientCalls, ["device-1"]);
    assert.deepStrictEqual(physical.getDeviceUsbClientsCalls, [
      {
        deviceId: "device-1",
        timeout: 55,
        clientName: "Demo",
      },
      {
        deviceId: "device-1",
        timeout: 66,
        clientName: null,
      },
    ]);
    assert.deepStrictEqual(physical.waitDeviceUsbClientsCalls, []);
    assert.deepStrictEqual(first[0], {
      port: 9008,
      id: 8,
      query: {
        app: "app-8",
        os: "Android",
        device: "Pixel",
        device_model: "Pixel",
        device_id: "device-1",
        raw_info: {
          App: "Demo",
        },
      },
    });
    assert.deepStrictEqual(
      second.map((item) => item.id),
      [8]
    );
  });

  it("connectUsbClients uses waitDeviceUsbClients when waitTimeout is false", async function () {
    const { host, physical } = createHost();
    const device = createDevice("device-1");
    const client = createClient(9, {
      deviceId: "device-1",
    });
    physical.devices.set(device.serial, device);
    physical.usbClients.set(client.clientId(), client);

    const result = await host.handleControlRpc(
      1,
      createRpcRequest("connectUsbClients", {
        deviceId: "device-1",
        timeout: 77,
        waitTimeout: false,
      })
    );

    assert.deepStrictEqual(physical.getDeviceUsbClientsCalls, []);
    assert.deepStrictEqual(physical.waitDeviceUsbClientsCalls, [
      {
        deviceId: "device-1",
        timeout: 77,
      },
    ]);
    assert.deepStrictEqual(
      result.map((item) => item.id),
      [9]
    );
  });

  it("dispatches send and close RPCs through physical objects", async function () {
    const { host, physical } = createHost();
    const device = createDevice("device-1");
    const client = createClient(1);
    physical.devices.set(device.serial, device);
    physical.usbClients.set(client.clientId(), client);
    const rawMessage = {
      event: "Initialize",
      data: 1,
    };

    const customized = await host.handleControlRpc(
      7,
      createRpcRequest("sendCustomizedMessage", {
        clientId: 1,
        method: "Runtime.evaluate",
        params: { expression: "1 + 1" },
        sessionId: 9,
        type: "CDP",
      })
    );
    const raw = await host.handleControlRpc(
      7,
      createRpcRequest("sendRawMessage", {
        clientId: 1,
        message: rawMessage,
      })
    );
    await host.handleControlRpc(
      7,
      createRpcRequest("sendMessage", {
        clientId: 1,
        message: { event: "Ping" },
      })
    );
    await host.handleControlRpc(
      7,
      createRpcRequest("sendMessageToApp", {
        id: 1,
        message: "hello",
      })
    );
    await host.handleControlRpc(
      7,
      createRpcRequest("closeClient", { clientId: 1 })
    );

    assert.strictEqual(customized, "customized-result");
    assert.deepStrictEqual(raw, {
      event: "Register",
      data: {
        id: 1,
        info: {},
      },
    });
    assert.deepStrictEqual(client.state.sendCustomizedCalls, [
      {
        method: "Runtime.evaluate",
        params: { expression: "1 + 1" },
        sessionId: 9,
        type: "CDP",
      },
    ]);
    assert.deepStrictEqual(physical.sendRawMessageCalls, [
      {
        clientId: 1,
        message: rawMessage,
      },
    ]);
    assert.deepStrictEqual(physical.sendMessageCalls, [
      {
        clientId: 1,
        message: { event: "Ping" },
      },
      {
        clientId: 1,
        message: "hello",
      },
    ]);
    assert.deepStrictEqual(physical.closeClientCalls, [1]);
    assert.strictEqual(client.state.closeCalls, 1);
  });

  it("handles legacy ownership loss by stopping physical state and publishing an empty snapshot", async function () {
    const { host, physical } = createHost({
      now: () => 3000,
    });
    const device = createDevice("device-1");
    const client = createClient(7, {
      deviceId: "device-1",
    });
    host.controlServer = new FakeControlServer();
    host.bindPhysicalConnectorEvents();
    physical.devices.set(device.serial, device);
    physical.usbClients.set(client.clientId(), client);

    host.legacyOwnershipGuard.emitStatus(
      "unattached",
      "legacy-preempted",
      200
    );

    assert.strictEqual(host.legacyOwnershipAttached, false);
    assert.strictEqual(physical.disableAllClientsCalls, 1);
    assert.strictEqual(device.state.stopWatchCalls, 1);
    assert.strictEqual(client.state.closeCalls, 1);
    assert.strictEqual(physical.devices.size, 0);
    assert.strictEqual(physical.usbClients.size, 0);
    assert.deepStrictEqual(await host.getDevices(), []);
    assert.deepStrictEqual(host.getAllUsbClients(), []);
    assert.deepStrictEqual(
      host.controlServer.broadcasts.map((event) => event.event),
      ["snapshot", "legacy-ownership-changed"]
    );
    assert.deepStrictEqual(host.controlServer.broadcasts[0].data, {
      protocolVersion: 1,
      generatedAt: 3000,
      devices: [],
      clients: [],
      daemonVersion: undefined,
      capabilities: undefined,
    });
    assert.deepStrictEqual(host.controlServer.broadcasts[1].data, {
      status: "unattached",
      ownerPid: 100,
      previousOwnerPid: 200,
      reason: "legacy-preempted",
    });

    physical.emit("device-connected", createDevice("late-device"));
    physical.emit("client-connected", createClient(8));
    physical.emit("usb-client-message", {
      id: 8,
      message: "late",
    });
    assert.deepStrictEqual(
      host.controlServer.broadcasts.map((event) => event.event),
      ["snapshot", "legacy-ownership-changed"]
    );
  });

  it("invalidates in-flight device discovery when legacy ownership is lost", async function () {
    const deferred = createDeferred();
    const physical = new FakePhysicalConnector({
      connectDevicesImpl: () => deferred.promise,
    });
    const device = createDevice("device-1");
    physical.devices.set(device.serial, device);
    const { host } = createHost({
      physical,
    });
    host.controlServer = new FakeControlServer();

    const discovery = host.handleControlRpc(
      1,
      createRpcRequest("connectDevices", {
        isAutoListenClients: true,
      })
    );
    await nextTick();

    host.legacyOwnershipGuard.emitStatus(
      "unattached",
      "legacy-preempted",
      200
    );
    deferred.resolve([device]);

    await assert.rejects(discovery, /legacy owner is not attached/);
    assert.strictEqual(host.deviceDiscoveryStarted, false);
    assert.strictEqual(host.deviceDiscoveryStarting, null);
    assert.deepStrictEqual(host.clientDiscoveryStartingByDeviceId.size, 0);
    assert.deepStrictEqual(host.clientDiscoveryStartedDeviceIds.size, 0);
    assert.deepStrictEqual(physical.startWatchClientCalls, []);
  });

  it("invalidates in-flight client watcher startup when legacy ownership is lost", async function () {
    const startDeferred = createDeferred();
    const { host, physical } = createHost({
      startWatchClientImpl: async (device, shouldStart) => {
        await startDeferred.promise;
        if (shouldStart()) {
          device.startWatchClient();
        }
      },
    });
    const device = createDevice("device-1");
    physical.devices.set(device.serial, device);
    host.controlServer = new FakeControlServer();

    const discovery = host.handleControlRpc(
      1,
      createRpcRequest("connectUsbClients", {
        deviceId: "device-1",
      })
    );
    await nextTick();
    assert.deepStrictEqual(physical.startWatchClientCalls, ["device-1"]);

    host.legacyOwnershipGuard.emitStatus(
      "unattached",
      "legacy-preempted",
      200
    );
    startDeferred.resolve();

    await assert.rejects(discovery, /legacy owner is not attached/);
    assert.strictEqual(device.state.startWatchCalls, 0);
    assert.strictEqual(host.clientDiscoveryStartingByDeviceId.size, 0);
    assert.strictEqual(host.clientDiscoveryStartedDeviceIds.size, 0);
    assert.deepStrictEqual(physical.getDeviceUsbClientsCalls, []);
  });

  it("reacquireLegacyOwnership reattaches the host and allows watcher recovery", async function () {
    const { host, physical } = createHost();
    const device = createDevice("device-1");
    host.controlServer = new FakeControlServer();
    physical.devices.set(device.serial, device);

    host.legacyOwnershipGuard.emitStatus(
      "unattached",
      "legacy-preempted",
      200
    );
    await host.handleControlRpc(
      1,
      createRpcRequest("reacquireLegacyOwnership", {})
    );
    physical.devices.set(device.serial, device);
    await host.handleControlRpc(
      1,
      createRpcRequest("startWatchAllClients", {
        force: true,
      })
    );

    assert.strictEqual(host.legacyOwnershipAttached, true);
    assert.strictEqual(host.legacyOwnershipGuard.reacquireCalls, 1);
    assert.deepStrictEqual(
      host.controlServer.broadcasts.map((event) => event.event),
      [
        "snapshot",
        "legacy-ownership-changed",
        "legacy-ownership-changed",
        "snapshot",
      ]
    );
    assert.deepStrictEqual(physical.startWatchClientCalls, ["device-1"]);
    assert.strictEqual(device.state.startWatchCalls, 1);
  });

  it("connectUsbClients waits for an existing client discovery promise without starting a duplicate watcher", async function () {
    const { host, physical } = createHost();
    const deferred = createDeferred();
    const device = createDevice("device-1");
    physical.devices.set(device.serial, device);
    host.clientDiscoveryStartingByDeviceId.set("device-1", deferred.promise);

    const promise = host.handleControlRpc(
      1,
      createRpcRequest("connectUsbClients", {
        deviceId: "device-1",
      })
    );
    await nextTick();
    assert.deepStrictEqual(physical.startWatchClientCalls, []);

    deferred.resolve();
    await promise;

    assert.deepStrictEqual(physical.startWatchClientCalls, []);
    assert.deepStrictEqual(physical.getDeviceUsbClientsCalls, [
      {
        deviceId: "device-1",
        timeout: -1,
        clientName: null,
      },
    ]);
  });

  it("connectUsbClients handles missing devices without starting client discovery", async function () {
    const { host, physical } = createHost();

    const result = await host.handleControlRpc(
      1,
      createRpcRequest("connectUsbClients", {
        deviceId: "missing-device",
      })
    );

    assert.deepStrictEqual(physical.startWatchClientCalls, []);
    assert.deepStrictEqual(physical.getDeviceUsbClientsCalls, [
      {
        deviceId: "missing-device",
        timeout: -1,
        clientName: null,
      },
    ]);
    assert.deepStrictEqual(result, []);
  });

  it("device disconnect clears client discovery state so a later connect can watch again", async function () {
    const { host, physical } = createHost();
    const device = createDevice("device-1");
    physical.devices.set(device.serial, device);
    host.controlServer = new FakeControlServer();
    host.bindPhysicalConnectorEvents();

    await host.handleControlRpc(
      1,
      createRpcRequest("connectUsbClients", {
        deviceId: "device-1",
      })
    );
    physical.emit("device-disconnected", device);
    await host.handleControlRpc(
      1,
      createRpcRequest("connectUsbClients", {
        deviceId: "device-1",
      })
    );

    assert.deepStrictEqual(physical.startWatchClientCalls, [
      "device-1",
      "device-1",
    ]);
  });

  it("startWatchClient starts device discovery and watches each device only once", async function () {
    const { host, physical } = createHost();
    const device = createDevice("device-1");
    physical.devices.set(device.serial, device);

    await host.handleControlRpc(
      1,
      createRpcRequest("startWatchClient", {
        deviceId: "device-1",
      })
    );
    await host.handleControlRpc(
      1,
      createRpcRequest("startWatchClient", {
        deviceId: "device-1",
      })
    );
    await host.handleControlRpc(
      1,
      createRpcRequest("startWatchClient", {
        deviceId: "missing-device",
      })
    );

    assert.deepStrictEqual(physical.connectDevicesCalls, [
      {
        timeout: -1,
        serial: null,
        isAutoListenClients: false,
      },
    ]);
    assert.deepStrictEqual(physical.startWatchClientCalls, ["device-1"]);
    assert.strictEqual(device.state.startWatchCalls, 1);
  });

  it("stopWatchClient stops existing device watchers, ignores missing devices, and allows later restart", async function () {
    const { host, physical } = createHost();
    const device = createDevice("device-1");
    physical.devices.set(device.serial, device);

    await host.handleControlRpc(
      1,
      createRpcRequest("startWatchClient", {
        deviceId: "device-1",
      })
    );
    await host.handleControlRpc(
      1,
      createRpcRequest("stopWatchClient", {
        deviceId: "device-1",
      })
    );
    await host.handleControlRpc(
      1,
      createRpcRequest("stopWatchClient", {
        deviceId: "missing-device",
      })
    );
    await host.handleControlRpc(
      1,
      createRpcRequest("startWatchClient", {
        deviceId: "device-1",
      })
    );

    assert.deepStrictEqual(physical.startWatchClientCalls, [
      "device-1",
      "device-1",
    ]);
    assert.strictEqual(device.state.stopWatchCalls, 1);
  });

  it("disconnectDevice clears watcher state, delegates device disconnect, and ignores missing devices", async function () {
    const { host, physical } = createHost();
    const device = createDevice("device-1");
    physical.devices.set(device.serial, device);

    await host.handleControlRpc(
      1,
      createRpcRequest("startWatchClient", {
        deviceId: "device-1",
      })
    );
    await host.handleControlRpc(
      1,
      createRpcRequest("disconnectDevice", {
        deviceId: "device-1",
      })
    );
    await host.handleControlRpc(
      1,
      createRpcRequest("disconnectDevice", {
        deviceId: "missing-device",
      })
    );
    await host.handleControlRpc(
      1,
      createRpcRequest("startWatchClient", {
        deviceId: "device-1",
      })
    );

    assert.strictEqual(device.state.disconnectCalls, 1);
    assert.deepStrictEqual(physical.startWatchClientCalls, [
      "device-1",
      "device-1",
    ]);
  });

  it("startWatchAllClients watches current and future devices", async function () {
    const { host, physical } = createHost();
    const firstDevice = createDevice("device-1");
    const secondDevice = createDevice("device-2");
    physical.devices.set(firstDevice.serial, firstDevice);
    host.controlServer = new FakeControlServer();
    host.bindPhysicalConnectorEvents();

    await host.handleControlRpc(
      1,
      createRpcRequest("startWatchAllClients", {
        force: false,
      })
    );
    physical.devices.set(secondDevice.serial, secondDevice);
    physical.emit("device-connected", secondDevice);
    await nextTick();

    assert.deepStrictEqual(physical.startWatchClientCalls, [
      "device-1",
      "device-2",
    ]);
    assert.strictEqual(firstDevice.state.startWatchCalls, 1);
    assert.strictEqual(secondDevice.state.startWatchCalls, 1);
  });

  it("broadcasts physical events and snapshot updates", function () {
    const { host, physical } = createHost();
    const controlServer = new FakeControlServer();
    const device = createDevice("device-1");
    const client = createClient(1);
    host.controlServer = controlServer;
    host.bindPhysicalConnectorEvents();

    physical.devices.set(device.serial, device);
    physical.emit("device-connected", device);
    physical.usbClients.set(client.clientId(), client);
    physical.emit("client-connected", client);
    physical.emit("usb-client-message", {
      id: 1,
      message: "runtime-event",
    });
    physical.emit("client-disconnected", 1);
    physical.emit("device-disconnected", device);

    assert.deepStrictEqual(
      controlServer.broadcasts.map((event) => event.event),
      [
        "device-connected",
        "snapshot",
        "client-connected",
        "snapshot",
        "usb-client-message",
        "client-disconnected",
        "snapshot",
        "device-disconnected",
        "snapshot",
      ]
    );
    assert.strictEqual(controlServer.broadcasts[0].data.serial, "device-1");
    assert.strictEqual(controlServer.broadcasts[2].data.id, 1);
    assert.deepStrictEqual(controlServer.broadcasts[4].data, {
      id: 1,
      message: "runtime-event",
    });

    host.unbindPhysicalConnectorEvents();
  });

  it("omits device host when serialization cannot read it", function () {
    const { host } = createHost();
    const device = createDevice("device-1", { throwHost: true });

    assert.deepStrictEqual(host.serializeDevices([device]), [
      {
        os: "Android",
        title: "Device device-1",
        serial: "device-1",
        ports: [8901, 8902],
      },
    ]);
  });

  it("returns no-op WebSocket server info until MR6 wires routing", async function () {
    const { host } = createHost();

    assert.strictEqual(
      await host.handleControlRpc(7, createRpcRequest("startWSServer", {})),
      undefined
    );
    assert.strictEqual(
      await host.handleControlRpc(
        7,
        createRpcRequest("sendMessageToWeb", { message: "hello" })
      ),
      undefined
    );
  });

  it("throws a control error when a target USB client is missing", async function () {
    const { host } = createHost();

    await assert.rejects(
      () =>
        host.handleControlRpc(
          7,
          createRpcRequest("sendCustomizedMessage", {
            clientId: 404,
            method: "Runtime.evaluate",
          })
        ),
      (error) => {
        assert.strictEqual(error.code, "multiplexer-client-not-found");
        assert.match(error.message, /404/);
        return true;
      }
    );
  });
});
