// Copyright 2026 The Lynx Authors. All rights reserved.
// Licensed under the Apache License Version 2.0 that can be found in the
// LICENSE file in the root directory of this source tree.

import { BaseDevice } from "../../device/BaseDevice";
import {
  PhysicalConnector,
  PhysicalConnectorOption,
} from "../../physical/PhysicalConnector";
import {
  ConnectionTraceOptions,
  ConnectionTraceRecorder,
  createConnectionTraceRecorder,
} from "../../trace/ConnectionTraceRecorder";
import { UsbClient } from "../../usb/Client";
import { defaultLogger } from "../../utils/logger";
import {
  ClientDescription,
  DeviceDescription,
  PhysicalConnectorEvent,
} from "../../utils/type";
import {
  ClientSnapshot,
  ControlEvent,
  ControlRpcError,
  ControlRpcParams,
  ControlRpcRequest,
  DeviceSnapshot,
  MultiplexerDebugInfo,
  Snapshot,
} from "../protocol";
import {
  LegacyOwnershipChange,
  LegacyOwnershipGuard,
} from "./LegacyOwnershipGuard";
import { MultiplexerControlServer } from "./MultiplexerControlServer";

export type MultiplexerDaemonHostOption = {
  controlEndpoint: string;
  protocolVersion: number;
  debugInfo?: MultiplexerDebugInfo;
  legacyDriverDir?: string;
  multiplexerDaemonIdleTimeout?: number;
  enableWebSocket?: boolean;
  connectionTrace?: ConnectionTraceOptions;
  websocketOption?: { port?: number; roomId?: string };
  physicalConnectorOption?: PhysicalConnectorOption;

  // Only used for tests or embedding.
  physicalConnector?: PhysicalConnector;
  PhysicalConnectorCtor?: new (
    option?: PhysicalConnectorOption,
  ) => PhysicalConnector;
  now?: () => number;
};

export class MultiplexerDaemonHost {
  private physicalConnector: PhysicalConnector;
  private readonly connectionTraceRecorder: ConnectionTraceRecorder | null;
  private connectionTraceRecorderClosed = false;
  private readonly option: MultiplexerDaemonHostOption;
  private readonly protocolVersion: number;
  private readonly now: () => number;
  private controlServer: MultiplexerControlServer | null = null;
  private deviceDiscoveryStarted = false;
  private deviceDiscoveryStarting: Promise<void> | null = null;
  private deviceDiscoveryAutoListensClients = false;
  private readonly clientDiscoveryStartedDeviceIds = new Set<string>();
  private readonly clientDiscoveryStartingByDeviceId = new Map<
    string,
    Promise<void>
  >();
  private allClientWatchersRequested = false;
  private readonly activeControlIds = new Set<number>();
  private readonly legacyOwnershipGuard: LegacyOwnershipGuard;
  private physicalDiscoveryGeneration = 0;
  private clientWatchGeneration = 0;
  private idleTimer: NodeJS.Timeout | null = null;
  private idleTimeoutHandler: (() => void | Promise<void>) | undefined;
  private shutdownHandler: (() => void | Promise<void>) | undefined;
  private started = false;
  private shutdownRequested = false;

  private get legacyOwnershipAttached(): boolean {
    return this.legacyOwnershipGuard.currentStatus === "attached";
  }

  private readonly handleDeviceConnected = (device: BaseDevice): void => {
    if (!this.legacyOwnershipAttached) return;
    if (
      this.deviceDiscoveryAutoListensClients ||
      this.allClientWatchersRequested
    ) {
      void this.ensureClientDiscovery(device.serial);
    }
    this.publishSnapshot();
  };

  private readonly handleDeviceDisconnected = (device: BaseDevice): void => {
    if (!this.legacyOwnershipAttached) return;
    this.clearClientDiscoveryForDevice(device.serial);
    this.publishSnapshot();
  };

  private readonly handleClientConnected = (client: UsbClient): void => {
    if (!this.legacyOwnershipAttached) return;
    this.connectionTraceRecorder?.recordAppClientConnected(client);
    this.publishSnapshot();
  };

  private readonly handleClientDisconnected = (id: number): void => {
    if (!this.legacyOwnershipAttached) return;
    this.connectionTraceRecorder?.recordAppClientDisconnected(id);
    this.publishSnapshot();
  };

  private readonly handleUsbClientMessage = (
    payload: PhysicalConnectorEvent["usb-client-message"],
  ): void => {
    if (!this.legacyOwnershipAttached) return;
    this.broadcast({
      kind: "event",
      event: "client-message",
      data: { source: "usb-runtime", ...payload },
    });
  };

  private readonly handleLegacyOwnershipChanged = (
    change: LegacyOwnershipChange,
  ): void => {
    if (change.status === "unattached") {
      this.handleLegacyOwnershipLost();
    }
    this.broadcast({
      kind: "event",
      event: "legacy-ownership-changed",
      data: change,
    });
  };

  constructor(option: MultiplexerDaemonHostOption) {
    this.option = option;
    this.protocolVersion = option.protocolVersion;
    this.now = option.now ?? Date.now;
    this.connectionTraceRecorder = createConnectionTraceRecorder(
      option.connectionTrace,
      process.env.DriverConnectionTracePath,
    );
    this.physicalConnector = this.createPhysicalConnector();
    this.legacyOwnershipGuard = new LegacyOwnershipGuard({
      legacyDriverDir: option.legacyDriverDir,
      onStatusChanged: this.handleLegacyOwnershipChanged,
    });
  }

  async start(): Promise<void> {
    if (this.started) return;
    if (!this.option.controlEndpoint) {
      throw new Error("Multiplexer control endpoint is required");
    }

    this.shutdownRequested = false;
    this.bindPhysicalConnectorEvents();
    const controlServer = new MultiplexerControlServer({
      host: this,
      controlEndpoint: this.option.controlEndpoint,
      protocolVersion: this.protocolVersion,
      ...(this.option.debugInfo ? { debugInfo: this.option.debugInfo } : {}),
      now: this.now,
    });
    this.controlServer = controlServer;
    try {
      await controlServer.start();
      this.started = true;
      this.legacyOwnershipGuard.start();
      this.scheduleIdleTimeoutIfNeeded();
    } catch (error) {
      await this.stop();
      throw error;
    }
  }

  async stop(): Promise<void> {
    if (
      !this.started &&
      !this.controlServer &&
      (!this.connectionTraceRecorder || this.connectionTraceRecorderClosed)
    ) {
      return;
    }

    const stopErrors: unknown[] = [];
    this.started = false;
    this.shutdownRequested = false;
    this.clearIdleTimeout();
    this.legacyOwnershipGuard.stop();
    this.activeControlIds.clear();
    this.unbindPhysicalConnectorEvents();
    const controlServer = this.controlServer;
    this.controlServer = null;
    try {
      await controlServer?.stop();
    } catch (error) {
      stopErrors.push(error);
    }
    try {
      await this.physicalConnector.close();
    } catch (error) {
      stopErrors.push(error);
    } finally {
      this.resetDiscoveryState();
    }
    if (this.connectionTraceRecorder && !this.connectionTraceRecorderClosed) {
      try {
        await this.connectionTraceRecorder.close();
        this.connectionTraceRecorderClosed = true;
      } catch (error) {
        stopErrors.push(error);
      }
    }
    if (stopErrors.length > 0) throw createStopError(stopErrors);
  }

  setIdleTimeoutHandler(handler: () => void | Promise<void>): void {
    this.idleTimeoutHandler = handler;
    this.scheduleIdleTimeoutIfNeeded();
  }

  setShutdownHandler(handler: () => void | Promise<void>): void {
    this.shutdownHandler = handler;
  }

  handleControlConnected(controlId: number): void {
    this.activeControlIds.add(controlId);
    this.clearIdleTimeout();
    this.sendToControl(controlId, {
      kind: "event",
      event: "snapshot",
      data: this.createSnapshot(),
    });
  }

  handleControlDisconnected(controlId: number): void {
    this.activeControlIds.delete(controlId);
    this.scheduleIdleTimeoutIfNeeded();
  }

  isInUse(): boolean {
    return this.activeControlIds.size > 0;
  }

  async handleControlRpc(
    _controlId: number,
    message: ControlRpcRequest,
  ): Promise<unknown> {
    switch (message.method) {
      case "connectDevices":
        return this.connectDevices(
          message.params as ControlRpcParams["connectDevices"],
        );
      case "connectUsbClients":
        return this.connectUsbClients(
          message.params as ControlRpcParams["connectUsbClients"],
        );
      case "startDeviceClientWatcher":
        return this.startWatchClient(
          (message.params as ControlRpcParams["startDeviceClientWatcher"])
            .deviceId,
        );
      case "stopDeviceClientWatcher":
        return this.stopWatchClient(
          (message.params as ControlRpcParams["stopDeviceClientWatcher"])
            .deviceId,
        );
      case "startAllDeviceClientWatchers":
        return this.startWatchAllClients();
      case "stopAllDeviceClientWatchers":
        return this.stopWatchAllClients();
      case "disconnectDevice":
        return this.disconnectDevice(
          message.params as ControlRpcParams["disconnectDevice"],
        );
      case "shutdownDaemon":
        this.requestDaemonShutdown();
        return undefined;
      case "startWSServer":
        throw createControlError(
          "websocket-disabled",
          "WebSocket routing is introduced after the host mirror sync stage",
        );
      case "sendMessageWithReply": {
        const params = message.params as ControlRpcParams["sendMessageWithReply"];
        return this.getUsbClient(params.clientId).sendRawMessage(
          params.message,
        );
      }
      case "sendMessageWithoutReply":
        this.sendMessageWithoutReply(
          message.params as ControlRpcParams["sendMessageWithoutReply"],
        );
        return undefined;
      case "closeClient":
        this.physicalConnector.closeClient(
          (message.params as ControlRpcParams["closeClient"]).clientId,
        );
        return undefined;
      default:
        throw createControlError(
          "unknown-control-rpc",
          `Unknown multiplexer control RPC: ${message.method}`,
        );
    }
  }

  broadcast(event: ControlEvent): void {
    this.controlServer?.broadcast(event);
  }

  sendToControl(controlId: number, event: ControlEvent): void {
    this.controlServer?.sendToControl(controlId, event);
  }

  publishSnapshot(snapshot: Snapshot = this.createSnapshot()): void {
    this.broadcast({ kind: "event", event: "snapshot", data: snapshot });
  }

  createSnapshot(): Snapshot {
    const generatedAt = this.now();
    const devices = this.legacyOwnershipAttached
      ? Array.from(this.physicalConnector.devices.values())
      : [];
    const clients = this.legacyOwnershipAttached
      ? this.physicalConnector.getAllUsbClients()
      : [];
    const debugInfo = this.createDebugInfo(generatedAt);
    return {
      protocolVersion: this.protocolVersion,
      generatedAt,
      devices: this.serializeDevices(devices),
      clients: this.serializeClients(clients),
      ...(debugInfo ? { debugInfo } : {}),
    };
  }

  serializeDevices(devices: BaseDevice[]): DeviceSnapshot[] {
    return devices.map((device) => this.serializeDevice(device));
  }

  serializeClients(clients: UsbClient[]): ClientSnapshot[] {
    return clients.map((client) => this.serializeClient(client));
  }

  createClientId(): number {
    return this.physicalConnector.createClientId();
  }

  getAllUsbClients(): UsbClient[] {
    return this.legacyOwnershipAttached
      ? this.physicalConnector.getAllUsbClients()
      : [];
  }

  getDevices(
    timeout: number = -1,
    serial: string | null = null,
  ): Promise<BaseDevice[]> {
    return this.legacyOwnershipAttached
      ? this.physicalConnector.getDevices(timeout, serial)
      : Promise.resolve([]);
  }

  private async connectDevices(
    params: ControlRpcParams["connectDevices"],
  ): Promise<DeviceSnapshot[]> {
    this.legacyOwnershipGuard.reacquire();
    const generation = this.physicalDiscoveryGeneration;
    await this.ensureDeviceDiscovery(
      params.isAutoListenClients ?? true,
      generation,
    );
    this.assertPhysicalDiscoveryCurrent(generation);
    return this.serializeDevices(
      await this.physicalConnector.getDevices(
        params.timeout ?? -1,
        params.serial ?? null,
      ),
    );
  }

  private async connectUsbClients(
    params: ControlRpcParams["connectUsbClients"],
  ): Promise<ClientSnapshot[]> {
    const generation = this.physicalDiscoveryGeneration;
    await this.ensureDeviceDiscovery(false, generation);
    await this.ensureClientDiscovery(params.deviceId, generation);
    this.assertPhysicalDiscoveryCurrent(generation);
    const clients =
      params.waitTimeout ?? true
        ? await this.physicalConnector.getDeviceUsbClients(
            params.deviceId,
            params.timeout ?? -1,
            params.clientName ?? null,
          )
        : await this.physicalConnector.waitDeviceUsbClients(
            params.deviceId,
            params.timeout ?? -1,
          );
    const snapshots = this.serializeClients(clients);
    this.publishSnapshot();
    return snapshots;
  }

  private async startWatchClient(deviceId: string): Promise<void> {
    const generation = this.physicalDiscoveryGeneration;
    await this.ensureDeviceDiscovery(false, generation);
    await this.ensureClientDiscovery(
      deviceId,
      generation,
      this.clientWatchGeneration,
    );
  }

  private async stopWatchClient(deviceId: string): Promise<void> {
    this.clearClientDiscoveryForDevice(deviceId);
    await this.physicalConnector.devices.get(deviceId)?.stopWatchClient();
  }

  private disconnectDevice(params: ControlRpcParams["disconnectDevice"]): void {
    this.clearClientDiscoveryForDevice(params.deviceId);
    this.physicalConnector.devices.get(params.deviceId)?.disConnect();
  }

  private async ensureDeviceDiscovery(
    autoWatch: boolean,
    generation: number,
  ): Promise<void> {
    this.assertPhysicalDiscoveryCurrent(generation);
    if (!this.deviceDiscoveryStarted && !this.deviceDiscoveryStarting) {
      this.deviceDiscoveryStarting = this.physicalConnector
        .connectDevices(-1, null)
        .then(() => {
          if (this.isPhysicalDiscoveryCurrent(generation)) {
            this.deviceDiscoveryStarted = true;
          }
        })
        .finally(() => {
          if (this.isPhysicalDiscoveryCurrent(generation)) {
            this.deviceDiscoveryStarting = null;
          }
        });
    }
    if (this.deviceDiscoveryStarting) await this.deviceDiscoveryStarting;
    this.assertPhysicalDiscoveryCurrent(generation);
    if (autoWatch) await this.ensureAutoClientDiscovery(generation);
  }

  private async ensureAutoClientDiscovery(generation: number): Promise<void> {
    this.assertPhysicalDiscoveryCurrent(generation);
    if (this.option.physicalConnectorOption?.manualConnect) return;
    this.deviceDiscoveryAutoListensClients = true;
    await this.ensureClientDiscoveryForCurrentDevices(generation);
  }

  private async ensureClientDiscovery(
    deviceId: string,
    generation: number = this.physicalDiscoveryGeneration,
    watchGeneration: number = this.clientWatchGeneration,
  ): Promise<void> {
    this.assertPhysicalDiscoveryCurrent(generation);
    if (
      watchGeneration !== this.clientWatchGeneration ||
      this.clientDiscoveryStartedDeviceIds.has(deviceId)
    ) {
      return;
    }
    const existing = this.clientDiscoveryStartingByDeviceId.get(deviceId);
    if (existing) return existing;

    const starting = Promise.resolve()
      .then(async () => {
        this.assertPhysicalDiscoveryCurrent(generation);
        if (watchGeneration !== this.clientWatchGeneration) return;
        const device = this.physicalConnector.devices.get(deviceId);
        if (!device) return;
        await this.physicalConnector.startWatchClient(
          device,
          () =>
            this.isPhysicalDiscoveryCurrent(generation) &&
            watchGeneration === this.clientWatchGeneration,
        );
        if (
          this.isPhysicalDiscoveryCurrent(generation) &&
          watchGeneration === this.clientWatchGeneration
        ) {
          this.clientDiscoveryStartedDeviceIds.add(deviceId);
        }
      })
      .finally(() => {
        if (this.clientDiscoveryStartingByDeviceId.get(deviceId) === starting) {
          this.clientDiscoveryStartingByDeviceId.delete(deviceId);
        }
      });
    this.clientDiscoveryStartingByDeviceId.set(deviceId, starting);
    await starting;
  }

  private async startWatchAllClients(): Promise<void> {
    this.legacyOwnershipGuard.reacquire();
    const generation = this.physicalDiscoveryGeneration;
    const watchGeneration = this.clientWatchGeneration;
    this.assertPhysicalDiscoveryCurrent(generation);
    this.allClientWatchersRequested = true;
    await this.ensureDeviceDiscovery(false, generation);
    if (watchGeneration !== this.clientWatchGeneration) return;
    await this.ensureClientDiscoveryForCurrentDevices(
      generation,
      watchGeneration,
    );
    this.publishSnapshot();
  }

  private async stopWatchAllClients(): Promise<void> {
    this.clientWatchGeneration++;
    this.allClientWatchersRequested = false;
    this.deviceDiscoveryAutoListensClients = false;
    await Promise.allSettled(
      Array.from(this.clientDiscoveryStartingByDeviceId.values()),
    );
    this.clientDiscoveryStartingByDeviceId.clear();
    this.clientDiscoveryStartedDeviceIds.clear();
    await Promise.all(
      Array.from(this.physicalConnector.devices.values(), (device) =>
        device.stopWatchClient(),
      ),
    );
    this.publishSnapshot();
  }

  private sendMessageWithoutReply(
    params: ControlRpcParams["sendMessageWithoutReply"],
  ): void {
    if (params.target === "web") {
      throw createControlError(
        "websocket-disabled",
        "WebSocket routing is introduced after the host mirror sync stage",
      );
    }
    this.getUsbClient(params.clientId).sendMessage(params.message);
  }

  private getUsbClient(clientId: number): UsbClient {
    const client = this.physicalConnector.usbClients.get(clientId);
    if (!client) {
      throw createControlError(
        "multiplexer-client-not-found",
        `Multiplexer USB client was not found: ${clientId}`,
      );
    }
    return client;
  }

  private serializeDevice(device: BaseDevice): DeviceSnapshot {
    const info: DeviceDescription = device.info;
    const snapshot: DeviceSnapshot = {
      os: info.os,
      title: info.title,
      serial: info.serial,
      ports: [...device.ports],
    };
    const host = safeGetDeviceHost(device);
    if (host !== undefined) snapshot.host = host;
    return snapshot;
  }

  private serializeClient(client: UsbClient): ClientSnapshot {
    const info: ClientDescription = client.info;
    const query = {
      app: info.query.app,
      os: info.query.os,
      device: info.query.device,
      device_model: info.query.device_model,
      device_id: info.query.device_id,
      sdk_version: info.query.sdk_version,
      raw_info: cloneJsonValue(info.query.raw_info),
    };
    if (query.sdk_version === undefined) delete query.sdk_version;
    if (query.raw_info === undefined) delete query.raw_info;
    return { port: info.port, id: info.id, query };
  }

  private bindPhysicalConnectorEvents(): void {
    this.physicalConnector.on("device-connected", this.handleDeviceConnected);
    this.physicalConnector.on(
      "device-disconnected",
      this.handleDeviceDisconnected,
    );
    this.physicalConnector.on("client-connected", this.handleClientConnected);
    this.physicalConnector.on(
      "client-disconnected",
      this.handleClientDisconnected,
    );
    this.physicalConnector.on(
      "usb-client-message",
      this.handleUsbClientMessage,
    );
  }

  private unbindPhysicalConnectorEvents(): void {
    this.physicalConnector.off("device-connected", this.handleDeviceConnected);
    this.physicalConnector.off(
      "device-disconnected",
      this.handleDeviceDisconnected,
    );
    this.physicalConnector.off("client-connected", this.handleClientConnected);
    this.physicalConnector.off(
      "client-disconnected",
      this.handleClientDisconnected,
    );
    this.physicalConnector.off(
      "usb-client-message",
      this.handleUsbClientMessage,
    );
  }

  private createPhysicalConnector(): PhysicalConnector {
    if (this.option.physicalConnector && !this.option.PhysicalConnectorCtor) {
      return this.option.physicalConnector;
    }
    const Ctor = this.option.PhysicalConnectorCtor ?? PhysicalConnector;
    return new Ctor({
      ...this.option.physicalConnectorOption,
      traceRecorder: this.connectionTraceRecorder,
    });
  }

  private async ensureClientDiscoveryForCurrentDevices(
    generation: number,
    watchGeneration: number = this.clientWatchGeneration,
  ): Promise<void> {
    this.assertPhysicalDiscoveryCurrent(generation);
    await Promise.all(
      Array.from(this.physicalConnector.devices.keys(), (deviceId) =>
        this.ensureClientDiscovery(deviceId, generation, watchGeneration),
      ),
    );
  }

  private clearClientDiscoveryForDevice(deviceId: string): void {
    this.clientDiscoveryStartedDeviceIds.delete(deviceId);
    this.clientDiscoveryStartingByDeviceId.delete(deviceId);
  }

  private resetDiscoveryState(): void {
    this.resetPhysicalDiscoveryState();
    this.activeControlIds.clear();
    this.clearIdleTimeout();
  }

  private handleLegacyOwnershipLost(): void {
    this.resetPhysicalDiscoveryState();
    this.physicalConnector.disableAllClients();
    this.physicalConnector.usbClients.clear();
    this.publishSnapshot();
  }

  private resetPhysicalDiscoveryState(): void {
    this.physicalDiscoveryGeneration++;
    this.deviceDiscoveryStarted = false;
    this.deviceDiscoveryStarting = null;
    this.deviceDiscoveryAutoListensClients = false;
    this.clientDiscoveryStartedDeviceIds.clear();
    this.clientDiscoveryStartingByDeviceId.clear();
    this.allClientWatchersRequested = false;
  }

  private isPhysicalDiscoveryCurrent(generation: number): boolean {
    return (
      this.legacyOwnershipAttached &&
      this.physicalDiscoveryGeneration === generation
    );
  }

  private assertPhysicalDiscoveryCurrent(generation: number): void {
    if (!this.isPhysicalDiscoveryCurrent(generation)) {
      throw new Error("Multiplexer legacy owner is not attached");
    }
  }

  private createDebugInfo(timestamp: number): MultiplexerDebugInfo | undefined {
    if (!this.option.debugInfo) return undefined;
    return {
      ...this.option.debugInfo,
      protocolVersion: this.protocolVersion,
      processId: process.pid,
      timestamp,
    };
  }

  private requestDaemonShutdown(): void {
    if (!this.shutdownHandler) {
      throw createControlError(
        "daemon-shutdown-unavailable",
        "Multiplexer daemon shutdown handler is not configured",
      );
    }
    if (this.shutdownRequested) return;
    this.shutdownRequested = true;
    this.clearIdleTimeout();
    const handler = this.shutdownHandler;
    setImmediate(() => void handler());
  }

  private scheduleIdleTimeoutIfNeeded(): void {
    if (!this.started || !this.idleTimeoutHandler) return;
    const idleTimeout = this.option.multiplexerDaemonIdleTimeout;
    if (
      idleTimeout === undefined ||
      !Number.isFinite(idleTimeout) ||
      idleTimeout < 0 ||
      this.isInUse()
    ) {
      return;
    }
    this.clearIdleTimeout();
    this.idleTimer = setTimeout(() => {
      this.idleTimer = null;
      if (!this.isInUse()) void this.idleTimeoutHandler?.();
    }, idleTimeout);
  }

  private clearIdleTimeout(): void {
    if (!this.idleTimer) return;
    clearTimeout(this.idleTimer);
    this.idleTimer = null;
  }
}

function createControlError(
  code: string,
  message: string,
  details?: unknown,
): ControlRpcError {
  return { code, message, ...(details === undefined ? {} : { details }) };
}

function createStopError(errors: unknown[]): Error {
  const message = errors
    .map((error) => (error instanceof Error ? error.message : String(error)))
    .join("; ");
  const stopError = new Error(`Failed to stop multiplexer host: ${message}`);
  (stopError as any).errors = errors;
  return stopError;
}

function safeGetDeviceHost(device: BaseDevice): string | undefined {
  try {
    return device.getHost();
  } catch (error: any) {
    defaultLogger.warn(
      `Failed to serialize multiplexer device host: ${error?.message}`,
    );
    return undefined;
  }
}

function cloneJsonValue(value: unknown): unknown {
  if (value === undefined || value === null) return value;
  try {
    return JSON.parse(JSON.stringify(value));
  } catch (_error) {
    return undefined;
  }
}
