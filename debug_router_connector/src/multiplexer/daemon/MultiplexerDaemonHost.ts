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
  multiplexerDaemonIdleTimeout: number;
  debugInfo?: MultiplexerDebugInfo;
  legacyDriverDir?: string;
  enableWebSocket?: boolean;
  connectionTrace?: ConnectionTraceOptions;
  websocketOption?: { port?: number; roomId?: string };
  physicalConnectorOption?: PhysicalConnectorOption;

  // Only used for tests or embedding.
  physicalConnector?: PhysicalConnector;
  now?: () => number;
};

export class MultiplexerDaemonHost {
  private physicalConnector: PhysicalConnector;
  private readonly manualConnect: boolean;
  private readonly connectionTraceRecorder: ConnectionTraceRecorder | null;
  private readonly option: MultiplexerDaemonHostOption;
  private readonly protocolVersion: number;
  private readonly now: () => number;
  private controlServer: MultiplexerControlServer | null = null;

  private readonly clientWatcherStartedDeviceIds = new Set<string>();
  private readonly clientWatcherStartingByDeviceId = new Map<
    string,
    Promise<void>
  >();

  private readonly activeControlIds = new Set<number>();
  private readonly legacyOwnershipGuard: LegacyOwnershipGuard;
  private idleTimer: NodeJS.Timeout | null = null;
  private idleTimeoutHandler: (() => void | Promise<void>) | undefined;
  private shutdownHandler: (() => void | Promise<void>) | undefined;
  private started = false;
  private shutdownRequested = false;
  private daemonStopReason: string | undefined;

  private get legacyOwnershipAttached(): boolean {
    return this.legacyOwnershipGuard.currentStatus === "attached";
  }

  private readonly handleDeviceConnected = (device: BaseDevice): void => {
    if (!this.legacyOwnershipAttached) return;
    if (!this.manualConnect) {
      void this.ensureClientWatcher(device.serial);
    }
    this.publishSnapshot();
  };

  private readonly handleDeviceDisconnected = (device: BaseDevice): void => {
    if (!this.legacyOwnershipAttached) return;
    this.clearClientWatcherStartState(device.serial);
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

  private readonly handleLegacyOwnershipChanged = (
    change: LegacyOwnershipChange,
  ): void => {
    if (change.status === "unattached") {
      this.connectionTraceRecorder?.recordLegacyOwnershipLost({
        ownerPid: change.ownerPid,
        previousOwnerPid: change.previousOwnerPid,
        reason: change.reason,
      });
      this.handleLegacyOwnershipLost();
    } else {
      this.connectionTraceRecorder?.recordLegacyOwnershipAttached({
        ownerPid: change.ownerPid,
        previousOwnerPid: change.previousOwnerPid,
        reason: change.reason,
      });
    }
    this.broadcast({
      kind: "event",
      event: "legacy-ownership-changed",
      data: change,
    });
  };

  constructor(option: MultiplexerDaemonHostOption) {
    this.option = option;
    this.manualConnect = option.physicalConnectorOption?.manualConnect ?? false;
    this.protocolVersion = option.protocolVersion;
    this.now = option.now ?? Date.now;
    this.connectionTraceRecorder = createConnectionTraceRecorder(
      option.connectionTrace,
      process.env.DriverConnectionTracePath,
    );
    this.physicalConnector =
      option.physicalConnector ??
      new PhysicalConnector({
        ...option.physicalConnectorOption,
        traceRecorder: this.connectionTraceRecorder,
      });
    this.legacyOwnershipGuard = new LegacyOwnershipGuard({
      legacyDriverDir: option.legacyDriverDir,
      onStatusChanged: this.handleLegacyOwnershipChanged,
    });
  }

  async start(): Promise<void> {
    if (this.started) return;

    this.shutdownRequested = false;
    this.daemonStopReason = undefined;
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
      const debugInfo = this.createDebugInfo(this.now());
      this.connectionTraceRecorder?.recordDaemonStarted({
        pid: process.pid,
        controlEndpoint: controlServer.controlEndpoint,
        protocolVersion: this.protocolVersion,
        ...(debugInfo ? { debugInfo } : {}),
      });
      this.legacyOwnershipGuard.start();
      this.scheduleIdleTimeoutIfNeeded();
    } catch (error) {
      await this.stop();
      throw error;
    }
  }

  async stop(): Promise<void> {
    if (!this.started && !this.controlServer && !this.connectionTraceRecorder) {
      return;
    }

    const stopErrors: unknown[] = [];
    const wasStarted = this.started;
    const daemonStopReason = this.daemonStopReason;
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
      this.clearRuntimeState();
    }
    if (wasStarted) {
      this.connectionTraceRecorder?.recordDaemonStopped({
        pid: process.pid,
        reason: daemonStopReason,
      });
    }
    this.daemonStopReason = undefined;
    try {
      await this.connectionTraceRecorder?.close();
    } catch (error) {
      stopErrors.push(error);
    }
    if (stopErrors.length > 0) throw createStopError(stopErrors);
  }

  setIdleTimeoutHandler(handler: () => void | Promise<void>): void {
    this.idleTimeoutHandler = handler;
  }

  setShutdownHandler(handler: () => void | Promise<void>): void {
    this.shutdownHandler = handler;
  }

  handleControlConnected(controlId: number): void {
    this.activeControlIds.add(controlId);
    this.connectionTraceRecorder?.recordControlSocketConnected(controlId, {
      activeControlCount: this.activeControlIds.size,
    });
    this.clearIdleTimeout();
    this.sendToControl(controlId, {
      kind: "event",
      event: "snapshot",
      data: this.createSnapshot(),
    });
  }

  handleControlDisconnected(controlId: number): void {
    this.activeControlIds.delete(controlId);
    this.connectionTraceRecorder?.recordControlSocketDisconnected(controlId, {
      activeControlCount: this.activeControlIds.size,
    });
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
        this.requestDaemonShutdown(
          (message.params as ControlRpcParams["shutdownDaemon"]).reason,
        );
        return undefined;
      case "closeClient":
        this.closeClient(
          (message.params as ControlRpcParams["closeClient"]).clientId,
        );
        return undefined;

      // The following features will be implemented in the next MR.
      case "startWSServer":
      case "sendMessageWithReply":
      case "sendMessageWithoutReply":
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
    const physicalClients = this.legacyOwnershipAttached
      ? this.physicalConnector.getAllUsbClients()
      : [];
    const debugInfo = this.createDebugInfo(generatedAt);
    return {
      protocolVersion: this.protocolVersion,
      generatedAt,
      devices: this.serializeDevices(devices),
      clients: this.serializeClients(physicalClients),
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
    return this.serializeDevices(
      await this.physicalConnector.connectDevices(
        params.timeout ?? -1,
        params.serial ?? null,
      ),
    );
  }

  private async connectUsbClients(
    params: ControlRpcParams["connectUsbClients"],
  ): Promise<ClientSnapshot[]> {
    defaultLogger.debug(
      "connectUsbClients of :" +
        params.deviceId +
        " waitTimeout:" +
        (params.waitTimeout ?? true) +
        " timeout:" +
        (params.timeout ?? -1),
    );
    if (!this.physicalConnector.devices.has(params.deviceId)) {
      defaultLogger.debug("connectUsbClients: resolve device == null");
      return [];
    }
    await this.ensureClientWatcher(params.deviceId);
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
    const clientInfos = clients.map((client) => client.info);
    defaultLogger.debug(
      "connectUsbClients: clients:" + JSON.stringify(clientInfos),
    );
    this.publishSnapshot();
    return snapshots;
  }

  private async startWatchClient(deviceId: string): Promise<void> {
    await this.ensureClientWatcher(deviceId);
  }

  private async stopWatchClient(deviceId: string): Promise<void> {
    this.clearClientWatcherStartState(deviceId);
    await this.physicalConnector.devices.get(deviceId)?.stopWatchClient();
  }

  private disconnectDevice(params: ControlRpcParams["disconnectDevice"]): void {
    this.clearClientWatcherStartState(params.deviceId);
    this.physicalConnector.devices.get(params.deviceId)?.disConnect();
  }

  private closeClient(clientId: number): void {
    this.physicalConnector.closeClient(clientId);
  }

  private async ensureClientWatcher(deviceId: string): Promise<void> {
    const device = this.physicalConnector.devices.get(deviceId);
    if (!device) {
      defaultLogger.debug(
        "ensureClientWatcher: resolve device == null:" + deviceId,
      );
      return;
    }
    this.assertLegacyOwnershipAttached();
    if (this.clientWatcherStartedDeviceIds.has(deviceId)) {
      return;
    }
    const existing = this.clientWatcherStartingByDeviceId.get(deviceId);
    if (existing) return existing;

    const starting = Promise.resolve()
      .then(async () => {
        await this.physicalConnector.startWatchClient(
          device,
          () => this.legacyOwnershipAttached,
        );
        this.clientWatcherStartedDeviceIds.add(deviceId);
      })
      .finally(() => {
        if (this.clientWatcherStartingByDeviceId.get(deviceId) === starting) {
          this.clientWatcherStartingByDeviceId.delete(deviceId);
        }
      });
    this.clientWatcherStartingByDeviceId.set(deviceId, starting);
    await starting;
  }

  private async startWatchAllClients(): Promise<void> {
    this.legacyOwnershipGuard.reacquire();
    await this.ensureAllClientWatchers();
    this.publishSnapshot();
  }

  private async ensureAllClientWatchers(): Promise<void> {
    await Promise.all(
      Array.from(this.physicalConnector.devices.keys(), (deviceId) =>
        this.ensureClientWatcher(deviceId),
      ),
    );
  }

  private async stopWatchAllClients(): Promise<void> {
    await Promise.allSettled(
      Array.from(this.clientWatcherStartingByDeviceId.values()),
    );
    this.clientWatcherStartingByDeviceId.clear();
    this.clientWatcherStartedDeviceIds.clear();
    await Promise.all(
      Array.from(this.physicalConnector.devices.values(), (device) =>
        device.stopWatchClient(),
      ),
    );
    this.publishSnapshot();
  }

  private serializeDevice(device: BaseDevice): DeviceSnapshot {
    return {
      ...device.info,
      ports: [...device.ports],
      host: device.getHost(),
    };
  }

  private serializeClient(client: UsbClient): ClientSnapshot {
    return { ...client.info };
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
  }

  private clearClientWatcherStartState(deviceId: string): void {
    this.clientWatcherStartedDeviceIds.delete(deviceId);
    this.clientWatcherStartingByDeviceId.delete(deviceId);
  }

  private clearRuntimeState(): void {
    this.clearAllClientWatcherStartState();
    this.activeControlIds.clear();
    this.clearIdleTimeout();
  }

  private handleLegacyOwnershipLost(): void {
    this.clearAllClientWatcherStartState();
    this.physicalConnector.disableAllClients();
    this.physicalConnector.usbClients.clear();
    this.publishSnapshot();
  }

  private clearAllClientWatcherStartState(): void {
    this.clientWatcherStartedDeviceIds.clear();
    this.clientWatcherStartingByDeviceId.clear();
  }

  private assertLegacyOwnershipAttached(): void {
    if (!this.legacyOwnershipAttached) {
      throw new Error("Multiplexer legacy owner is not attached");
    }
  }

  private createDebugInfo(
    timestamp: number = this.now(),
  ): MultiplexerDebugInfo | undefined {
    if (!this.option.debugInfo) return undefined;
    return {
      ...this.option.debugInfo,
      protocolVersion: this.protocolVersion,
      processId: process.pid,
      timestamp,
    };
  }

  private requestDaemonShutdown(reason?: string): void {
    if (!this.shutdownHandler) {
      throw createControlError(
        "daemon-shutdown-unavailable",
        "Multiplexer daemon shutdown handler is not configured",
      );
    }
    if (this.shutdownRequested) return;
    this.shutdownRequested = true;
    this.daemonStopReason = reason ?? "control_request";
    this.connectionTraceRecorder?.recordDaemonShutdownRequested({
      reason: this.daemonStopReason,
    });
    this.clearIdleTimeout();
    const handler = this.shutdownHandler;
    setImmediate(() => void handler());
  }

  private scheduleIdleTimeoutIfNeeded(): void {
    if (!this.started || !this.idleTimeoutHandler) return;
    const idleTimeout = this.option.multiplexerDaemonIdleTimeout;
    if (idleTimeout < 0 || this.isInUse()) {
      return;
    }
    this.clearIdleTimeout();
    this.idleTimer = setTimeout(() => {
      this.idleTimer = null;
      if (this.isInUse()) return;
      this.daemonStopReason = "idle_timeout";
      this.connectionTraceRecorder?.recordDaemonIdleTimeoutReached({
        idleTimeout,
      });
      void this.idleTimeoutHandler?.();
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
  return { code, message, details };
}

function createStopError(errors: unknown[]): Error {
  const message = errors
    .map((error) => (error instanceof Error ? error.message : String(error)))
    .join("; ");
  const stopError = new Error(`Failed to stop multiplexer host: ${message}`);
  (stopError as any).errors = errors;
  return stopError;
}
