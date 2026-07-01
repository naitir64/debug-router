// Copyright 2024 The Lynx Authors. All rights reserved.
// Licensed under the Apache License Version 2.0 that can be found in the
// LICENSE file in the root directory of this source tree.

import { BaseDevice } from "../../device/BaseDevice";
import {
  PhysicalConnector,
  PhysicalConnectorOption,
} from "../../physical/PhysicalConnector";
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
  MULTIPLEXER_MIN_SUPPORTED_PROTOCOL_VERSION,
  MULTIPLEXER_PROTOCOL_VERSION,
  Snapshot,
  WebSocketServerInfo,
} from "../protocol";
import { MultiplexerDaemonHost } from "./MultiplexerDaemon";
import {
  MultiplexerControlHost,
  MultiplexerControlServer,
} from "./MultiplexerControlServer";
import {
  LegacyOwnershipChange,
  LegacyOwnershipGuard,
} from "./LegacyOwnershipGuard";

export type MultiplexerHostOption = PhysicalConnectorOption & {
  controlPort?: number;
  protocolVersion?: number;
  minSupportedProtocolVersion?: number;
  daemonVersion?: string;
  capabilities?: string[];
  legacyDriverDir?: string;
  multiplexerDaemonIdleTimeout?: number;

  // only used for tests or embedding
  physicalConnector?: PhysicalConnector;
  PhysicalConnectorCtor?: new (
    option?: PhysicalConnectorOption,
  ) => PhysicalConnector;
  now?: () => number;
};

type MultiplexerHostStartOption = {
  multiplexerDaemonIdleTimeout?: number;
};

export class MultiplexerHost
  implements MultiplexerDaemonHost, MultiplexerControlHost {
  private physicalConnector: PhysicalConnector;
  private readonly option: MultiplexerHostOption;
  private readonly protocolVersion: number;
  private readonly minSupportedProtocolVersion: number;
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
  private legacyOwnershipAttached = false;
  private idleTimer: NodeJS.Timeout | null = null;
  private idleTimeoutHandler: (() => void | Promise<void>) | undefined;
  private runtimeIdleTimeout: number | undefined;
  private started = false;

  private readonly handleDeviceConnected = (device: BaseDevice): void => {
    if (!this.legacyOwnershipAttached) {
      return;
    }

    if (
      this.deviceDiscoveryAutoListensClients ||
      this.allClientWatchersRequested
    ) {
      void this.ensureClientDiscovery(device.serial);
    }

    this.broadcast({
      kind: "event",
      event: "device-connected",
      data: this.serializeDevice(device),
    });
    this.publishSnapshot();
  };

  private readonly handleDeviceDisconnected = (device: BaseDevice): void => {
    if (!this.legacyOwnershipAttached) {
      return;
    }

    this.clearClientDiscoveryForDevice(device.serial);

    this.broadcast({
      kind: "event",
      event: "device-disconnected",
      data: {
        serial: device.serial,
      },
    });
    this.publishSnapshot();
  };

  private readonly handleClientConnected = (client: UsbClient): void => {
    if (!this.legacyOwnershipAttached) {
      return;
    }

    this.broadcast({
      kind: "event",
      event: "client-connected",
      data: this.serializeClient(client),
    });
    this.publishSnapshot();
  };

  private readonly handleClientDisconnected = (id: number): void => {
    if (!this.legacyOwnershipAttached) {
      return;
    }

    this.broadcast({
      kind: "event",
      event: "client-disconnected",
      data: {
        id,
      },
    });
    this.publishSnapshot();
  };

  private readonly handleUsbClientMessage = (
    payload: PhysicalConnectorEvent["usb-client-message"],
  ): void => {
    if (!this.legacyOwnershipAttached) {
      return;
    }

    this.broadcast({
      kind: "event",
      event: "usb-client-message",
      data: {
        id: payload.id,
        message: payload.message,
      },
    });
  };

  private readonly handleLegacyOwnershipChanged = (
    change: LegacyOwnershipChange,
  ): void => {
    if (change.status === "unattached") {
      this.handleLegacyOwnershipLost();
    } else {
      this.legacyOwnershipAttached = true;
    }

    this.broadcast({
      kind: "event",
      event: "legacy-ownership-changed",
      data: change,
    });
  };

  constructor(option: MultiplexerHostOption = {}) {
    this.option = option;
    this.protocolVersion =
      option.protocolVersion ?? MULTIPLEXER_PROTOCOL_VERSION;
    this.minSupportedProtocolVersion =
      option.minSupportedProtocolVersion ??
      MULTIPLEXER_MIN_SUPPORTED_PROTOCOL_VERSION;
    this.now = option.now ?? Date.now;
    this.physicalConnector = this.createPhysicalConnector();
    this.legacyOwnershipGuard = new LegacyOwnershipGuard({
      legacyDriverDir: option.legacyDriverDir,
      onStatusChanged: this.handleLegacyOwnershipChanged,
    });
  }

  async start(startOption?: unknown): Promise<void> {
    if (this.started) {
      return;
    }

    if (
      isMultiplexerHostStartOption(startOption) &&
      startOption.multiplexerDaemonIdleTimeout !== undefined
    ) {
      this.runtimeIdleTimeout = startOption.multiplexerDaemonIdleTimeout;
    }
    this.bindPhysicalConnectorEvents();
    const controlServer = new MultiplexerControlServer({
      host: this,
      controlPort: this.option.controlPort,
      protocolVersion: this.protocolVersion,
      minSupportedProtocolVersion: this.minSupportedProtocolVersion,
      daemonVersion: this.option.daemonVersion,
      capabilities: this.option.capabilities,
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
    if (!this.started && !this.controlServer) {
      return;
    }

    const stopErrors: unknown[] = [];
    this.started = false;
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

    if (stopErrors.length > 0) {
      throw createStopError(stopErrors);
    }
  }

  getControlPort(): number {
    return this.controlServer?.controlPort ?? this.option.controlPort ?? 0;
  }

  setIdleTimeoutHandler(handler: () => void | Promise<void>): void {
    this.idleTimeoutHandler = handler;
    this.scheduleIdleTimeoutIfNeeded();
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

  async handleControlRpc(
    _controlId: number,
    message: ControlRpcRequest,
  ): Promise<unknown> {
    switch (message.method) {
      case "connectDevices":
        return this.connectDevices(
          message.params as ControlRpcParams["connectDevices"],
        );
      case "getDevices":
        return this.getDeviceSnapshots(
          message.params as ControlRpcParams["getDevices"],
        );
      case "connectUsbClients":
        return this.connectUsbClients(
          message.params as ControlRpcParams["connectUsbClients"],
        );
      case "startWatchClient":
        return this.startWatchClient(
          message.params as ControlRpcParams["startWatchClient"],
        );
      case "stopWatchClient":
        return this.stopWatchClient(
          message.params as ControlRpcParams["stopWatchClient"],
        );
      case "disconnectDevice":
        return this.disconnectDevice(
          message.params as ControlRpcParams["disconnectDevice"],
        );
      case "reacquireLegacyOwnership":
        return this.reacquireLegacyOwnership();
      case "startWSServer":
        return this.startWSServer();
      case "startWatchAllClients":
        return this.startWatchAllClients(
          message.params as ControlRpcParams["startWatchAllClients"],
        );
      case "sendMessageToWeb":
        return this.sendMessageToWeb(
          (message.params as ControlRpcParams["sendMessageToWeb"]).message,
        );
      case "sendMessageToApp":
        return this.sendMessageToApp(
          (message.params as ControlRpcParams["sendMessageToApp"]).id,
          (message.params as ControlRpcParams["sendMessageToApp"]).message,
        );
      case "sendCustomizedMessage":
        return this.sendCustomizedMessage(
          message.params as ControlRpcParams["sendCustomizedMessage"],
        );
      case "sendRawMessage":
        return this.physicalConnector.sendRawMessage(
          (message.params as ControlRpcParams["sendRawMessage"]).clientId,
          (message.params as ControlRpcParams["sendRawMessage"]).message,
        );
      case "sendMessage":
        this.physicalConnector.sendMessage(
          (message.params as ControlRpcParams["sendMessage"]).clientId,
          (message.params as ControlRpcParams["sendMessage"]).message,
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
          `Unknown multiplexer control RPC: ${
            (message as ControlRpcRequest).method
          }`,
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
    this.broadcast({
      kind: "event",
      event: "snapshot",
      data: snapshot,
    });
  }

  createSnapshot(): Snapshot {
    return {
      protocolVersion: this.protocolVersion,
      generatedAt: this.now(),
      devices: this.serializeDevices(
        Array.from(this.physicalConnector.devices.values()),
      ),
      clients: this.serializeClients(this.physicalConnector.getAllUsbClients()),
      daemonVersion: this.option.daemonVersion,
      capabilities: this.option.capabilities
        ? [...this.option.capabilities]
        : undefined,
    };
  }

  createEmptySnapshot(): Snapshot {
    return {
      protocolVersion: this.protocolVersion,
      generatedAt: this.now(),
      devices: [],
      clients: [],
      daemonVersion: this.option.daemonVersion,
      capabilities: this.option.capabilities
        ? [...this.option.capabilities]
        : undefined,
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
    if (!this.legacyOwnershipAttached) {
      return [];
    }

    return this.physicalConnector.getAllUsbClients();
  }

  getDevices(
    timeout: number = -1,
    serial: string | null = null,
  ): Promise<BaseDevice[]> {
    if (!this.legacyOwnershipAttached) {
      return Promise.resolve([]);
    }

    return this.physicalConnector.getDevices(timeout, serial);
  }

  private async connectDevices(
    params: ControlRpcParams["connectDevices"],
  ): Promise<DeviceSnapshot[]> {
    const generation = this.physicalDiscoveryGeneration;
    await this.ensureDeviceDiscovery(
      params.isAutoListenClients ?? true,
      generation,
    );
    this.assertPhysicalDiscoveryCurrent(generation);
    return this.getDeviceSnapshots({
      timeout: params.timeout,
      serial: params.serial,
    });
  }

  private async getDeviceSnapshots(
    params: ControlRpcParams["getDevices"],
  ): Promise<DeviceSnapshot[]> {
    this.assertPhysicalDiscoveryCurrent(this.physicalDiscoveryGeneration);
    const devices = await this.physicalConnector.getDevices(
      params.timeout ?? -1,
      params.serial ?? null,
    );
    return this.serializeDevices(devices);
  }

  private async connectUsbClients(
    params: ControlRpcParams["connectUsbClients"],
  ): Promise<ClientSnapshot[]> {
    const generation = this.physicalDiscoveryGeneration;
    await this.ensureDeviceDiscovery(false, generation);
    await this.ensureClientDiscovery(params.deviceId, generation);
    this.assertPhysicalDiscoveryCurrent(generation);

    const clients = await this.getDeviceUsbClients(
      params.deviceId,
      params.timeout ?? -1,
      params.waitTimeout ?? true,
      params.clientName ?? null,
    );
    const snapshots = this.serializeClients(clients);
    this.publishClientSnapshot();
    return snapshots;
  }

  private async startWatchClient(
    params: ControlRpcParams["startWatchClient"],
  ): Promise<void> {
    const generation = this.physicalDiscoveryGeneration;
    await this.ensureDeviceDiscovery(false, generation);
    await this.ensureClientDiscovery(params.deviceId, generation);
  }

  private async stopWatchClient(
    params: ControlRpcParams["stopWatchClient"],
  ): Promise<void> {
    this.clearClientDiscoveryForDevice(params.deviceId);

    const device = this.physicalConnector.devices.get(params.deviceId);
    if (!device) {
      return;
    }

    await device.stopWatchClient();
  }

  private disconnectDevice(
    params: ControlRpcParams["disconnectDevice"],
  ): void {
    this.clearClientDiscoveryForDevice(params.deviceId);

    const device = this.physicalConnector.devices.get(params.deviceId);
    if (!device) {
      return;
    }

    device.disConnect();
  }

  private reacquireLegacyOwnership(): void {
    this.legacyOwnershipGuard.reacquire();
  }

  private async ensureDeviceDiscovery(
    isAutoListenClients: boolean = true,
    generation: number = this.physicalDiscoveryGeneration,
  ): Promise<void> {
    this.assertPhysicalDiscoveryCurrent(generation);
    if (!this.deviceDiscoveryStarted && !this.deviceDiscoveryStarting) {
      this.deviceDiscoveryStarting = this.physicalConnector
        .connectDevices(-1, null, false)
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

    if (this.deviceDiscoveryStarting) {
      await this.deviceDiscoveryStarting;
    }
    this.assertPhysicalDiscoveryCurrent(generation);
    if (isAutoListenClients) {
      await this.ensureAutoClientDiscovery(generation);
    }
  }

  private async ensureAutoClientDiscovery(
    generation: number = this.physicalDiscoveryGeneration,
  ): Promise<void> {
    this.assertPhysicalDiscoveryCurrent(generation);
    if (this.option.manualConnect) {
      return;
    }

    this.deviceDiscoveryAutoListensClients = true;
    await this.ensureClientDiscoveryForCurrentDevices(generation);
  }

  private async ensureClientDiscovery(
    deviceId: string,
    generation: number = this.physicalDiscoveryGeneration,
  ): Promise<void> {
    this.assertPhysicalDiscoveryCurrent(generation);
    if (this.clientDiscoveryStartedDeviceIds.has(deviceId)) {
      return;
    }

    const existing = this.clientDiscoveryStartingByDeviceId.get(deviceId);
    if (existing) {
      await existing;
      return;
    }

    const starting = Promise.resolve()
      .then(async () => {
        this.assertPhysicalDiscoveryCurrent(generation);
        const device = this.physicalConnector.devices.get(deviceId);
        if (!device) {
          return;
        }

        await this.physicalConnector.startWatchClient(device, () =>
          this.isPhysicalDiscoveryCurrent(generation),
        );
        if (this.isPhysicalDiscoveryCurrent(generation)) {
          this.clientDiscoveryStartedDeviceIds.add(deviceId);
        }
      })
      .finally(() => {
        if (
          this.isPhysicalDiscoveryCurrent(generation) &&
          this.clientDiscoveryStartingByDeviceId.get(deviceId) === starting
        ) {
          this.clientDiscoveryStartingByDeviceId.delete(deviceId);
        }
      });

    this.clientDiscoveryStartingByDeviceId.set(deviceId, starting);
    await starting;
  }

  private async getDeviceUsbClients(
    deviceId: string,
    timeout: number,
    waitTimeout: boolean,
    clientName: string | null,
  ): Promise<UsbClient[]> {
    if (!waitTimeout) {
      return this.physicalConnector.waitDeviceUsbClients(deviceId, timeout);
    }

    return this.physicalConnector.getDeviceUsbClients(
      deviceId,
      timeout,
      clientName,
    );
  }

  private async startWatchAllClients(
    params: ControlRpcParams["startWatchAllClients"],
  ): Promise<void> {
    const generation = this.physicalDiscoveryGeneration;
    this.assertPhysicalDiscoveryCurrent(generation);
    this.allClientWatchersRequested = true;
    await this.ensureDeviceDiscovery(false, generation);
    await this.ensureClientDiscoveryForCurrentDevices(generation);
    this.publishClientSnapshot();
  }

  private startWSServer(): WebSocketServerInfo | undefined {
    return undefined;
  }

  private sendMessageToWeb(_message: string): void {
    defaultLogger.warn(
      "Multiplexer WebSocket frontend is not enabled in this split",
    );
  }

  private sendMessageToApp(id: number, message: string): void {
    this.physicalConnector.sendMessage(id, message);
  }

  private sendCustomizedMessage(
    params: ControlRpcParams["sendCustomizedMessage"],
  ): Promise<string> {
    const client = this.getUsbClient(params.clientId);
    return client.sendCustomizedMessage(
      params.method,
      params.params,
      params.sessionId ?? -1,
      params.type ?? "CDP",
    );
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
    const host = safeGetDeviceHost(device);
    const snapshot: DeviceSnapshot = {
      os: info.os,
      title: info.title,
      serial: info.serial,
      ports: [...device.ports],
    };

    if (host !== undefined) {
      snapshot.host = host;
    }

    return snapshot;
  }

  private serializeClient(client: UsbClient): ClientSnapshot {
    const info: ClientDescription = client.info;
    const rawInfo = cloneJsonValue(info.query.raw_info);
    const query = {
      app: info.query.app,
      os: info.query.os,
      device: info.query.device,
      device_model: info.query.device_model,
      device_id: info.query.device_id,
      sdk_version: info.query.sdk_version,
      raw_info: rawInfo,
    };

    if (query.sdk_version === undefined) {
      delete query.sdk_version;
    }
    if (query.raw_info === undefined) {
      delete query.raw_info;
    }

    return {
      port: info.port,
      id: info.id,
      query,
    };
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

    const PhysicalConnectorCtor =
      this.option.PhysicalConnectorCtor ?? PhysicalConnector;
    return new PhysicalConnectorCtor(this.option);
  }

  private canRecreatePhysicalConnector(): boolean {
    return (
      !this.option.physicalConnector ||
      this.option.PhysicalConnectorCtor !== undefined
    );
  }

  private async ensureClientDiscoveryForCurrentDevices(
    generation: number = this.physicalDiscoveryGeneration,
  ): Promise<void> {
    this.assertPhysicalDiscoveryCurrent(generation);
    const deviceIds = Array.from(this.physicalConnector.devices.keys());
    await Promise.all(
      deviceIds.map((deviceId) =>
        this.ensureClientDiscovery(deviceId, generation),
      ),
    );
  }

  private clearClientDiscoveryForDevice(deviceId: string): void {
    this.clientDiscoveryStartedDeviceIds.delete(deviceId);
    this.clientDiscoveryStartingByDeviceId.delete(deviceId);
  }

  private resetDiscoveryState(): void {
    this.physicalDiscoveryGeneration++;
    this.legacyOwnershipAttached = false;
    this.deviceDiscoveryStarted = false;
    this.deviceDiscoveryStarting = null;
    this.deviceDiscoveryAutoListensClients = false;
    this.clientDiscoveryStartedDeviceIds.clear();
    this.clientDiscoveryStartingByDeviceId.clear();
    this.allClientWatchersRequested = false;
    this.activeControlIds.clear();
    this.clearIdleTimeout();
  }

  private publishClientSnapshot(): void {
    this.publishSnapshot();
  }

  private handleLegacyOwnershipLost(): void {
    this.legacyOwnershipAttached = false;
    this.resetPhysicalDiscoveryState();
    const oldPhysicalConnector = this.physicalConnector;
    this.unbindPhysicalConnectorEvents();
    oldPhysicalConnector.disableAllClients();
    oldPhysicalConnector.devices.clear();
    oldPhysicalConnector.usbClients.clear();
    oldPhysicalConnector.selectedClient = undefined;
    if (this.canRecreatePhysicalConnector()) {
      this.physicalConnector = this.createPhysicalConnector();
      this.bindPhysicalConnectorEvents();
      void oldPhysicalConnector.close().catch((error: any) => {
        defaultLogger.warn(
          `Failed to close preempted physical connector: ${error?.message}`,
        );
      });
    } else {
      this.bindPhysicalConnectorEvents();
    }
    this.publishSnapshot(this.createEmptySnapshot());
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

  private scheduleIdleTimeoutIfNeeded(): void {
    if (!this.started || !this.idleTimeoutHandler) {
      return;
    }

    const idleTimeout = this.getIdleTimeout();
    if (idleTimeout === undefined || !this.isIdle()) {
      return;
    }

    this.clearIdleTimeout();
    this.idleTimer = setTimeout(() => {
      this.idleTimer = null;
      if (!this.isIdle()) {
        return;
      }
      void this.idleTimeoutHandler?.();
    }, idleTimeout);
  }

  private clearIdleTimeout(): void {
    if (!this.idleTimer) {
      return;
    }

    clearTimeout(this.idleTimer);
    this.idleTimer = null;
  }

  private isIdle(): boolean {
    return this.activeControlIds.size === 0;
  }

  private getIdleTimeout(): number | undefined {
    const idleTimeout =
      this.runtimeIdleTimeout ?? this.option.multiplexerDaemonIdleTimeout;
    if (
      idleTimeout === undefined ||
      !Number.isFinite(idleTimeout) ||
      idleTimeout < 0
    ) {
      return undefined;
    }

    return idleTimeout;
  }
}

function createControlError(
  code: string,
  message: string,
  details?: unknown,
): ControlRpcError {
  return {
    code,
    message,
    details,
  };
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
  if (value === undefined || value === null) {
    return value;
  }

  try {
    return JSON.parse(JSON.stringify(value));
  } catch (_error) {
    return undefined;
  }
}

function isMultiplexerHostStartOption(
  option: unknown,
): option is MultiplexerHostStartOption {
  return typeof option === "object" && option !== null;
}
