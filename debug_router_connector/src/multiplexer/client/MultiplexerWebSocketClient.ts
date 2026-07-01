// Copyright 2026 The Lynx Authors. All rights reserved.
// Licensed under the Apache License Version 2.0 that can be found in the
// LICENSE file in the root directory of this source tree.

import { Client } from "../../connector/Client";
import { defaultLogger } from "../../utils/logger";
import { RequireMessageType, SocketEvent } from "../../utils/type";
import type { WebSocketClientSnapshot } from "../protocol";
import { MultiplexerDaemonClient } from "./MultiplexerDaemonClient";

export type MultiplexerWebSocketClientOption = {
  snapshot: WebSocketClientSnapshot;
  daemonClient: MultiplexerDaemonClient;
};

export class MultiplexerWebSocketClient extends Client {
  private snapshot: WebSocketClientSnapshot;
  private readonly daemonClient: MultiplexerDaemonClient;

  constructor(option: MultiplexerWebSocketClientOption) {
    super();
    this.snapshot = cloneSnapshot(option.snapshot);
    this.daemonClient = option.daemonClient;
  }

  get info(): WebSocketClientSnapshot {
    return cloneSnapshot(this.snapshot);
  }

  static fromSnapshot(
    snapshot: WebSocketClientSnapshot,
    daemonClient: MultiplexerDaemonClient,
  ): MultiplexerWebSocketClient {
    return new MultiplexerWebSocketClient({
      snapshot,
      daemonClient,
    });
  }

  updateFromSnapshot(snapshot: WebSocketClientSnapshot): void {
    if (snapshot.id !== this.clientId()) {
      throw new Error(
        `Cannot update multiplexer WebSocket client ${this.clientId()} with snapshot ${
          snapshot.id
        }`,
      );
    }
    this.snapshot = cloneSnapshot(snapshot);
  }

  clientId(): number {
    return this.snapshot.id;
  }

  type(): string {
    return this.snapshot.type;
  }

  close(): void {
    void this.daemonClient
      .call("closeClient", { clientId: this.clientId() })
      .catch((error: Error) => {
        defaultLogger.warn(
          `Failed to close multiplexer WebSocket client: ${error.message}`,
        );
      });
  }

  sendMessage(message: string): void {
    void this.daemonClient
      .call("sendMessageWithoutReply", {
        target: this.type() === "Driver" ? "web" : "app",
        clientId: this.clientId(),
        message,
      })
      .catch((error: Error) => {
        defaultLogger.warn(
          `Failed to send multiplexer WebSocket client message: ${error.message}`,
        );
      });
  }

  sendCustomizedMessage(
    method: string,
    params: Object | string = "",
    sessionId: number = -1,
    type: string = "CDP",
  ): Promise<string> {
    const message: RequireMessageType = {
      event: SocketEvent.Customized,
      data: {
        type,
        data: {
          client_id: -1,
          session_id: sessionId,
          message: {
            id: Client.messageIdCounter++,
            method,
            params,
          },
        },
        sender: 0,
      },
    };

    return this.daemonClient
      .call("sendMessageWithReply", {
        clientId: this.clientId(),
        message,
      })
      .then((response) => {
        const responseMessage = (response as any)?.data?.data?.message;
        if (typeof responseMessage !== "string") {
          throw new Error("Invalid Customized response message");
        }

        return responseMessage;
      });
  }
}

function cloneSnapshot(
  snapshot: WebSocketClientSnapshot,
): WebSocketClientSnapshot {
  return JSON.parse(JSON.stringify(snapshot));
}
