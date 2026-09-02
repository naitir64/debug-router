// Copyright 2024 The Lynx Authors. All rights reserved.
// Licensed under the Apache License Version 2.0 that can be found in the
// LICENSE file in the root directory of this source tree.

import { Client } from "../connector/Client";
import { ClientDescription } from "../utils/type";
import { Connection } from "./Connection";

export class UsbClient extends Client {
  constructor(
    readonly info: ClientDescription,
    readonly connection: Connection,
  ) {
    super();
  }

  clientId(): number {
    return this.info.id;
  }

  deviceId() {
    return this.info.query.device_id;
  }

  close() {
    this.connection.close();
  }

  // just send message
  sendMessage(message: any) {
    this.connection.send(message);
  }
}
