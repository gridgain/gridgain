/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Server } from 'ws';
import {RequestHook} from 'testcafe';
import getPort from 'get-port';

export class WebSocketHook extends RequestHook {
    constructor() {
        super(([/browsers/]));

        this._port = getPort();
    }

    async onRequest(e) {
        e.requestOptions.host = `localhost:${await this._port}`;
        e.requestOptions.hostname = 'localhost';
        e.requestOptions.port = await this._port;
    }

    async onResponse() {}

    destroy() {
        this._io.close();
    }

    /**
     * @param {string} event
     * @param {()=>void} listener
     */
    on(event, listener) {
        this._io.on(event, listener);

        return this;
    }

    /**
     * @param {string} eventType
     * @param {any} payload
     */
    emit(eventType, payload) {
        this._io.clients.forEach((ws) => {
            ws.send(JSON.stringify({eventType, payload: JSON.stringify(payload)}));
        });
    }

    /**
     * @param {Array<(hook: WebSocketHook) => any>} hooks
     */
    use(...hooks) {
        this._port.then((port) => {
            this._io = new Server({port});

            hooks.forEach((hook) => hook(this));
        });

        return this;
    }
}
