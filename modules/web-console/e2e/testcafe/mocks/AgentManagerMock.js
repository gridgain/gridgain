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

import thebugger from 'thebugger';
import Server from 'socket.io';
import {RequestHook} from 'testcafe';

const START_PORT = 3003;

const portPool = {
    async getNext() {
        return START_PORT.toString();
    }
};

const noop = () => {};

export class AgentManagerMock extends RequestHook {
    constructor() {
        super(([/socket\.io/]));
        this._port = portPool.getNext();
        this._io = Server();
        this._port.then((port) => this._io.listen(port));
    }
    async onRequest(e) {
    	e.requestOptions.host = e.requestOptions.host.replace(e.requestOptions.port, await this._port);
    	e.requestOptions.port = await this._port;
    }
    async onResponse() {}

    destroy() {
    	this._io.close();
    }
}
