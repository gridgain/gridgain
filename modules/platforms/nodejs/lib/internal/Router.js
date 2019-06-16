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

'use strict';

const Util = require('util');
const Errors = require('../Errors');
const IgniteClient = require('../IgniteClient');
const ClientSocket = require('./ClientSocket');
const AffinityAwarenessUtils = require('./AffinityAwarenessUtils');
const BinaryUtils = require('./BinaryUtils');
const BinaryObject = require('../BinaryObject');
const ObjectType = require('../ObjectType');
const ArgumentChecker = require('./ArgumentChecker');
const Logger = require('./Logger');

// Number of tries to get cache partitions info
const GET_CACHE_PARTITIONS_RETRIES = 3;
// Delay between tries to get cache partitions info
const GET_CACHE_PARTITIONS_DELAY = 100;

class Router {

    constructor(onStateChanged) {
        this._state = IgniteClient.STATE.DISCONNECTED;
        this._onStateChanged = onStateChanged;

        this._affinityAwarenessAllowed = false;
        // ClientSocket instance with no node UUID
        this._legacyConnection = null;
        // Array of endpoints which we are not connected to. Mostly used when Affinity Awareness is on
        this._inactiveEndpoints = [];

        /** Affinity Awareness only fields */
        // This flag indicates if we have at least two alive connections
        this._affinityAwarenessActive = false;
        // Contains the background task (promise) or null
        this._backgroundConnectTask = null;
        // {Node UUID -> ClientSocket instance}
        this._connections = {};
        // {cacheId -> CacheAffinityMap}
        this._distributionMap = new Map();
        this._affinityTopologyVer = null;
    }

    async connect(communicator, config) {
        if (this._state !== IgniteClient.STATE.DISCONNECTED) {
            throw new Errors.IllegalStateError();
        }

        // Wait for background task to stop before we move forward
        await this._waitBackgroundConnect();

        this._communicator = communicator;
        this._config = config;
        this._affinityAwarenessAllowed = config._affinityAwareness;
        this._inactiveEndpoints = [...config._endpoints];

        await this._connect();
    }

    disconnect() {
        if (this._state !== IgniteClient.STATE.DISCONNECTED) {
            this._changeState(IgniteClient.STATE.DISCONNECTED);

            for (const socket of this._getAllConnections()) {
                    socket.disconnect();
            }

            this._cleanUp();
        }
    }

    async send(opCode, payloadWriter, payloadReader = null, affinityHint = null) {
        if (this._state !== IgniteClient.STATE.CONNECTED) {
            throw new Errors.IllegalStateError();
        }

        if (this._affinityAwarenessActive && affinityHint) {
            await this._affinitySend(opCode, payloadWriter, payloadReader, affinityHint);
        }
        else {
            // If _affinityAwarenessActive flag is not set, we have exactly one connection
            // but it can be either a legacy one or a modern one
            // If affinityHint has not been passed we want to always use one socket (as long as it is alive)
            // because some SQL requests (e.g., cursor-related) require to be sent to the same cluster node
            await this._getAllConnections()[0].sendRequest(opCode, payloadWriter, payloadReader);
        }
    }

    removeActiveCache(cacheId) {
        this._distributionMap.delete(cacheId);
    }

    async _connect() {
        const errors = new Array();
        const endpoints = this._inactiveEndpoints;
        const config = this._config;
        const communicator = this._communicator;
        const onSocketDisconnect = this._onSocketDisconnect.bind(this);
        const onAffinityTopologyChange = this._onAffinityTopologyChange.bind(this);
        const endpointsNum = endpoints.length;
        const random = this._getRandomInt(endpointsNum);

        this._changeState(IgniteClient.STATE.CONNECTING);

        for (let i = 0; i < endpoints.length; i++) {
            const index = (i + random) % endpointsNum;
            const endpoint = endpoints[index];

            try {
                const socket = new ClientSocket(
                    endpoint, config, communicator,
                    onSocketDisconnect,
                    onAffinityTopologyChange);

                await socket.connect();
                Logger.logDebug(Util.format('Connected to %s', endpoint));
                this._changeState(IgniteClient.STATE.CONNECTED);
                this._addConnection(socket);

                this._runBackgroundConnect();

                return;
            }
            catch (err) {
                Logger.logDebug(Util.format('Could not connect to %s. Error: "%s"', endpoint, err.message));
                errors.push(Util.format('[%s] %s', endpoint, err.message));
            }
        }

        const error = errors.join('; ');
        this._changeState(IgniteClient.STATE.DISCONNECTED, error);
        throw new Errors.IgniteClientError(error);
    }

    // Can be called when there are no alive connections left
    async _reconnect() {
        await this._waitBackgroundConnect();
        await this._connect();
    }

    _runBackgroundConnect() {
        if (this._affinityAwarenessAllowed && !this._backgroundConnectTask) {
            // Only one task can be active
            this._backgroundConnectTask = this._backgroundConnect();
            this._backgroundConnectTask.then(() => this._backgroundConnectTask = null);
        }
    }

    async _waitBackgroundConnect() {
        if (this._backgroundConnectTask) {
            await this._backgroundConnectTask;
        }
    }

    async _backgroundConnect() {
        // Local copy of _inactiveEndpoints to make sure the array is not being changed during the 'for' cycle
        const endpoints = [...this._inactiveEndpoints];
        const config = this._config;
        const communicator = this._communicator;
        const onSocketDisconnect = this._onSocketDisconnect.bind(this);
        const onAffinityTopologyChange = this._onAffinityTopologyChange.bind(this);

        for (const endpoint of endpoints) {
            try {
                const socket = new ClientSocket(
                    endpoint, config, communicator,
                    onSocketDisconnect,
                    onAffinityTopologyChange);

                await socket.connect();
                Logger.logDebug(Util.format('Connected (in background) to %s', endpoint));

                // While we were waiting for socket to connect, someone could call disconnect()
                if (this._state !== IgniteClient.STATE.CONNECTED) {
                    // If became not connected, stop this task
                    socket.disconnect();
                    return;
                }

                this._addConnection(socket);
            }
            catch (err) {
                Logger.logDebug(Util.format('Could not connect (in background) to %s. Error: "%s"', endpoint, err.message));

                // While we were waiting for socket to connect, someone could call disconnect()
                if (this._state !== IgniteClient.STATE.CONNECTED) {
                    // If became not connected, stop this task
                    socket.disconnect();
                    return;
                }
            }
        }
    }

    _cleanUp() {
        this._legacyConnection = null;
        this._inactiveEndpoints = [];

        this._affinityAwarenessActive = false;
        this._connections = {};
        this._distributionMap = new Map();
        this._affinityTopologyVer = null;
    }

    _getAllConnections() {
        const allConnections = Object.values(this._connections);

        if (this._legacyConnection) {
            allConnections.push(this._legacyConnection);
        }

        return allConnections;
    }

    _addConnection(socket) {
        const nodeUUID = socket.nodeUUID;

        if (this._affinityAwarenessAllowed && nodeUUID) {
            if (nodeUUID in this._connections) {
                // This can happen if the same node has several IPs
                // We will keep more fresh connection alive
                this._connections[nodeUUID].disconnect();
            }
            this._connections[nodeUUID] = socket;
        }
        else {
            if (this._legacyConnection) {
                // We already have a legacy connection
                // We will keep more fresh connection alive
                this._legacyConnection.disconnect();
            }
            this._legacyConnection = socket;
        }
        // Remove the endpoint from _inactiveEndpoints
        const index = this._inactiveEndpoints.indexOf(socket.endpoint);
        if (index > -1) {
            this._inactiveEndpoints.splice(index, 1);
        }

        if (!this._affinityAwarenessActive &&
            this._getAllConnections().length >= 2) {
            this._affinityAwarenessActive = true;
        }
    }

    _removeConnection(socket) {
        if (socket.nodeUUID in this._connections) {
            delete this._connections[socket.nodeUUID];
            // Add the endpoint to _inactiveEndpoints
            this._inactiveEndpoints.push(socket.endpoint);
        }
        else if (this._legacyConnection == socket) {
            this._legacyConnection = null;
            // Add the endpoint to _inactiveEndpoints
            this._inactiveEndpoints.push(socket.endpoint);
        }

        if (this._affinityAwarenessActive &&
            this._getAllConnections().length < 2) {
            this._affinityAwarenessActive = false;
        }
    }

    async _onSocketDisconnect(socket, error = null) {
        this._removeConnection(socket);

        if (this._getAllConnections().length != 0) {
            // We had more than one connection before this disconnection
            this._runBackgroundConnect();
            return;
        }

        try {
            await this._reconnect();
        }
        catch (err) {
            this._cleanUp();
        }
    }

    /** Affinity Awareness methods */

    async _affinitySend(opCode, payloadWriter, payloadReader, affinityHint) {
        while (true) {
            const connection = await this._chooseConnection(affinityHint);
            Logger.logDebug('Endpoint chosen: ' + connection.endpoint);

            try {
                await connection.sendRequest(opCode, payloadWriter, payloadReader);
                return;
            }
            catch (err) {
                if (!(err instanceof Errors.LostConnectionError)) {
                    throw err;
                }

                Logger.logDebug(connection.endpoint + ' is unavailable');

                this._removeConnection(connection);

                if (this._getAllConnections().length == 0) {
                    throw new Errors.LostConnectionError("Cluster is unavailable");
                }
            }

            // Now we have to choose a connection randomly
            affinityHint = null;
        }
    }

    async _chooseConnection(affinityHint) {
        if (affinityHint === null) {
            Logger.logDebug('Node has been chosen randomly because no affinity hint passed');
            return this._getRandomConnection();
        }

        const cacheId = affinityHint.cacheId;

        if (!this._distributionMap.has(cacheId)) {
            Logger.logDebug('Distribution map does not have info for the cache ' + cacheId);
            Logger.logDebug('Node has been chosen randomly');
            // We are not awaiting here in order to not increase latency of requests
            this._getCachePartitions(cacheId);
            return this._getRandomConnection();
        }

        const cacheAffinityMap = this._distributionMap.get(cacheId);

        const key = affinityHint.key;
        const keyType = affinityHint.keyType ? affinityHint.keyType : BinaryUtils.calcObjectType(key);
        const nodeId = await this._determineNodeId(cacheAffinityMap, key, keyType);

        if (nodeId in this._connections) {
            Logger.logDebug('Node has been chosen by affinity');
            return this._connections[nodeId];
        }

        Logger.logDebug('Node has been chosen randomly');
        return this._getRandomConnection();
    }

    async _determineNodeId(cacheAffinityMap, key, keyType) {
        const partitionMap = cacheAffinityMap.partitionMapping;

        if (partitionMap.size == 0) {
            return null;
        }

        const keyAffinityMap = cacheAffinityMap.keyConfig;

        const convertedKey = await this._convertKey(key, keyType);
        key = convertedKey.key;
        const keyTypeCode = convertedKey.typeCode;

        let affinityKey = key;
        let affinityKeyTypeCode = keyTypeCode;

        if (keyAffinityMap.has(keyTypeCode)) {
            const affinityKeyId = keyAffinityMap.get(keyTypeCode);

            if (key instanceof BinaryObject &&
                key._fields.has(affinityKeyId)) {
                const field = key._fields.get(affinityKeyId);
                affinityKey = field.getValue();
                affinityKeyTypeCode = field.typeCode();
            }
        }

        const keyHash = await BinaryUtils.hashCode(affinityKey, this._communicator, affinityKeyTypeCode);
        const partition = AffinityAwarenessUtils.RendezvousAffinityFunction.calcPartition(keyHash, partitionMap.size);
        Logger.logDebug('Partition = ' + partition);

        const nodeId = partitionMap.get(partition);
        Logger.logDebug('Node ID = ' + nodeId);

        return nodeId;
    }

    async _convertKey(key, keyType) {
        let typeCode = BinaryUtils.getTypeCode(keyType);

        if (keyType instanceof ObjectType.ComplexObjectType) {
            key = await BinaryObject.fromObject(key, keyType);
            typeCode = BinaryUtils.TYPE_CODE.BINARY_OBJECT;
        }

        return {"key": key, "typeCode": typeCode};
    }

    async _onAffinityTopologyChange(newVersion) {
        // If affinityTopologyVer is null, we haven't requested cache partitions yet.
        // Hence we don't have caches to request partitions for
        if (!this._affinityAwarenessActive ||
            this._affinityTopologyVer === null ||
            this._affinityTopologyVer.compareTo(newVersion) >= 0) {
            return;
        }

        Logger.logDebug('New topology version reported...');

        await this._getCachePartitions();

        this._runBackgroundConnect();
    }

    async _getCachePartitions(newCache = null, tries = GET_CACHE_PARTITIONS_RETRIES) {
        if (tries <= 0) {
            return;
        }

        Logger.logDebug('Getting cache partitions info...');

        const knownCachesNum = this._distributionMap.size;
        if (knownCachesNum === 0 && newCache === null) {
            Logger.logDebug('No caches to get cache partitions for...');
            return;
        }

        try {
            await this.send(
                BinaryUtils.OPERATION.CACHE_PARTITIONS,
                async (payload) => {
                    if (newCache !== null) {
                        payload.writeInteger(knownCachesNum + 1);
                        payload.writeInteger(newCache);
                    }
                    else {
                        payload.writeInteger(knownCachesNum);
                    }

                    for (const cacheId of this._distributionMap.keys()) {
                        payload.writeInteger(cacheId);
                    }
                },
                this._handleCachePartitions.bind(this));
        }
        catch (err) {
            if (err instanceof Errors.LostConnectionError) {
                return;
            }

            // Retries in case of an error (most probably
            // "Getting affinity for topology version earlier than affinity is calculated")
            await this._sleep(GET_CACHE_PARTITIONS_DELAY);
            this._getCachePartitions(newCache, tries - 1);
        }
    }

    async _handleCachePartitions(payload) {
        const affinityTopologyVer = new AffinityAwarenessUtils.AffinityTopologyVersion(payload);
        const groupsNum = payload.readInteger();
        // {cacheId -> CacheAffinityMap}
        const distributionMap = new Map();

        for (let i = 0; i < groupsNum; i++) {
            const group = await AffinityAwarenessUtils.AffinityAwarenessCacheGroup.build(this._communicator, payload);
            // {partition -> nodeId}
            const partitionMapping = new Map();

            for (const [nodeId, partitions] of group.partitionMap) {
                for (const partition of partitions) {
                    partitionMapping.set(partition, nodeId);
                }
            }

            for (const [cacheId, config] of group.caches) {
                const cacheAffinityMap = new AffinityAwarenessUtils.CacheAffinityMap(cacheId, partitionMapping, config);
                distributionMap.set(cacheId, cacheAffinityMap);
            }
        }

        this._distributionMap = distributionMap;
        this._affinityTopologyVer = affinityTopologyVer;

        Logger.logDebug('Got cache partitions info');
        Logger.logDebug('Affinity topology version: ' + affinityTopologyVer);
    }

    _getRandomConnection() {
        const allConnections = this._getAllConnections();
        return allConnections[this._getRandomInt(allConnections.length)];
    }

    _changeState(state, reason = null) {
        if (Logger.debug) {
            Logger.logDebug(Util.format('Router state: %s -> %s'),
                this._getState(this._state),
                this._getState(state));
        }
        if (this._state !== state) {
            this._state = state;
            if (this._onStateChanged) {
                this._onStateChanged(state, reason);
            }
        }
    }

    _getState(state) {
        switch (state) {
            case IgniteClient.STATE.DISCONNECTED:
                return 'DISCONNECTED';
            case IgniteClient.STATE.CONNECTING:
                return 'CONNECTING';
            case IgniteClient.STATE.CONNECTED:
                return 'CONNECTED';
            default:
                return 'UNKNOWN';
        }
    }

    // Returns a random integer between 0 and max - 1
    _getRandomInt(max) {
        if (max === 0) {
            return 0;
        }
        return Math.floor(Math.random() * max);
    }

    _sleep(milliseconds) {
        return new Promise(resolve => setTimeout(resolve, milliseconds));
    }
}

module.exports = Router;
