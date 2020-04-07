/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.tracing;

/**
 * List of span type names used in appropriate sub-systems.
 */
public enum SpanType {
    // Discovery traces.
    /** Node join request. */
    DISCOVERY_NODE_JOIN_REQUEST(Scope.Discovery, "discovery.node.join.request", 1),

    /** Node join add. */
    DISCOVERY_NODE_JOIN_ADD(Scope.Discovery, "discovery.node.join.add", 2),

    /** Node join finish. */
    DISCOVERY_NODE_JOIN_FINISH(Scope.Discovery, "discovery.node.join.finish", 3),

    /** Node failed. */
    DISCOVERY_NODE_FAILED(Scope.Discovery, "discovery.node.failed", 4),

    /** Node left. */
    DISCOVERY_NODE_LEFT(Scope.Discovery, "discovery.node.left", 5),

    /** Custom event. */
    DISCOVERY_CUSTOM_EVENT(Scope.Discovery, "discovery.custom.event", 6),

    // Exchange traces.
    EXCHANGE_FUTURE(Scope.Discovery, "exchange.future", 7),

    // Communication traces.
    /** Job execution request. */
    COMMUNICATION_JOB_EXECUTE_REQUEST(Scope.Communication, "communication.job.execute.request", 8),

    /** Job execution response. */
    COMMUNICATION_JOB_EXECUTE_RESPONSE(Scope.Communication, "communication.job.execute.response", 9),

    /** Socket write action. */
    COMMUNICATION_SOCKET_WRITE(Scope.Communication, "socket.write", 10),

    /** Socket read action. */
    COMMUNICATION_SOCKET_READ(Scope.Communication, "socket.read", 11),

    /** Process regular. */
    COMMUNICATION_REGULAR_PROCESS(Scope.Communication, "process.regular", 12),

    /** Process ordered. */
    COMMUNICATION_ORDERED_PROCESS(Scope.Communication, "process.ordered", 13),

    // Tx traces.
    TX(Scope.TX, "transaction", 14),

    TX_COMMIT(Scope.TX, "transactions.commit", 15),

    TX_NEAR_PREPARE(Scope.TX, "transactions.near.prepare", 16),

    TX_NEAR_PREPARE_ON_DONE(Scope.TX, "transactions.near.prepare.ondone", 17),

    TX_NEAR_PREPARE_ON_ERROR(Scope.TX, "transactions.near.prepare.onerror", 18),

    TX_NEAR_PREPARE_ON_TIMEOUT(Scope.TX, "transactions.near.prepare.ontimeout", 18),

    TX_DHT_PREPARE(Scope.TX, "transactions.dht.prepare", 18),

    TX_DHT_PREPARE_ON_DONE(Scope.TX, "transactions.dht.prepare.ondone", 19),

    TX_NEAR_FINISH(Scope.TX, "transactions.near.finish", 20),

    TX_NEAR_FINISH_ON_DONE(Scope.TX, "transactions.near.finish.ondone", 21),

    TX_NEAR_FAST_FINISH(Scope.TX, "transactions.near.fast.finish", 22),

    TX_NEAR_FAST_FINISH_ON_DONE(Scope.TX, "transactions.near.fast.finish.ondone", 23),

    TX_NEAR_FINISH_AND_ACK(Scope.TX, "transactions.near.finishandack", 24),

    TX_DHT_FINISH(Scope.TX, "transactions.dht.finish", 25),

    TX_DHT_FINISH_ON_DONE(Scope.TX, "transactions.dht.finish.ondone", 26),

    TX_DHT_LOCAL(Scope.TX, "transactions.dht.local", 27),

    TX_MAP_PROCEED(Scope.TX, "transactions.lock.map.proceed", 30),

    TX_COLOCATED_LOCK_MAP(Scope.TX, "transactions.colocated.lock.map", 28),

    TX_DHT_LOCK_MAP(Scope.TX, "transactions.dht.lock.map", 31),

    TX_NEAR_ENLIST_READ(Scope.TX, "transactions.near.enlist.read", 32),

    TX_NEAR_ENLIST_WRITE(Scope.TX, "transactions.near.enlist.write", 33),

    CACHE_PUT(Scope.TX, "cache.put", 34),

    TX_PROCESS_DHT_PREPARE_REQ(Scope.TX, "tx.dht.process.prepare.req", 34),

    //TODO bellow
    TX_PROCESS_DHT_FINISH_REQ(Scope.TX, "tx.dht.process.prepare.req", 34),

    TX_PROCESS_DHT_FINISH_RESP(Scope.TX, "tx.dht.process.prepare.req", 34),

    TX_PROCESS_DHT_ONE_PHASE_COMMIT_ACK_REQ(Scope.TX, "tx.dht.process.prepare.req", 34),

    TX_PROCESS_DHT_PREPARE_RESP(Scope.TX, "tx.dht.process.prepare.req", 34),

    // OTHER
    // TODO Rework other with some specific trace scope.
    OTHER_AFFINITY_CALCULATION(Scope.OTHER, "affinity.calculation", 29);

    private Scope scope;

    private String traceName;

    private int idx;

    private static final SpanType[] VALS;

    SpanType(Scope scope, String traceName, int idx) {
        this.scope = scope;
        this.traceName = traceName;
        this.idx = idx;
    }

    /**
     * @return Scope.
     */
    public Scope scope() {
        return scope;
    }

    /**
     * @return Trace name.
     */
    public String traceName() {
        return traceName;
    }

    /**
     * @return idx.
     */
    public int idx() {
        return idx;
    }

    static {
        SpanType[] traces = SpanType.values();

        int maxIdx = 0;

        for (SpanType trace : traces)
            maxIdx = Math.max(maxIdx, trace.idx);

        VALS = new SpanType[maxIdx + 1];

        for (SpanType trace : traces)
            VALS[trace.idx] = trace;
    }

    public static SpanType fromIndex(int idx) {
        return idx < 0 || idx >= VALS.length ? null : VALS[idx];
    }
}
