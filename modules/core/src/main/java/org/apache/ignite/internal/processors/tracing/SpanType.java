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
    DISCOVERY_NODE_JOIN_REQUEST(Scope.DISCOVERY, "discovery.node.join.request", 1),

    /** Node join add. */
    DISCOVERY_NODE_JOIN_ADD(Scope.DISCOVERY, "discovery.node.join.add", 2),

    /** Node join finish. */
    DISCOVERY_NODE_JOIN_FINISH(Scope.DISCOVERY, "discovery.node.join.finish", 3),

    /** Node failed. */
    DISCOVERY_NODE_FAILED(Scope.DISCOVERY, "discovery.node.failed", 4),

    /** Node left. */
    DISCOVERY_NODE_LEFT(Scope.DISCOVERY, "discovery.node.left", 5),

    /** Custom event. */
    DISCOVERY_CUSTOM_EVENT(Scope.DISCOVERY, "discovery.custom.event", 6),

    // Exchange traces.
    /** Exchange future. */
    EXCHANGE_FUTURE(Scope.DISCOVERY, "exchange.future", 7),

    // Communication traces.
    /** Job execution request. */
    COMMUNICATION_JOB_EXECUTE_REQUEST(Scope.COMMUNICATION, "communication.job.execute.request", 8),

    /** Job execution response. */
    COMMUNICATION_JOB_EXECUTE_RESPONSE(Scope.COMMUNICATION, "communication.job.execute.response", 9),

    /** Socket write action. */
    COMMUNICATION_SOCKET_WRITE(Scope.COMMUNICATION, "socket.write", 10),

    /** Socket read action. */
    COMMUNICATION_SOCKET_READ(Scope.COMMUNICATION, "socket.read", 11),

    /** Process regular. */
    COMMUNICATION_REGULAR_PROCESS(Scope.COMMUNICATION, "process.regular", 12),

    /** Process ordered. */
    COMMUNICATION_ORDERED_PROCESS(Scope.COMMUNICATION, "process.ordered", 13),

    // Tx traces.
    /** Transaction start. */
    TX(Scope.TX, "transaction", 14),

    /** Transaction commit.*/
    TX_COMMIT(Scope.TX, "transactions.commit", 15),

    /** Transaction rollback.*/
    TX_ROLLBACK(Scope.TX, "transactions.rollback", 16),

    /** Transaction close.*/
    TX_CLOSE(Scope.TX, "transactions.close", 17),

    /** Transaction suspend.*/
    TX_SUSPEND(Scope.TX, "transactions.suspend", 18),

    /** Transaction resume.*/
    TX_RESUME(Scope.TX, "transactions.resume", 19),

    /** Transaction near prepare.*/
    TX_NEAR_PREPARE(Scope.TX, "transactions.near.prepare", 20),

    /** Transaction near prepare ondone.*/
    TX_NEAR_PREPARE_ON_DONE(Scope.TX, "transactions.near.prepare.ondone", 21),

    /** Transaction near prepare onerror.*/
    TX_NEAR_PREPARE_ON_ERROR(Scope.TX, "transactions.near.prepare.onerror", 22),

    /** Transaction near prepare ontimeout.*/
    TX_NEAR_PREPARE_ON_TIMEOUT(Scope.TX, "transactions.near.prepare.ontimeout", 23),

    /** Transaction dht prepare.*/
    TX_DHT_PREPARE(Scope.TX, "transactions.dht.prepare", 24),

    /** Transaction dht prepare ondone.*/
    TX_DHT_PREPARE_ON_DONE(Scope.TX, "transactions.dht.prepare.ondone", 25),

    /** Transaction near finish.*/
    TX_NEAR_FINISH(Scope.TX, "transactions.near.finish", 26),

    /** Transaction near finish ondone.*/
    TX_NEAR_FINISH_ON_DONE(Scope.TX, "transactions.near.finish.ondone", 27),

    /** Transaction dht finish.*/
    TX_DHT_FINISH(Scope.TX, "transactions.dht.finish", 28),

    /** Transaction dht finish ondone.*/
    TX_DHT_FINISH_ON_DONE(Scope.TX, "transactions.dht.finish.ondone", 29),

    /** Transaction map proceed.*/
    TX_MAP_PROCEED(Scope.TX, "transactions.lock.map.proceed", 30),

    /** Transaction map proceed.*/
    TX_COLOCATED_LOCK_MAP(Scope.TX, "transactions.colocated.lock.map", 31),

    /** Transaction lock map.*/
    TX_DHT_LOCK_MAP(Scope.TX, "transactions.dht.lock.map", 32),

    /** Transaction near enlist read.*/
    TX_NEAR_ENLIST_READ(Scope.TX, "transactions.near.enlist.read", 33),

    /** Transaction near enlist write.*/
    TX_NEAR_ENLIST_WRITE(Scope.TX, "transactions.near.enlist.write", 34),

    /** Transaction dht process prepare request.*/
    TX_PROCESS_DHT_PREPARE_REQ(Scope.TX, "tx.dht.process.prepare.req", 35),

    /** Transaction dht process finish request. */
    TX_PROCESS_DHT_FINISH_REQ(Scope.TX, "tx.dht.process.finish.req", 36),

    /** Transaction dht finish response. */
    TX_PROCESS_DHT_FINISH_RESP(Scope.TX, "tx.dht.process.finish.resp", 37),

    /** Transaction dht one phase commit ack request. */
    TX_PROCESS_DHT_ONE_PHASE_COMMIT_ACK_REQ(Scope.TX, "tx.dht.process.one-phase-commit-ack.req", 38),

    /** Transaction dht prepare response. */
    TX_PROCESS_DHT_PREPARE_RESP(Scope.TX, "tx.dht.process.prepare.response", 39),

    // Affinity
    /** Affinity calculation. */
    AFFINITY_CALCULATION(Scope.COMMUNICATION, "affinity.calculation", 40);

    /** Scope */
    private Scope scope;

    /** Trace name. */
    private String traceName;

    /** Index. */
    private int idx;

    /** Values. */
    private static final SpanType[] VALS;

    /**
     * Constructor.
     *
     * @param scope Scope.
     * @param traceName Trace name.
     * @param idx Index.
     */
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

    /**
     * @param idx Index.
     * @return Enum instance based on specified index.
     */
    public static SpanType fromIndex(int idx) {
        return idx < 0 || idx >= VALS.length ? null : VALS[idx];
    }
}
