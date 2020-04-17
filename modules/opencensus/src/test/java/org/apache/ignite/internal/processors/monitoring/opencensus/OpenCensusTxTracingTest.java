/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.monitoring.opencensus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.google.common.collect.ImmutableMap;
import io.opencensus.common.Functions;
import io.opencensus.exporter.trace.zipkin.ZipkinExporterConfiguration;
import io.opencensus.exporter.trace.zipkin.ZipkinTraceExporter;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Span;
import io.opencensus.trace.SpanId;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.export.SpanData;
import io.opencensus.trace.export.SpanExporter;
import io.opencensus.trace.samplers.Samplers;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.tracing.SpanType;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTraceExporter;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.opencensus.trace.AttributeValue.stringAttributeValue;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_COLOCATED_LOCK_MAP;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_COMMIT;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_DHT_FINISH;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_DHT_PREPARE;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_ENLIST_WRITE;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_FINISH;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_FINISH_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_FINISH_RESP;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_PREPARE;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_PREPARE_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_PREPARE_RESP;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_PROCESS_DHT_FINISH_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_PROCESS_DHT_PREPARE_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_PROCESS_DHT_PREPARE_RESP;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests to check correctness of OpenCensus Transactions Tracing implementation.
 */
public class OpenCensusTxTracingTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;
    /** Span buffer count - hardcode in open census. */
    private static final int SPAN_BUFFER_COUNT = 32;

    /** Test trace exporter handler. */
    private TraceExporterTestHandler hnd;

    /** Wrapper of test exporter handler. */
    private OpenCensusTraceExporter exporter;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        if (igniteInstanceName.contains("client"))
            cfg.setClientMode(true);

        cfg.setTracingSpi(new OpenCensusTracingSpi());

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setBackups(2);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     *
     */
    @BeforeClass
    public static void beforeTests() {
        /* Uncomment following code to see visualisation on local Zipkin: */

        ZipkinTraceExporter.createAndRegister(ZipkinExporterConfiguration.builder()
            .setV2Url("http://localhost:9411/api/v2/spans")
            .setServiceName("ignite")
            .build());
    }

    /**
     *
     */
    @Before
    public void before() throws Exception {
        stopAllGrids();

        hnd = new TraceExporterTestHandler();

        exporter = new OpenCensusTraceExporter(hnd);

        exporter.start("test");

        startGrids(GRID_CNT);
    }

    /**
     *
     */
    @After
    public void after() {
        exporter.stop();

        stopAllGrids();
    }

    /**
     * <ol>
     *     <li>Run pessimistic serializable transaction with some label.</li>
     *     <li>Call two puts inside the transaction.</li>
     *     <li>Commit given transaction.</li>
     * </ol>
     *
     * Check that got trace is equal to:
     *  transaction
     *      transactions.near.enlist.write
     *      transactions.colocated.lock.map
     *      transactions.near.enlist.write
     *      transactions.colocated.lock.map
     *      transactions.commit
     *          transactions.near.prepare
     *              tx.near.process.prepare.request
     *                  transactions.dht.prepare
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.near.process.prepare.response
     *              transactions.near.finish
     *                  tx.near.process.finish.request
     *                      transactions.dht.finish
     *                          tx.dht.process.finish.req
     *                          tx.dht.process.finish.req
     *                      tx.near.process.finish.response
     *
     *   <p>
     *   Also check that root transaction span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>concurrency</li>
     *       <li>isolation</li>
     *       <li>timeout</li>
     *       <li>label</li>
     *   </ol>
     *
     */
    @Test
    public void testPessimisticSerializableTxTracing() throws Exception {
        IgniteEx client = startGrid("client");

        Transaction tx = client.transactions().withLabel("label1").txStart(PESSIMISTIC, SERIALIZABLE);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);
        client.cache(DEFAULT_CACHE_NAME).put(2, 2);

        tx.commit();

        hnd.flush();

        List<SpanId> txSpanIds = checkSpan(
            TX,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("concurrency", PESSIMISTIC.name())
                .put("isolation", SERIALIZABLE.name())
                .put("timeout", String.valueOf(0))
                .put("label", "label1")
                .build()
        );

        checkSpan(
            TX_NEAR_ENLIST_WRITE,
            txSpanIds.get(0),
            2,
            null);

        checkSpan(
            TX_COLOCATED_LOCK_MAP,
            txSpanIds.get(0),
            2,
            null);

        List<SpanId> commitSpanIds = checkSpan(
            TX_COMMIT,
            txSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareSpanIds = checkSpan(
            TX_NEAR_PREPARE,
            commitSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareReqSpanIds = checkSpan(
            TX_NEAR_PREPARE_REQ,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareSpanIds = checkSpan(
            TX_DHT_PREPARE,
            txNearPrepareReqSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareReqSpanIds = checkSpan(
            TX_PROCESS_DHT_PREPARE_REQ,
            txDhtPrepareSpanIds.get(0),
            2,
            null);

        for (SpanId parentSpanId: txDhtPrepareReqSpanIds) {
            checkSpan(
                TX_PROCESS_DHT_PREPARE_RESP,
                parentSpanId,
                1,
                null);
        }

        checkSpan(
            TX_NEAR_PREPARE_RESP,
            txDhtPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishSpanIds = checkSpan(
            TX_NEAR_FINISH,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishReqSpanIds = checkSpan(
            TX_NEAR_FINISH_REQ,
            txNearFinishSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtFinishSpanIds = checkSpan(
            TX_DHT_FINISH,
            txNearFinishReqSpanIds.get(0),
            1,
            null);

        checkSpan(
            TX_PROCESS_DHT_FINISH_REQ,
            txDhtFinishSpanIds.get(0),
            2,
            null);

        checkSpan(
            TX_NEAR_FINISH_RESP,
            txNearFinishReqSpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run optimistic serializable transaction with some label.</li>
     *     <li>Call two puts inside the transaction.</li>
     *     <li>Commit given transaction.</li>
     * </ol>
     *
     * Check that got trace is equal to:
     *  transaction
     *      transactions.near.enlist.write
     *      transactions.near.enlist.write
     *      transactions.commit
     *          transactions.near.prepare
     *              tx.near.process.prepare.request
     *                  transactions.dht.prepare
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.near.process.prepare.response
     *              transactions.near.finish
     *                  tx.near.process.finish.request
     *                      transactions.dht.finish
     *                          tx.dht.process.finish.req
     *                          tx.dht.process.finish.req
     *                      tx.near.process.finish.response
     *
     *   <p>
     *   Also check that root transaction span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>concurrency</li>
     *       <li>isolation</li>
     *       <li>timeout</li>
     *       <li>label</li>
     *   </ol>
     *
     */
    @Test
    public void testOptimisticSerializableTxTracing() throws Exception {
        IgniteEx client = startGrid("client");

        Transaction tx = client.transactions().withLabel("label1").txStart(OPTIMISTIC, SERIALIZABLE);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);
        client.cache(DEFAULT_CACHE_NAME).put(2, 2);

        tx.commit();

        hnd.flush();

        List<SpanId> txSpanIds = checkSpan(
            TX,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("concurrency", OPTIMISTIC.name())
                .put("isolation", SERIALIZABLE.name())
                .put("timeout", String.valueOf(0))
                .put("label", "label1")
                .build()
        );

        checkSpan(
            TX_NEAR_ENLIST_WRITE,
            txSpanIds.get(0),
            2,
            null);

        List<SpanId> commitSpanIds = checkSpan(
            TX_COMMIT,
            txSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareSpanIds = checkSpan(
            TX_NEAR_PREPARE,
            commitSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareReqSpanIds = checkSpan(
            TX_NEAR_PREPARE_REQ,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareSpanIds = checkSpan(
            TX_DHT_PREPARE,
            txNearPrepareReqSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareReqSpanIds = checkSpan(
            TX_PROCESS_DHT_PREPARE_REQ,
            txDhtPrepareSpanIds.get(0),
            2,
            null);

        for (SpanId parentSpanId: txDhtPrepareReqSpanIds) {
            checkSpan(
                TX_PROCESS_DHT_PREPARE_RESP,
                parentSpanId,
                1,
                null);
        }

        checkSpan(
            TX_NEAR_PREPARE_RESP,
            txDhtPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishSpanIds = checkSpan(
            TX_NEAR_FINISH,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishReqSpanIds = checkSpan(
            TX_NEAR_FINISH_REQ,
            txNearFinishSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtFinishSpanIds = checkSpan(
            TX_DHT_FINISH,
            txNearFinishReqSpanIds.get(0),
            1,
            null);

        checkSpan(
            TX_PROCESS_DHT_FINISH_REQ,
            txDhtFinishSpanIds.get(0),
            2,
            null);

        checkSpan(
            TX_NEAR_FINISH_RESP,
            txNearFinishReqSpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run pessimistic read-committed transaction with some label.</li>
     *     <li>Call two puts inside the transaction.</li>
     *     <li>Commit given transaction.</li>
     * </ol>
     *
     * Check that got trace is equal to:
     *  transaction
     *      transactions.near.enlist.write
     *      transactions.colocated.lock.map
     *      transactions.near.enlist.write
     *      transactions.colocated.lock.map
     *      transactions.commit
     *          transactions.near.prepare
     *              tx.near.process.prepare.request
     *                  transactions.dht.prepare
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.near.process.prepare.response
     *              transactions.near.finish
     *                  tx.near.process.finish.request
     *                      transactions.dht.finish
     *                          tx.dht.process.finish.req
     *                          tx.dht.process.finish.req
     *                      tx.near.process.finish.response
     *
     *   <p>
     *   Also check that root transaction span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>concurrency</li>
     *       <li>isolation</li>
     *       <li>timeout</li>
     *       <li>label</li>
     *   </ol>
     *
     */
    @Test
    public void testPessimisticReadCommittedTxTracing() throws Exception {
        IgniteEx client = startGrid("client");

        Transaction tx = client.transactions().withLabel("label1").txStart(PESSIMISTIC, READ_COMMITTED);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);
        client.cache(DEFAULT_CACHE_NAME).put(2, 2);

        tx.commit();

        hnd.flush();

        List<SpanId> txSpanIds = checkSpan(
            TX,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("concurrency", PESSIMISTIC.name())
                .put("isolation", READ_COMMITTED.name())
                .put("timeout", String.valueOf(0))
                .put("label", "label1")
                .build()
        );

        checkSpan(
            TX_NEAR_ENLIST_WRITE,
            txSpanIds.get(0),
            2,
            null);

        checkSpan(
            TX_COLOCATED_LOCK_MAP,
            txSpanIds.get(0),
            2,
            null);

        List<SpanId> commitSpanIds = checkSpan(
            TX_COMMIT,
            txSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareSpanIds = checkSpan(
            TX_NEAR_PREPARE,
            commitSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareReqSpanIds = checkSpan(
            TX_NEAR_PREPARE_REQ,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareSpanIds = checkSpan(
            TX_DHT_PREPARE,
            txNearPrepareReqSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareReqSpanIds = checkSpan(
            TX_PROCESS_DHT_PREPARE_REQ,
            txDhtPrepareSpanIds.get(0),
            2,
            null);

        for (SpanId parentSpanId: txDhtPrepareReqSpanIds) {
            checkSpan(
                TX_PROCESS_DHT_PREPARE_RESP,
                parentSpanId,
                1,
                null);
        }

        checkSpan(
            TX_NEAR_PREPARE_RESP,
            txDhtPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishSpanIds = checkSpan(
            TX_NEAR_FINISH,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishReqSpanIds = checkSpan(
            TX_NEAR_FINISH_REQ,
            txNearFinishSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtFinishSpanIds = checkSpan(
            TX_DHT_FINISH,
            txNearFinishReqSpanIds.get(0),
            1,
            null);

        checkSpan(
            TX_PROCESS_DHT_FINISH_REQ,
            txDhtFinishSpanIds.get(0),
            2,
            null);

        checkSpan(
            TX_NEAR_FINISH_RESP,
            txNearFinishReqSpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run optimistic read-committed transaction with some label.</li>
     *     <li>Call two puts inside the transaction.</li>
     *     <li>Commit given transaction.</li>
     * </ol>
     *
     * Check that got trace is equal to:
     *  transaction
     *      transactions.near.enlist.write
     *      transactions.near.enlist.write
     *      transactions.commit
     *          transactions.near.prepare
     *              tx.near.process.prepare.request
     *                  transactions.dht.prepare
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.near.process.prepare.response
     *              transactions.near.finish
     *                  tx.near.process.finish.request
     *                      transactions.dht.finish
     *                          tx.dht.process.finish.req
     *                          tx.dht.process.finish.req
     *                      tx.near.process.finish.response
     *
     *   <p>
     *   Also check that root transaction span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>concurrency</li>
     *       <li>isolation</li>
     *       <li>timeout</li>
     *       <li>label</li>
     *   </ol>
     *
     */
    @Test
    public void testOptimisticReadCommittedTxTracing() throws Exception {
        IgniteEx client = startGrid("client");

        Transaction tx = client.transactions().withLabel("label1").txStart(OPTIMISTIC, READ_COMMITTED);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);
        client.cache(DEFAULT_CACHE_NAME).put(2, 2);

        tx.commit();

        hnd.flush();

        List<SpanId> txSpanIds = checkSpan(
            TX,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("concurrency", OPTIMISTIC.name())
                .put("isolation", READ_COMMITTED.name())
                .put("timeout", String.valueOf(0))
                .put("label", "label1")
                .build()
        );

        checkSpan(
            TX_NEAR_ENLIST_WRITE,
            txSpanIds.get(0),
            2,
            null);

        List<SpanId> commitSpanIds = checkSpan(
            TX_COMMIT,
            txSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareSpanIds = checkSpan(
            TX_NEAR_PREPARE,
            commitSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareReqSpanIds = checkSpan(
            TX_NEAR_PREPARE_REQ,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareSpanIds = checkSpan(
            TX_DHT_PREPARE,
            txNearPrepareReqSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareReqSpanIds = checkSpan(
            TX_PROCESS_DHT_PREPARE_REQ,
            txDhtPrepareSpanIds.get(0),
            2,
            null);

        for (SpanId parentSpanId: txDhtPrepareReqSpanIds) {
            checkSpan(
                TX_PROCESS_DHT_PREPARE_RESP,
                parentSpanId,
                1,
                null);
        }

        checkSpan(
            TX_NEAR_PREPARE_RESP,
            txDhtPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishSpanIds = checkSpan(
            TX_NEAR_FINISH,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishReqSpanIds = checkSpan(
            TX_NEAR_FINISH_REQ,
            txNearFinishSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtFinishSpanIds = checkSpan(
            TX_DHT_FINISH,
            txNearFinishReqSpanIds.get(0),
            1,
            null);

        checkSpan(
            TX_PROCESS_DHT_FINISH_REQ,
            txDhtFinishSpanIds.get(0),
            2,
            null);

        checkSpan(
            TX_NEAR_FINISH_RESP,
            txNearFinishReqSpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run pessimistic repeatable-read transaction with some label.</li>
     *     <li>Call two puts inside the transaction.</li>
     *     <li>Commit given transaction.</li>
     * </ol>
     *
     * Check that got trace is equal to:
     *  transaction
     *      transactions.near.enlist.write
     *      transactions.colocated.lock.map
     *      transactions.near.enlist.write
     *      transactions.colocated.lock.map
     *      transactions.commit
     *          transactions.near.prepare
     *              tx.near.process.prepare.request
     *                  transactions.dht.prepare
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.near.process.prepare.response
     *              transactions.near.finish
     *                  tx.near.process.finish.request
     *                      transactions.dht.finish
     *                          tx.dht.process.finish.req
     *                          tx.dht.process.finish.req
     *                      tx.near.process.finish.response
     *
     *   <p>
     *   Also check that root transaction span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>concurrency</li>
     *       <li>isolation</li>
     *       <li>timeout</li>
     *       <li>label</li>
     *   </ol>
     *
     */
    @Test
    public void testPessimisticRepeatableReadTxTracing() throws Exception {
        IgniteEx client = startGrid("client");

        Transaction tx = client.transactions().withLabel("label1").txStart(PESSIMISTIC, REPEATABLE_READ);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);
        client.cache(DEFAULT_CACHE_NAME).put(2, 2);

        tx.commit();

        hnd.flush();

        List<SpanId> txSpanIds = checkSpan(
            TX,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("concurrency", PESSIMISTIC.name())
                .put("isolation", REPEATABLE_READ.name())
                .put("timeout", String.valueOf(0))
                .put("label", "label1")
                .build()
        );

        checkSpan(
            TX_NEAR_ENLIST_WRITE,
            txSpanIds.get(0),
            2,
            null);

        checkSpan(
            TX_COLOCATED_LOCK_MAP,
            txSpanIds.get(0),
            2,
            null);

        List<SpanId> commitSpanIds = checkSpan(
            TX_COMMIT,
            txSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareSpanIds = checkSpan(
            TX_NEAR_PREPARE,
            commitSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareReqSpanIds = checkSpan(
            TX_NEAR_PREPARE_REQ,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareSpanIds = checkSpan(
            TX_DHT_PREPARE,
            txNearPrepareReqSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareReqSpanIds = checkSpan(
            TX_PROCESS_DHT_PREPARE_REQ,
            txDhtPrepareSpanIds.get(0),
            2,
            null);

        for (SpanId parentSpanId: txDhtPrepareReqSpanIds) {
            checkSpan(
                TX_PROCESS_DHT_PREPARE_RESP,
                parentSpanId,
                1,
                null);
        }

        checkSpan(
            TX_NEAR_PREPARE_RESP,
            txDhtPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishSpanIds = checkSpan(
            TX_NEAR_FINISH,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishReqSpanIds = checkSpan(
            TX_NEAR_FINISH_REQ,
            txNearFinishSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtFinishSpanIds = checkSpan(
            TX_DHT_FINISH,
            txNearFinishReqSpanIds.get(0),
            1,
            null);

        checkSpan(
            TX_PROCESS_DHT_FINISH_REQ,
            txDhtFinishSpanIds.get(0),
            2,
            null);

        checkSpan(
            TX_NEAR_FINISH_RESP,
            txNearFinishReqSpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run optimistic repeatable-read transaction with some label.</li>
     *     <li>Call two puts inside the transaction.</li>
     *     <li>Commit given transaction.</li>
     * </ol>
     *
     * Check that got trace is equal to:
     *  transaction
     *      transactions.near.enlist.write
     *      transactions.near.enlist.write
     *      transactions.commit
     *          transactions.near.prepare
     *              tx.near.process.prepare.request
     *                  transactions.dht.prepare
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.dht.process.prepare.req
     *                          tx.dht.process.prepare.response
     *                      tx.near.process.prepare.response
     *              transactions.near.finish
     *                  tx.near.process.finish.request
     *                      transactions.dht.finish
     *                          tx.dht.process.finish.req
     *                          tx.dht.process.finish.req
     *                      tx.near.process.finish.response
     *
     *   <p>
     *   Also check that root transaction span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>concurrency</li>
     *       <li>isolation</li>
     *       <li>timeout</li>
     *       <li>label</li>
     *   </ol>
     *
     */
    @Test
    public void testOptimisticRepeatableReadTxTracing() throws Exception {
        IgniteEx client = startGrid("client");

        Transaction tx = client.transactions().withLabel("label1").txStart(OPTIMISTIC, REPEATABLE_READ);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);
        client.cache(DEFAULT_CACHE_NAME).put(2, 2);

        tx.commit();

        hnd.flush();

        List<SpanId> txSpanIds = checkSpan(
            TX,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("concurrency", OPTIMISTIC.name())
                .put("isolation", REPEATABLE_READ.name())
                .put("timeout", String.valueOf(0))
                .put("label", "label1")
                .build()
        );

        checkSpan(
            TX_NEAR_ENLIST_WRITE,
            txSpanIds.get(0),
            2,
            null);

        List<SpanId> commitSpanIds = checkSpan(
            TX_COMMIT,
            txSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareSpanIds = checkSpan(
            TX_NEAR_PREPARE,
            commitSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearPrepareReqSpanIds = checkSpan(
            TX_NEAR_PREPARE_REQ,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareSpanIds = checkSpan(
            TX_DHT_PREPARE,
            txNearPrepareReqSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtPrepareReqSpanIds = checkSpan(
            TX_PROCESS_DHT_PREPARE_REQ,
            txDhtPrepareSpanIds.get(0),
            2,
            null);

        for (SpanId parentSpanId: txDhtPrepareReqSpanIds) {
            checkSpan(
                TX_PROCESS_DHT_PREPARE_RESP,
                parentSpanId,
                1,
                null);
        }

        checkSpan(
            TX_NEAR_PREPARE_RESP,
            txDhtPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishSpanIds = checkSpan(
            TX_NEAR_FINISH,
            txNearPrepareSpanIds.get(0),
            1,
            null);

        List<SpanId> txNearFinishReqSpanIds = checkSpan(
            TX_NEAR_FINISH_REQ,
            txNearFinishSpanIds.get(0),
            1,
            null);

        List<SpanId> txDhtFinishSpanIds = checkSpan(
            TX_DHT_FINISH,
            txNearFinishReqSpanIds.get(0),
            1,
            null);

        checkSpan(
            TX_PROCESS_DHT_FINISH_REQ,
            txDhtFinishSpanIds.get(0),
            2,
            null);

        checkSpan(
            TX_NEAR_FINISH_RESP,
            txNearFinishReqSpanIds.get(0),
            1,
            null);
    }

    /**
     * Verify that given spanData contains all (and only) propagated expected attributes.
     * @param spanData Span data to check.
     * @param expAttrs Attributes to check.
     */
    private void checkSpanAttributes(SpanData spanData, /** tagName: tagValue*/ Map<String, String> expAttrs) {
        Map<String, AttributeValue> attrs = spanData.getAttributes().getAttributeMap();

        if (expAttrs != null) {
            assertEquals(expAttrs.size(), attrs.size());

            for (Map.Entry<String, String> entry : expAttrs.entrySet())
                assertEquals(entry.getValue(), attributeValueToString(attrs.get(entry.getKey())));
        }
    }

    /**
     * Check span.
     *
     * @param spanType Span type.
     * @param parentSpanId Parent span id.
     * @param expSpansCnt expected spans count.
     * @param expAttrs Attributes to check.
     * @return
     */
    private List<SpanId> checkSpan(
        SpanType spanType,
        SpanId parentSpanId,
        int expSpansCnt,
        /** tagName: tagValue*/ Map<String, String> expAttrs
    ) {
        List<SpanData> gotSpans = hnd.allSpans()
            .filter(
                span -> parentSpanId != null ?
                    parentSpanId.equals(span.getParentSpanId()) && spanType.traceName().equals(span.getName()) :
                    spanType.traceName().equals(span.getName()))
            .collect(Collectors.toList());

        assertEquals(expSpansCnt, gotSpans.size());

        List<SpanId> spanIds = new ArrayList<>();

        gotSpans.forEach(spanData -> {
            spanIds.add(spanData.getContext().getSpanId());

            checkSpanAttributes(spanData, expAttrs);
        });

        return spanIds;
    }

    /**
     * @param attributeVal Attribute value.
     */
    private static String attributeValueToString(AttributeValue attributeVal) {
        return attributeVal.match(
            Functions.returnToString(),
            Functions.returnToString(),
            Functions.returnToString(),
            Functions.returnToString(),
            Functions.returnConstant(""));
    }

    /**
     * Test span exporter handler.
     */
    static class TraceExporterTestHandler extends SpanExporter.Handler {
        /** Collected spans. */
        private final Map<SpanId, SpanData> collectedSpans = new ConcurrentHashMap<>();
        /** */
        private final Map<SpanId, List<SpanData>> collectedSpansByParents = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public void export(Collection<SpanData> spanDataList) {
            for (SpanData data : spanDataList) {
                collectedSpans.put(data.getContext().getSpanId(), data);

                if (data.getParentSpanId() != null)
                    collectedSpansByParents.computeIfAbsent(data.getParentSpanId(), (k) -> new ArrayList<>()).add(data);
            }
        }

        /**
         * @return Stream of all exported spans.
         */
        public Stream<SpanData> allSpans() {
            return collectedSpans.values().stream();
        }

        /**
         * @param id Span id.
         * @return Exported span by given id.
         */
        public SpanData spanById(SpanId id) {
            return collectedSpans.get(id);
        }

        /**
         * @param name Span name for search.
         * @return Span with given name.
         */
        public SpanData spanByName(String name) {
            return allSpans()
                .filter(span -> span.getName().contains(name))
                .findFirst()
                .orElse(null);
        }

        /**
         * @param parentId Parent id.
         * @return All spans by parent id.
         */
        public List<SpanData> spanByParentId(SpanId parentId) {
            return collectedSpansByParents.get(parentId);
        }

        /**
         * @param parentSpan Top span.
         * @return All span which are childs of parentSpan in any generation.
         */
        public List<SpanData> unrollByParent(SpanData parentSpan) {
            ArrayList<SpanData> spanChain = new ArrayList<>();

            LinkedList<SpanData> queue = new LinkedList<>();

            queue.add(parentSpan);

            spanChain.add(parentSpan);

            while (!queue.isEmpty()) {
                SpanData cur = queue.pollFirst();

                List<SpanData> child = spanByParentId(cur.getContext().getSpanId());

                if (child != null) {
                    spanChain.addAll(child);

                    queue.addAll(child);
                }
            }

            return spanChain;
        }

        public Stream<SpanData> spansReportedByNode(String igniteInstanceName) {
            return collectedSpans.values().stream()
                .filter(spanData -> stringAttributeValue(igniteInstanceName)
                    .equals(spanData.getAttributes().getAttributeMap().get("node.name")));
        }

        /**
         * Forces to flush ended spans that not passed to exporter yet.
         */
        public void flush() throws IgniteInterruptedCheckedException {
            // There is hardcoded invariant, that ended spans will be passed to exporter in 2 cases:
            // By 5 seconds timeout and if buffer size exceeds 32 spans.
            // There is no ability to change this behavior in Opencensus, so this hack is needed to "flush" real spans to exporter.
            // @see io.opencensus.implcore.trace.export.ExportComponentImpl.
            for (int i = 0; i < SPAN_BUFFER_COUNT; i++) {
                Span span = Tracing.getTracer().spanBuilder("test-" + i).setSampler(Samplers.alwaysSample()).startSpan();

                U.sleep(10); // See same hack in OpenCensusSpanAdapter#end() method.

                span.end();
            }
        }
    }
}
