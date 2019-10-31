/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
package org.apache.ignite.internal.processors.cache.local;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.commandline.CommandHandler.initLogger;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Class for testing rollback transaction with local cache.
 */
public class GridCacheLocalRollbackSelfTest extends GridCommonAbstractTest {
    /** Logger for listen log messages. */
    private static final ListeningTestLogger log = new ListeningTestLogger(false, GridAbstractTest.log);

    /** Number of nodes. */
    private static final int NODES = 4;

    /** Number of transactions. */
    private static final int TX_COUNT = 20;

    /** Creating a client node. */
    private boolean clientNode;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        log.clearListeners();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setClientMode(clientNode)
            .setCacheConfiguration(
                new CacheConfiguration(DEFAULT_CACHE_NAME)
                    .setAtomicityMode(TRANSACTIONAL)
                    .setBackups(3)
            )
            .setGridLogger(log)
            .setConnectorConfiguration(new ConnectorConfiguration());
    }

    /**
     * Test transaction rollback when one of the nodes drops out.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRollbackTransactions() throws Exception {
        String cacheName = DEFAULT_CACHE_NAME;

        int txCnt = TX_COUNT;

        int nodes = NODES;

        IgniteEx crd = createCluster(nodes);

        IgniteCache<Object, Object> cache = crd.cache(cacheName);

        List<Integer> keys = primaryKeys(cache, txCnt);

        Map<Integer, Integer> cacheValues = range(0, txCnt / 2).boxed().collect(toMap(keys::get, identity()));

        cache.putAll(cacheValues);

        Collection<Transaction> txs = createTxs(
            grid(nodes),
            cacheName,
            range(txCnt / 2, txCnt).mapToObj(keys::get).collect(toList())
        );

        int stoppedNodeId = 2;

        stopGrid(stoppedNodeId);

        LogListener logLsnr = new LogListener();

        log.registerListener(logLsnr);

        for (Transaction tx : txs)
            tx.rollback();

        awaitPartitionMapExchange();

        check(cacheValues, cacheName, logLsnr, stoppedNodeId);
    }

    /**
     * Test for rollback transactions when one of the nodes drops out,
     * with operations performed on keys outside the transaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRollbackTransactionsWithKeyOperationOutsideThem() throws Exception {
        String cacheName = DEFAULT_CACHE_NAME;

        int txCnt = TX_COUNT;

        int nodes = NODES;

        IgniteEx crd = createCluster(nodes);

        IgniteCache<Object, Object> cache = crd.cache(cacheName);

        List<Integer> keys = primaryKeys(cache, txCnt);

        Map<Integer, Integer> cacheValues = range(0, txCnt / 2).boxed().collect(toMap(keys::get, identity()));

        cache.putAll(cacheValues);

        List<Integer> txKeys = range(txCnt / 2, txCnt).mapToObj(keys::get).collect(toList());

        IgniteEx clientNode = grid(nodes);

        Collection<Transaction> txs = createTxs(clientNode, cacheName, txKeys);

        int stoppedNodeId = 2;

        stopGrid(stoppedNodeId);

        CountDownLatch latch = new CountDownLatch(1);

        GridTestUtils.runAsync(() -> {
            latch.countDown();

            IgniteCache<Object, Object> clientCache = clientNode.cache(DEFAULT_CACHE_NAME);

            txKeys.forEach(clientCache::get);
        });

        LogListener logLsnr = new LogListener();

        log.registerListener(logLsnr);

        latch.await();

        for (Transaction tx : txs)
            tx.rollback();

        awaitPartitionMapExchange();

        check(cacheValues, cacheName, logLsnr, stoppedNodeId);
    }

    /**
     * Checking the contents of the cache after rollback transactions,
     * with restarting the stopped node with using "idle_verify".
     *
     * @param cacheValues Expected cache contents.
     * @param cacheName Cache name.
     * @param logLsnr LogListener.
     * @param stoppedNodeId ID of the stopped node.
     * @throws Exception If failed.
     */
    private void check(
        Map<Integer, Integer> cacheValues,
        String cacheName,
        LogListener logLsnr,
        int stoppedNodeId
    ) throws Exception {
        assert nonNull(cacheValues);
        assert nonNull(cacheName);
        assert nonNull(logLsnr);

        checkCacheData(cacheValues, cacheName);

        Boolean correctEx = logLsnr.correctEx.get();
        assertTrue(nonNull(correctEx) && correctEx);

        startGrid(stoppedNodeId);

        awaitPartitionMapExchange();

        checkCacheData(cacheValues, cacheName);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        Logger cmdLog = createTestLogger(baos);
        CommandHandler cmdHnd = new CommandHandler(cmdLog);

        cmdHnd.execute(asList("--cache", "idle_verify"));

        stream(cmdLog.getHandlers()).forEach(Handler::flush);

        assertContains(log, baos.toString(), "no conflicts have been found");
    }

    /**
     * Creating a logger for a CommandHandler.
     *
     * @param outputStream Stream for recording the result of a command.
     * @return Logger.
     */
    private Logger createTestLogger(OutputStream outputStream) {
        assert nonNull(outputStream);

        Logger log = initLogger(null);

        log.addHandler(new StreamHandler(outputStream, new Formatter() {
            @Override public String format(LogRecord record) {
                return record.getMessage() + "\n";
            }
        }));

        return log;
    }

    /**
     * Creating a cluster.
     *
     * @param nodes Number of server nodes, plus one client.
     * @throws Exception If failed.
     */
    private IgniteEx createCluster(int nodes) throws Exception {
        IgniteEx crd = startGrids(nodes);

        clientNode = true;

        startGrid(nodes);

        awaitPartitionMapExchange();

        return crd;
    }

    /**
     * Transaction creation.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param keys Keys.
     * @return Transactions.
     * @throws Exception If failed.
     */
    private Collection<Transaction> createTxs(
        IgniteEx node,
        String cacheName,
        Collection<Integer> keys
    ) throws Exception {
        assert nonNull(node);
        assert nonNull(cacheName);
        assert nonNull(keys);

        IgniteCache<Object, Object> cache = node.cache(cacheName);

        Collection<Transaction> txs = new ArrayList<>();

        for (Integer key : keys) {
            Transaction tx = node.transactions().txStart();

            cache.put(key, key + 10);

            ((TransactionProxyImpl)tx).tx().prepare(true);

            txs.add(tx);
        }

        return txs;
    }

    /**
     * Class for searching and analyzing exceptions in the log.
     */
    private static class LogListener implements Consumer<String> {
        /**
         * Flag on finding only valid exceptions.
         */
        final AtomicReference<Boolean> correctEx = new AtomicReference<>();

        /**
         * Class to search for an exception "node left topology" in the log
         * with no TcpCommunicationSpi on the stack.
         */
        final String PREFIX_EX_MSG = "Unable to send message (node left topology):";

        /**
         * Search an exception in log message with a check for presence of
         * TcpCommunicationSpi in it.
         *
         * @param logStr Line from the log.
         */
        @Override public void accept(String logStr) {
            if (!logStr.contains(PREFIX_EX_MSG))
                return;

            if (logStr.contains(TcpCommunicationSpi.class.getName()))
                correctEx.set(false);
            else if (isNull(correctEx.get()))
                correctEx.set(true);
        }
    }
}
