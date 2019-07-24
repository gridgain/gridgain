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
package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TransactionMetricsMxBeanImpl;
import org.apache.ignite.internal.TransactionsMXBeanImpl;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.mxbean.TransactionMetricsMxBean;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.MessageOrderLogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_TRANSACTION_TIME_DUMP_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_TRANSACTION_TIME_DUMP_SAMPLE_LIMIT;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridTransactionsSystemUserTimeMetricsTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "test";

    /** */
    private static final String CLIENT = "client";

    /** */
    private static final String CLIENT_2 = CLIENT + "2";

    /** */
    private static final long USER_DELAY = 1000;

    /** */
    private static final long SYSTEM_DELAY = 1000;

    /** */
    private static final long LONG_TRAN_TIMEOUT = Math.min(SYSTEM_DELAY, USER_DELAY);

    /** */
    private static final long LONG_OP_TIMEOUT = 500;

    /** */
    private final LogListener logLsnr = new MessageOrderLogListener("Long transaction detected.*");

    /** */
    private static String longTranTimeoutCommon;

    /** */
    private static String longTranSampleLimit;

    /** */
    private static String longOpTimeoutCommon;

    /** */
    private volatile boolean slowPrepare;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        ListeningTestLogger testLog = new ListeningTestLogger(false, log());

        testLog.registerListener(logLsnr);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(testLog);

        boolean isClient = igniteInstanceName.contains(CLIENT);

        cfg.setClientMode(isClient);

        if (!isClient) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(1);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);

            cfg.setCacheConfiguration(ccfg);

        }

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        return cfg;
    }

    /**
     * Setting long op timeout to small value to make this tests faster
     */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        longTranTimeoutCommon = System.getProperty(IGNITE_LONG_TRANSACTION_TIME_DUMP_THRESHOLD);
        longTranSampleLimit = System.getProperty(IGNITE_LONG_TRANSACTION_TIME_DUMP_SAMPLE_LIMIT);
        longOpTimeoutCommon = System.getProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT);

        System.setProperty(IGNITE_LONG_TRANSACTION_TIME_DUMP_THRESHOLD, String.valueOf(LONG_TRAN_TIMEOUT));
        System.setProperty(IGNITE_LONG_TRANSACTION_TIME_DUMP_SAMPLE_LIMIT, String.valueOf(1.0f));
        System.setProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, String.valueOf(LONG_OP_TIMEOUT));
    }

    /**
     * Returning long operations timeout to its former value.
     */
    @Override protected void afterTestsStopped() throws Exception {
        if (longTranTimeoutCommon != null)
            System.setProperty(IGNITE_LONG_TRANSACTION_TIME_DUMP_THRESHOLD, longTranTimeoutCommon);
        else
            System.clearProperty(IGNITE_LONG_TRANSACTION_TIME_DUMP_THRESHOLD);

        if (longTranSampleLimit != null)
            System.setProperty(IGNITE_LONG_TRANSACTION_TIME_DUMP_SAMPLE_LIMIT, longTranSampleLimit);
        else
            System.clearProperty(IGNITE_LONG_TRANSACTION_TIME_DUMP_SAMPLE_LIMIT);

        if (longOpTimeoutCommon != null)
            System.setProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, longOpTimeoutCommon);
        else
            System.clearProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT);


        super.afterTestsStopped();
    }

    /** */
    @Test
    public void testTransactionsSystemUserTime() throws Exception {
        Ignite ignite = startGrids(2);

        Ignite client = startGrid(CLIENT);

        assertTrue(client.configuration().isClientMode());

        IgniteCache<Integer, Integer> cache = client.getOrCreateCache(CACHE_NAME);

        cache.put(1, 1);

        TransactionMetricsMxBean tmMxMetricsBean = getMxBean(
            CLIENT,
            "TransactionMetrics",
            TransactionMetricsMxBean.class,
            TransactionMetricsMxBeanImpl.class
        );

        //slow user
        slowPrepare = false;

        doInTransaction(client, () -> {
            Integer val = cache.get(1);

            doSleep(USER_DELAY);

            cache.put(1, val + 1);

            return null;
        });

        assertEquals(2, cache.get(1).intValue());

        assertTrue(tmMxMetricsBean.getTotalNodeUserTime() >= USER_DELAY);
        assertTrue(tmMxMetricsBean.getTotalNodeSystemTime() < LONG_TRAN_TIMEOUT);

        //slow prepare
        slowPrepare = true;

        doInTransaction(client, () -> {
            Integer val = cache.get(1);

            cache.put(1, val + 1);

            return null;
        });

        assertEquals(3, cache.get(1).intValue());

        assertTrue(tmMxMetricsBean.getTotalNodeSystemTime() >= SYSTEM_DELAY);

        String sysTimeHisto = tmMxMetricsBean.getNodeSystemTimeHistogram();
        String userTimeHisto = tmMxMetricsBean.getNodeUserTimeHistogram();

        assertNotNull(sysTimeHisto);
        assertNotNull(userTimeHisto);

        assertTrue(!sysTimeHisto.isEmpty());
        assertTrue(!userTimeHisto.isEmpty());

        logLsnr.reset();

        //checking settings changing via JMX with second client
        Ignite client2 = startGrid(CLIENT_2);

        TransactionsMXBean tmMxBean = getMxBean(
                CLIENT,
                "Transactions",
                TransactionsMXBean.class,
                TransactionsMXBeanImpl.class
        );

        tmMxBean.setLongTransactionTimeDumpThreshold(0);

        doInTransaction(client2, () -> {
            Integer val = cache.get(1);

            cache.put(1, val + 1);

            return null;
        });

        assertFalse(logLsnr.check());

        U.log(log, sysTimeHisto);
        U.log(log, userTimeHisto);
    }

    /**
     *
     */
    private class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                if (slowPrepare && msg0 instanceof GridNearTxPrepareRequest)
                    doSleep(SYSTEM_DELAY);
            }

            super.sendMessage(node, msg, ackClosure);
        }
    }
}
