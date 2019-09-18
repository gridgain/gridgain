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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.DynamicMBean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TransactionsMXBeanImpl;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxAdapter;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.MessageOrderLogListener;
import org.apache.ignite.testframework.junits.SystemPropertiesList;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_TRANSACTION_TIME_DUMP_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_COEFFICIENT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_PER_SECOND_LIMIT;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.transactions.TransactionMetricsAdapter.METRIC_SYSTEM_TIME_HISTOGRAM;
import static org.apache.ignite.internal.processors.cache.transactions.TransactionMetricsAdapter.METRIC_TOTAL_SYSTEM_TIME;
import static org.apache.ignite.internal.processors.cache.transactions.TransactionMetricsAdapter.METRIC_TOTAL_USER_TIME;
import static org.apache.ignite.internal.processors.cache.transactions.TransactionMetricsAdapter.METRIC_USER_TIME_HISTOGRAM;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.TX_METRICS;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
@SystemPropertiesList(value = {
    @WithSystemProperty(key = IGNITE_LONG_TRANSACTION_TIME_DUMP_THRESHOLD, value = "999"),
    @WithSystemProperty(key = IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_COEFFICIENT, value = "1.0"),
    @WithSystemProperty(key = IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_PER_SECOND_LIMIT, value = "5"),
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = "500")
})
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
    private static final long EPSILON = 300;

    /** */
    private static final int TX_SKIPPED_WHEN_LOG_THROTTLING_CHECK = 2;

    /** */
    private static final long LONG_TRAN_TIMEOUT = Math.min(SYSTEM_DELAY, USER_DELAY);

    /** */
    private static final String TRANSACTION_TIME_DUMP_REGEX = ".*?ransaction time dump .*?totalTime=[0-9]{1,4}, " +
            "systemTime=[0-9]{1,4}, userTime=[0-9]{1,4}, cacheOperationsTime=[0-9]{1,4}.*";

    /** */
    private static final String ROLLBACK_TIME_DUMP_REGEX =
        ".*?Long transaction time dump .*?cacheOperationsTime=[0-9]{1,4}.*?rollbackTime=[0-9]{1,4}.*";

    /** */
    private LogListener logTxDumpLsnr = new MessageOrderLogListener(TRANSACTION_TIME_DUMP_REGEX);

    /** */
    private TransactionDumpListener transactionDumpLsnr = new TransactionDumpListener(TRANSACTION_TIME_DUMP_REGEX);

    /** */
    private LogListener rollbackDumpLsnr = new MessageOrderLogListener(ROLLBACK_TIME_DUMP_REGEX);

    /** */
    private static CommonLogProxy testLog = new CommonLogProxy(null);

    /** */
    private final ListeningTestLogger listeningTestLog = new ListeningTestLogger(false, log());

    /** */
    private static IgniteLogger oldLog;

    /** */
    private volatile boolean slowSystem;

    /** */
    private volatile boolean simulateFailure;

    /** */
    private static boolean gridStarted = false;

    /** */
    private Ignite client;

    /** */
    private IgniteCache<Integer, Integer> cache;

    /** */
    private Callable<Object> txCallable = () -> {
        Integer val = cache.get(1);

        cache.put(1, val + 1);

        return null;
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
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

        cfg.setMetricExporterSpi(new JmxMetricExporterSpi());

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        return cfg;
    }

    /** */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        oldLog = GridTestUtils.getFieldValue(IgniteTxAdapter.class, "log");

        GridTestUtils.setFieldValue(IgniteTxAdapter.class, "log", testLog);
    }

    /** */
    @Override protected void afterTestsStopped() throws Exception {
        GridTestUtils.setFieldValue(IgniteTxAdapter.class, "log", oldLog);

        oldLog = null;

        stopAllGrids();

        super.afterTestsStopped();
    }

    /** */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        testLog.setImpl(listeningTestLog);

        listeningTestLog.registerListener(logTxDumpLsnr);
        listeningTestLog.registerListener(transactionDumpLsnr);
        listeningTestLog.registerListener(rollbackDumpLsnr);

        if (!gridStarted) {
            startGrids(2);

            gridStarted = true;
        }

        client = startGrid(CLIENT);

        cache = client.getOrCreateCache(CACHE_NAME);

        cache.put(1, 1);

        applyJmxParameters(1000L, 0.0, 5);
    }

    /** */
    @Override protected void afterTest() throws Exception {
        stopGrid(CLIENT);

        super.afterTest();
    }

    /** */
    private TransactionsMXBean applyJmxParameters(Long threshold, Double coefficient, Integer limit) throws Exception {
        TransactionsMXBean tmMxBean = getMxBean(
            CLIENT,
            "Transactions",
            TransactionsMXBean.class,
            TransactionsMXBeanImpl.class
        );

        if (threshold != null)
            tmMxBean.setLongTransactionTimeDumpThreshold(threshold);

        if (coefficient != null)
            tmMxBean.setTransactionTimeDumpSamplesCoefficient(coefficient);

        if (limit != null)
            tmMxBean.setTransactionTimeDumpSamplesPerSecondLimit(limit);

        return tmMxBean;
    }

    /** */
    private void doAsyncTransactions(Ignite client, int txCount, long userDelay) throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(txCount);

        List<Future> futures = new LinkedList<>();

        for (int i = 0; i < txCount; i++) {
            futures.add(executorService.submit(() -> {
                try {
                    doInTransaction(client, () -> {
                        doSleep(userDelay);

                        txCallable.call();

                        return null;
                    });
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        executorService.shutdown();
    }

    /** */
    private void doTransaction(Ignite client, boolean systemDelay, boolean userDelay, TxTestMode mode) throws Exception {
        if (systemDelay)
            slowSystem = true;

        if (mode == TxTestMode.FAIL)
            simulateFailure = true;

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            if (userDelay)
                doSleep(USER_DELAY);

            txCallable.call();

            if (mode == TxTestMode.ROLLBACK)
                tx.rollback();
            else
                tx.commit();
        }

        slowSystem = false;
        simulateFailure = false;
    }

    /** */
    private ClientTxTestResult measureClientTransaction(boolean systemDelay, boolean userDelay, TxTestMode mode) throws Exception {
        logTxDumpLsnr.reset();
        rollbackDumpLsnr.reset();

        long startTime = System.currentTimeMillis();

        try {
            doTransaction(client, systemDelay, userDelay, mode);
        }
        catch (Exception e) {
            // Giving a time for transaction to rollback.
            doSleep(500);
        }

        long completionTime = System.currentTimeMillis();

        ClientTxTestResult res = new ClientTxTestResult(startTime, completionTime, metricSet(CLIENT, null, TX_METRICS));

        return res;
    }

    /** */
    private void assertNotEmpty(long[] histogram) {
        assertNotNull(histogram);

        assertTrue(histogram.length > 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUserDelayOnCommittedTx() throws Exception {
        ClientTxTestResult res = measureClientTransaction(false, true, TxTestMode.COMMIT);

        assertTrue(logTxDumpLsnr.check());

        long userTime = (Long)res.mBean.getAttribute(METRIC_TOTAL_USER_TIME);
        long systemTime = (Long)res.mBean.getAttribute(METRIC_TOTAL_SYSTEM_TIME);

        assertTrue(userTime >= USER_DELAY);
        assertTrue(userTime < res.completionTime - res.startTime - systemTime + EPSILON);
        assertTrue(systemTime >= 0);
        assertTrue(systemTime < EPSILON);

        assertNotEmpty((long[])res.mBean.getAttribute(METRIC_SYSTEM_TIME_HISTOGRAM));
        assertNotEmpty((long[])res.mBean.getAttribute(METRIC_USER_TIME_HISTOGRAM));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUserDelayOnRolledBackTx() throws Exception {
        ClientTxTestResult res = measureClientTransaction(false, true, TxTestMode.ROLLBACK);

        assertTrue(rollbackDumpLsnr.check());

        long userTime = (Long)res.mBean.getAttribute(METRIC_TOTAL_USER_TIME);
        long systemTime = (Long)res.mBean.getAttribute(METRIC_TOTAL_SYSTEM_TIME);

        assertTrue(userTime >= USER_DELAY);
        assertTrue(userTime < res.completionTime - res.startTime - systemTime + EPSILON);
        assertTrue(systemTime >= 0);
        assertTrue(systemTime < EPSILON);

        assertNotEmpty((long[])res.mBean.getAttribute(METRIC_SYSTEM_TIME_HISTOGRAM));
        assertNotEmpty((long[])res.mBean.getAttribute(METRIC_USER_TIME_HISTOGRAM));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUserDelayOnFailedTx() throws Exception {
        ClientTxTestResult res = measureClientTransaction(false, true, TxTestMode.FAIL);

        assertTrue(rollbackDumpLsnr.check());

        long userTime = (Long)res.mBean.getAttribute(METRIC_TOTAL_USER_TIME);
        long systemTime = (Long)res.mBean.getAttribute(METRIC_TOTAL_SYSTEM_TIME);

        assertTrue(userTime >= USER_DELAY);
        assertTrue(userTime < res.completionTime - res.startTime - systemTime + EPSILON);
        assertTrue(systemTime >= 0);
        assertTrue(systemTime < EPSILON);

        assertNotEmpty((long[])res.mBean.getAttribute(METRIC_SYSTEM_TIME_HISTOGRAM));
        assertNotEmpty((long[])res.mBean.getAttribute(METRIC_USER_TIME_HISTOGRAM));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSystemDelayOnCommittedTx() throws Exception {
        ClientTxTestResult res = measureClientTransaction(true, false, TxTestMode.COMMIT);

        assertTrue(logTxDumpLsnr.check());

        long userTime = (Long)res.mBean.getAttribute(METRIC_TOTAL_USER_TIME);
        long systemTime = (Long)res.mBean.getAttribute(METRIC_TOTAL_SYSTEM_TIME);

        assertTrue(userTime >= 0);
        assertTrue(userTime < EPSILON);
        assertTrue(systemTime >= SYSTEM_DELAY);
        assertTrue(systemTime < res.completionTime - res.startTime - userTime + EPSILON);

        assertNotEmpty((long[])res.mBean.getAttribute(METRIC_SYSTEM_TIME_HISTOGRAM));
        assertNotEmpty((long[])res.mBean.getAttribute(METRIC_USER_TIME_HISTOGRAM));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSystemDelayOnRolledBackTx() throws Exception {
        ClientTxTestResult res = measureClientTransaction(true, false, TxTestMode.ROLLBACK);

        assertTrue(rollbackDumpLsnr.check());

        long userTime = (Long)res.mBean.getAttribute(METRIC_TOTAL_USER_TIME);
        long systemTime = (Long)res.mBean.getAttribute(METRIC_TOTAL_SYSTEM_TIME);

        assertTrue(userTime >= 0);
        assertTrue(userTime < EPSILON);
        assertTrue(systemTime >= SYSTEM_DELAY);
        assertTrue(systemTime < res.completionTime - res.startTime - userTime + EPSILON);

        assertNotEmpty((long[])res.mBean.getAttribute(METRIC_SYSTEM_TIME_HISTOGRAM));
        assertNotEmpty((long[])res.mBean.getAttribute(METRIC_USER_TIME_HISTOGRAM));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSystemDelayOnFailedTx() throws Exception {
        ClientTxTestResult res = measureClientTransaction(true, false, TxTestMode.FAIL);

        assertTrue(rollbackDumpLsnr.check());

        long userTime = (Long)res.mBean.getAttribute(METRIC_TOTAL_USER_TIME);
        long systemTime = (Long)res.mBean.getAttribute(METRIC_TOTAL_SYSTEM_TIME);

        assertTrue(userTime >= 0);
        assertTrue(userTime < EPSILON);
        assertTrue(systemTime >= SYSTEM_DELAY);
        assertTrue(systemTime < res.completionTime - res.startTime - userTime + EPSILON);

        assertNotEmpty((long[])res.mBean.getAttribute(METRIC_SYSTEM_TIME_HISTOGRAM));
        assertNotEmpty((long[])res.mBean.getAttribute(METRIC_USER_TIME_HISTOGRAM));
    }

    /** */
    @Test
    public void testJmxParametersSpreading() throws Exception {
        startGrid(CLIENT_2);

        try {
            TransactionsMXBean tmMxBean = getMxBean(
                CLIENT,
                "Transactions",
                TransactionsMXBean.class,
                TransactionsMXBeanImpl.class
            );

            TransactionsMXBean tmMxBean2 = getMxBean(
                CLIENT_2,
                "Transactions",
                TransactionsMXBean.class,
                TransactionsMXBeanImpl.class
            );

            int oldLimit = tmMxBean.getTransactionTimeDumpSamplesPerSecondLimit();
            long oldThreshold = tmMxBean.getLongTransactionTimeDumpThreshold();
            double oldCoefficient = tmMxBean.getTransactionTimeDumpSamplesCoefficient();

            try {
                int newLimit = 1234;
                long newThreshold = 99999;
                double newCoefficient = 0.01;

                tmMxBean.setTransactionTimeDumpSamplesPerSecondLimit(newLimit);
                tmMxBean2.setLongTransactionTimeDumpThreshold(newThreshold);
                tmMxBean.setTransactionTimeDumpSamplesCoefficient(newCoefficient);

                assertEquals(newLimit, tmMxBean2.getTransactionTimeDumpSamplesPerSecondLimit());
                assertEquals(newThreshold, tmMxBean.getLongTransactionTimeDumpThreshold());
                assertTrue(tmMxBean2.getTransactionTimeDumpSamplesCoefficient() - newCoefficient < 0.0001);
            }
            finally {
                tmMxBean.setTransactionTimeDumpSamplesPerSecondLimit(oldLimit);
                tmMxBean.setLongTransactionTimeDumpThreshold(oldThreshold);
                tmMxBean.setTransactionTimeDumpSamplesCoefficient(oldCoefficient);
            }
        }
        finally {
            // CLIENT grid is stopped in afterTest.
            stopGrid(CLIENT_2);
        }
    }

    /** */
    @Test
    public void testLongTransactionDumpLimit() throws Exception {
        logTxDumpLsnr.reset();
        transactionDumpLsnr.reset();

        int txCnt = 10;

        applyJmxParameters(5000L, null, txCnt);

        doAsyncTransactions(client, txCnt, 5200);

        doSleep(3000);

        assertFalse(logTxDumpLsnr.check());

        doSleep(3000);

        assertTrue(logTxDumpLsnr.check());
        assertTrue(transactionDumpLsnr.check());

        assertEquals(txCnt, transactionDumpLsnr.value());
    }

    /** */
    @Test
    public void testSamplingCoefficient() throws Exception {
        logTxDumpLsnr.reset();
        transactionDumpLsnr.reset();

        int txCnt = 10;

        applyJmxParameters(null, 1.0, txCnt);

        // Wait for a second to reset hit counter.
        doSleep(1000);

        for (int i = 0; i < txCnt; i++)
            doInTransaction(client, txCallable);

        assertTrue(logTxDumpLsnr.check());
        assertTrue(transactionDumpLsnr.check());

        assertEquals(txCnt, transactionDumpLsnr.value());
    }

    /** */
    @Test
    public void testNoSamplingCoefficient() throws Exception {
        logTxDumpLsnr.reset();

        applyJmxParameters(null, 0.0, 10);

        int txCnt = 10;

        for (int i = 0; i < txCnt; i++)
            doInTransaction(client, txCallable);

        assertFalse(logTxDumpLsnr.check());
    }

    /** */
    @Test
    public void testSamplingLimit() throws Exception {
        logTxDumpLsnr.reset();
        transactionDumpLsnr.reset();

        int txCnt = 10;
        int txDumpCnt = 2;

        LogListener transactionDumpsSkippedLsnr = LogListener
                .matches("Transaction time dumps skipped because of log throttling: " + (txCnt - txDumpCnt))
                .build();

        listeningTestLog.registerListener(transactionDumpsSkippedLsnr);

        applyJmxParameters(null, 1.0, txDumpCnt);

        // Wait for a second to reset hit counter.
        doSleep(1000);

        for (int i = 0; i < txCnt; i++)
            doInTransaction(client, txCallable);

        // Wait for a second to reset hit counter.
        doSleep(1000);

        // One more sample to print information about skipped previous samples.
        doInTransaction(client, txCallable);

        assertTrue(logTxDumpLsnr.check());
        assertTrue(transactionDumpLsnr.check());
        assertTrue(transactionDumpsSkippedLsnr.check());

        assertEquals(txDumpCnt + 1, transactionDumpLsnr.value());
    }

    /** */
    @Test
    public void testSamplingNoThreshold() throws Exception {
        logTxDumpLsnr.reset();
        transactionDumpLsnr.reset();

        int txCnt = 10;

        applyJmxParameters(0L, 1.0, txCnt);

        // Wait for a second to reset hit counter.
        doSleep(1000);

        for (int i = 0; i < txCnt; i++)
            doInTransaction(client, txCallable);

        assertTrue(logTxDumpLsnr.check());
        assertTrue(transactionDumpLsnr.check());

        assertEquals(txCnt, transactionDumpLsnr.value());
    }

    /** */
    @Test
    public void testSamplingNoThresholdWithLimit() throws Exception {
        logTxDumpLsnr.reset();

        int txCnt = 10;

        applyJmxParameters(0L, 0.0, 5);

        for (int i = 0; i < txCnt; i++)
            doInTransaction(client, txCallable);

        assertFalse(logTxDumpLsnr.check());
    }

    /** */
    /*@Test
    public void testTransactionsSystemUserTime() throws Exception {
        Ignite ignite = startGrids(2);

        Ignite client = startGrid(CLIENT);

        IgniteLogger oldLog = GridTestUtils.getFieldValue(IgniteTxAdapter.class, "log");

        GridTestUtils.setFieldValue(IgniteTxAdapter.class, "log", testLog);

        try {
            assertTrue(client.configuration().isClientMode());

            IgniteCache<Integer, Integer> cache = client.getOrCreateCache(CACHE_NAME);

            cache.put(1, 1);

            Callable<Object> txCallable = () -> {
                Integer val = cache.get(1);

                cache.put(1, val + 1);

                return null;
            };

            DynamicMBean tranMBean = metricSet(CLIENT, null, TX_METRICS);

            //slow user
            slowPrepare = false;

            doInTransaction(client, () -> {
                Integer val = cache.get(1);

                doSleep(USER_DELAY);

                cache.put(1, val + 1);

                return null;
            });

            assertEquals(2, cache.get(1).intValue());

            assertTrue((Long)tranMBean.getAttribute(METRIC_TOTAL_USER_TIME) >= USER_DELAY);
            assertTrue((Long)tranMBean.getAttribute(METRIC_TOTAL_SYSTEM_TIME) < LONG_TRAN_TIMEOUT);

            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                Integer val = cache.get(1);

                doSleep(USER_DELAY);

                cache.put(1, val + 1);

                tx.rollback();
            }

            assertEquals(2, cache.get(1).intValue());

            assertTrue(rollbackDumpLsnr.check());

            //slow prepare
            slowPrepare = true;

            doInTransaction(client, txCallable);

            assertTrue(logTxDumpLsnr.check());

            assertEquals(3, cache.get(1).intValue());

            assertTrue((Long)tranMBean.getAttribute(METRIC_TOTAL_SYSTEM_TIME) >= SYSTEM_DELAY);

            long[] sysTimeHisto = (long[])tranMBean.getAttribute(METRIC_SYSTEM_TIME_HISTOGRAM);
            long[] userTimeHisto = (long[])tranMBean.getAttribute(METRIC_USER_TIME_HISTOGRAM);

            assertNotNull(sysTimeHisto);
            assertNotNull(userTimeHisto);

            assertTrue(sysTimeHisto != null && sysTimeHisto.length > 0);
            assertTrue(userTimeHisto != null && userTimeHisto.length > 0);

            logTxDumpLsnr.reset();

            //checking settings changing via JMX with second client
            Ignite client2 = startGrid(CLIENT_2);

            TransactionsMXBean tmMxBean = getMxBean(
                CLIENT,
                "Transactions",
                TransactionsMXBean.class,
                TransactionsMXBeanImpl.class
            );

            tmMxBean.setLongTransactionTimeDumpThreshold(0);
            tmMxBean.setTransactionTimeDumpSamplesCoefficient(0.0);

            doInTransaction(client2, txCallable);

            assertFalse(logTxDumpLsnr.check());

            //testing dumps limit

            doSleep(1000);

            transactionDumpLsnr.reset();

            transactionDumpsSkippedLsnr.reset();

            tmMxBean.setTransactionTimeDumpSamplesCoefficient(1.0);

            tmMxBean.setTransactionTimeDumpSamplesPerSecondLimit(TX_COUNT_FOR_LOG_THROTTLING_CHECK / 2);

            slowPrepare = false;

            for (int i = 0; i < TX_COUNT_FOR_LOG_THROTTLING_CHECK; i++)
                doInTransaction(client, txCallable);

            assertEquals(TX_COUNT_FOR_LOG_THROTTLING_CHECK / 2, transactionDumpLsnr.value());

            //testing skipped message in log

            doSleep(1000);

            doInTransaction(client, txCallable);

            assertTrue(transactionDumpsSkippedLsnr.check());

            U.log(log, sysTimeHisto);
            U.log(log, userTimeHisto);
        }
        finally {
            GridTestUtils.setFieldValue(IgniteTxAdapter.class, "log", oldLog);
        }
    }*/

    /**
     *
     */
    private class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                if (msg0 instanceof GridNearLockRequest) {
                    if (slowSystem) {
                        slowSystem = false;

                        doSleep(SYSTEM_DELAY);
                    }
                }

                if (msg0 instanceof GridNearTxPrepareRequest) {
                    if (simulateFailure) {
                        simulateFailure = false;

                        throw new RuntimeException("Simulating prepare failure.");
                    }
                }
            }

            super.sendMessage(node, msg, ackClosure);
        }
    }

    /**
     *
     */
    private static class TransactionDumpListener extends LogListener {
        /** */
        private final AtomicInteger counter = new AtomicInteger(0);

        /** */
        private final String regex;

        /** */
        private TransactionDumpListener(String regex) {
            this.regex = regex;
        }

        /** {@inheritDoc} */
        @Override public boolean check() {
            return value() > 0;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            counter.set(0);
        }

        /** {@inheritDoc} */
        @Override public void accept(String s) {
            if (s.matches(regex))
                counter.incrementAndGet();
        }

        /** */
        int value() {
            return counter.get();
        }
    }

    /**
     * Enum to define transaction test mode.
     */
    enum TxTestMode {
        /** If transaction should be committed. */
        COMMIT,

        /** If transaction should be rolled back. */
        ROLLBACK,

        /** If transaction should fail. */
        FAIL
    }

    /**
     *
     */
    private static class ClientTxTestResult {
        /** */
        final long startTime;

        /** */
        final long completionTime;

        /** */
        final DynamicMBean mBean;

        /** */
        public ClientTxTestResult(long startTime, long completionTime, DynamicMBean mBean) {
            this.startTime = startTime;
            this.completionTime = completionTime;
            this.mBean = mBean;
        }
    }

    /** */
    private static class CommonLogProxy implements IgniteLogger {
        /** */
        private IgniteLogger impl;

        /** */
        public CommonLogProxy(IgniteLogger impl) {
            this.impl = impl;
        }

        /** */
        public void setImpl(IgniteLogger impl) {
            this.impl = impl;
        }

        /** {@inheritDoc} */
        @Override public IgniteLogger getLogger(Object ctgr) {
            return impl.getLogger(ctgr);
        }

        /** {@inheritDoc} */
        @Override public void trace(String msg) {
            impl.trace(msg);
        }

        /** {@inheritDoc} */
        @Override public void debug(String msg) {
            impl.debug(msg);
        }

        /** {@inheritDoc} */
        @Override public void info(String msg) {
            impl.info(msg);
        }

        /** {@inheritDoc} */
        @Override public void warning(String msg, Throwable e) {
            impl.warning(msg, e);
        }

        /** {@inheritDoc} */
        @Override public void error(String msg, Throwable e) {
            impl.error(msg, e);
        }

        /** {@inheritDoc} */
        @Override public boolean isTraceEnabled() {
            return impl.isTraceEnabled();
        }

        /** {@inheritDoc} */
        @Override public boolean isDebugEnabled() {
            return impl.isDebugEnabled();
        }

        /** {@inheritDoc} */
        @Override public boolean isInfoEnabled() {
            return impl.isInfoEnabled();
        }

        /** {@inheritDoc} */
        @Override public boolean isQuiet() {
            return impl.isQuiet();
        }

        /** {@inheritDoc} */
        @Override public String fileName() {
            return impl.fileName();
        }
    }
}
