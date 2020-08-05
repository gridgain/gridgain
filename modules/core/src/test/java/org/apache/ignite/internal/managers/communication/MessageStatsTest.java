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
package org.apache.ignite.internal.managers.communication;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.DiagnosticMXBeanImpl;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.mxbean.DiagnosticMXBean;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.MessageOrderLogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MESSAGE_STATS_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_STARVATION_CHECK_INTERVAL;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_STAT_TOO_LONG_PROCESSING;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_STAT_TOO_LONG_WAITING;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.util.lang.GridFunc.t;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/**
 * Test for checking message statistics, JMX settings and log warnings.
 */
public class MessageStatsTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "test";

    /** */
    private static final String CLIENT = "client";

    /** */
    private static final int LONG_WAIT_TEST_THRESHOLD = 10;

    /** */
    private static final int LONG_PROC_TEST_THRESHOLD = 200;

    /** */
    private final LogListener slowMsgLogListener = LogListener.matches("Slow message").build();

    /** */
    private final LogListener slowMsgLongWaitLogListener =
        new MessageOrderLogListener(".*?Slow message: enqueueTs=[0-9 -:]{19}, waitTime=[0-9]{2,4}[.]{1}[0-9]{1,3}, procTime=[0-9]{1,3}[.]{1}[0-9]{1,3}, messageId=[0-9a-f]{1,8}, queueSzBefore=[0-9]{1,3}, headMessageId=(null|[0-9a-f]{1,8}), queueSzAfter=[0-9]{1,3}, message=.*");

    /** */
    private final LogListener slowMsgLongProcLogListener =
        new MessageOrderLogListener(".*?Slow message: enqueueTs=[0-9 -:]{19}, waitTime=[0-9]{1,3}[.]{1}[0-9]{1,3}, procTime=[0-9]{3,4}[.]{1}[0-9]{1,3}, messageId=[0-9a-f]{1,8}, queueSzBefore=[0-9]{1,3}, headMessageId=(null|[0-9a-f]{1,8}), queueSzAfter=[0-9]{1,3}, message=.*");

    /** */
    private final ListeningTestLogger testLog = new ListeningTestLogger(log(), slowMsgLogListener, slowMsgLongProcLogListener, slowMsgLongWaitLogListener);

    /** */
    public boolean slowPrepare = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.startsWith(CLIENT)) {
            cfg.setClientMode(true);
        }
        else {
            cfg.setCacheConfiguration(
                new CacheConfiguration(CACHE_NAME)
                    .setAtomicityMode(TRANSACTIONAL)
                    .setBackups(1)
                    .setWriteSynchronizationMode(FULL_SYNC)
            );
        }

        cfg.setMetricExporterSpi(new JmxMetricExporterSpi());

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @param nodeCnt Total node count.
     * @param checks List of checks, see {@link #testJmx(String, String, int, int, IgniteBiTuple[])} for description.
     * @throws Exception If failed.
     */
    private void doChecks(int nodeCnt, IgniteBiTuple<Boolean, Consumer<DiagnosticMXBean>>... checks) throws Exception {
        List<DiagnosticMXBean> diagnosticMXBeans = new LinkedList<>();

        for (int i = 0; i < nodeCnt; i++)
            diagnosticMXBeans.add(getMxBean(grid(0).name(), "Diagnostic", DiagnosticMXBean.class, DiagnosticMXBeanImpl.class));

        DiagnosticMXBean nodeMxBean = diagnosticMXBeans.get(0);

        for (IgniteBiTuple<Boolean, Consumer<DiagnosticMXBean>> check : checks) {
            if (check.get1()) {
                for (DiagnosticMXBean mxBean : diagnosticMXBeans)
                    check.get2().accept(mxBean);
            } else
                check.get2().accept(nodeMxBean);
        }
    }

    /**
     * @param sysProperty System property to set for this test.
     * @param sysPropertyVal System property value.
     * @param srvCnt Count of servers.
     * @param clientCnt Count of clients.
     * @param checks List of checks, which are tuples of {@code Boolean} and {@code Consumer<DiagnosticMXBean>}.
     *               Boolean value means whether we should do this check for mxBeans from every node.
     * @throws Exception If failed.
     */
    private void testJmx(
        String sysProperty,
        String sysPropertyVal,
        int srvCnt,
        int clientCnt,
        IgniteBiTuple<Boolean, Consumer<DiagnosticMXBean>>... checks
    ) throws Exception {
        String oldVal = System.getProperty(sysProperty);

        if (sysPropertyVal == null)
            System.clearProperty(sysProperty);
        else
            System.setProperty(sysProperty, sysPropertyVal);

        for (int i = 0; i < srvCnt; i++)
            startGrid(i);

        for (int i = 0; i < clientCnt; i++)
            startGrid(CLIENT + i);

        doChecks(srvCnt + clientCnt, checks);

        startGrid((srvCnt++) + clientCnt);
        startGrid(srvCnt + (clientCnt++));

        // Do last check after starting new nodes, for all nodes.
        doChecks(srvCnt + clientCnt, t(true, checks[checks.length - 1].get2()));

        if (oldVal == null)
            System.clearProperty(sysProperty);
        else
            System.setProperty(sysProperty, oldVal);
    }

    /** */
    @Test
    public void testJmxStatsEnabledOneNode() throws Exception {
        Consumer<DiagnosticMXBean> afterStart = mxBean -> assertFalse(mxBean.getDiagnosticMessageStatsEnabled());
        Consumer<DiagnosticMXBean> setTrue = mxBean -> mxBean.setDiagnosticMessageStatsEnabled(true);
        Consumer<DiagnosticMXBean> afterSet = mxBean -> assertTrue(mxBean.getDiagnosticMessageStatsEnabled());

        testJmx(IGNITE_MESSAGE_STATS_ENABLED, "false", 1, 0, t(false, afterStart), t(false, setTrue), t(false, afterSet));
    }

    /** */
    @Test
    public void testJmxStatsEnabledMultipleNodes() throws Exception {
        Consumer<DiagnosticMXBean> shouldBeFalse = mxBean -> assertFalse(mxBean.getDiagnosticMessageStatsEnabled());
        Consumer<DiagnosticMXBean> setTrue = mxBean -> mxBean.setDiagnosticMessageStatsEnabled(true);
        Consumer<DiagnosticMXBean> shouldBeTrue = mxBean -> assertTrue(mxBean.getDiagnosticMessageStatsEnabled());
        Consumer<DiagnosticMXBean> setFalse = mxBean -> mxBean.setDiagnosticMessageStatsEnabled(false);

        testJmx(IGNITE_MESSAGE_STATS_ENABLED, "false", 2, 2, t(true, shouldBeFalse), t(false, setTrue), t(true, shouldBeTrue), t(false, setFalse), t(true, shouldBeFalse));
    }

    /** */
    @Test
    public void testJmxStatsTooLongProcessingOneNode() throws Exception {
        Consumer<DiagnosticMXBean> afterStart = mxBean -> assertEquals(10, mxBean.getDiagnosticMessageStatTooLongProcessing());
        Consumer<DiagnosticMXBean> set20 = mxBean -> mxBean.setDiagnosticMessageStatTooLongProcessing(20);
        Consumer<DiagnosticMXBean> shouldBe20 = mxBean -> assertEquals(20, mxBean.getDiagnosticMessageStatTooLongProcessing());
        Consumer<DiagnosticMXBean> setZero = mxBean -> mxBean.setDiagnosticMessageStatTooLongProcessing(0);
        Consumer<DiagnosticMXBean> checkZero = mxBean -> assertEquals(0, mxBean.getDiagnosticMessageStatTooLongProcessing());
        Consumer<DiagnosticMXBean> setMinus1 = mxBean -> assertThrows(log, () -> mxBean.setDiagnosticMessageStatTooLongProcessing(-1), IgniteException.class, null);

        testJmx(IGNITE_STAT_TOO_LONG_PROCESSING, "10", 1, 0,
            t(false, afterStart),
            t(false, set20),
            t(true, shouldBe20),
            t(false, setZero),
            t(true, checkZero),
            t(false, setMinus1)
        );
    }

    /** */
    @Test
    public void testJmxStatsTooLongProcessingMultipleNode() throws Exception {
        Consumer<DiagnosticMXBean> afterStart = mxBean -> assertEquals(10, mxBean.getDiagnosticMessageStatTooLongProcessing());
        Consumer<DiagnosticMXBean> set20 = mxBean -> mxBean.setDiagnosticMessageStatTooLongProcessing(20);
        Consumer<DiagnosticMXBean> shouldBe20 = mxBean -> assertEquals(20, mxBean.getDiagnosticMessageStatTooLongProcessing());

        testJmx(IGNITE_STAT_TOO_LONG_PROCESSING, "10", 2, 2, t(true, afterStart), t(false, set20), t(true, shouldBe20));
    }

    /** */
    @Test
    public void testJmxStatsTooLongWaitingOneNode() throws Exception {
        Consumer<DiagnosticMXBean> afterStart = mxBean -> assertEquals(10, mxBean.getDiagnosticMessageStatTooLongWaiting());
        Consumer<DiagnosticMXBean> set20 = mxBean -> mxBean.setDiagnosticMessageStatTooLongWaiting(20);
        Consumer<DiagnosticMXBean> shouldBe20 = mxBean -> assertEquals(20, mxBean.getDiagnosticMessageStatTooLongWaiting());
        Consumer<DiagnosticMXBean> setZero = mxBean -> mxBean.setDiagnosticMessageStatTooLongWaiting(0);
        Consumer<DiagnosticMXBean> checkZero = mxBean -> assertEquals(0, mxBean.getDiagnosticMessageStatTooLongWaiting());
        Consumer<DiagnosticMXBean> setMinus1 = mxBean -> assertThrows(log, () -> mxBean.setDiagnosticMessageStatTooLongWaiting(-1), IgniteException.class, null);

        testJmx(IGNITE_STAT_TOO_LONG_WAITING, "10", 1, 0,
            t(false, afterStart),
            t(false, set20),
            t(true, shouldBe20),
            t(false, setZero),
            t(true, checkZero),
            t(false, setMinus1)
        );
    }

    /** */
    @Test
    public void testJmxStatsTooLongWaitingMultipleNode() throws Exception {
        Consumer<DiagnosticMXBean> afterStart = mxBean -> assertEquals(10, mxBean.getDiagnosticMessageStatTooLongWaiting());
        Consumer<DiagnosticMXBean> set20 = mxBean -> mxBean.setDiagnosticMessageStatTooLongWaiting(20);
        Consumer<DiagnosticMXBean> afterSet = mxBean -> assertEquals(20, mxBean.getDiagnosticMessageStatTooLongWaiting());

        testJmx(IGNITE_STAT_TOO_LONG_WAITING, "10", 2, 2, t(true, afterStart), t(false, set20), t(true, afterSet));
    }

    /**
     * Imitates long processing of some messages.
     *
     * @param ignite Node.
     * @throws Exception If failed.
     */
    private void imitateLongProcessing(IgniteEx ignite) throws Exception {
        slowPrepare = true;

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(CACHE_NAME);

        try {
            for (int i = 0; i < 10; i++) {
                int j = i;

                doInTransaction(ignite, () -> {
                    Integer a = cache.get(j);

                    if (a == null)
                        a = 0;

                    cache.put(j, a + 1);

                    return null;
                });
            }
        }
        finally {
            slowPrepare = false;
        }
    }

    /**
     * Imitates long queue waiting of some messages.
     *
     * @param ignite Node.
     * @throws Exception If failed.
     */
    private void imitateLongWaiting(IgniteEx ignite) throws Exception {
        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(CACHE_NAME);

        int procCnt = Runtime.getRuntime().availableProcessors();

        runMultiThreadedAsync(() -> {
            try {
                for (int i = 0; i < 300; i++) {
                    int j = i;

                    doInTransaction(ignite, () -> {
                        Integer p = cache.get(j);

                        if (p == null)
                            p = 0;

                        cache.put(j, p + 1);

                        return null;
                    });
                }
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }, procCnt * 5, "txAsyncLoad").get();
    }

    /**
     * Tests the presence of long warnings with given JMX settings. Printing of warnings is called manually.
     *
     * @param ignite Ignite node.
     * @param mxBean JMX bean.
     * @param tooLongWaiting Long waiting threshold.
     * @param tooLongProcessing Long processing threshold.
     * @throws Exception If failed.
     */
    private void testLogWarningsWithJmxSettings(IgniteEx ignite,
        DiagnosticMXBean mxBean,
        long tooLongWaiting,
        long tooLongProcessing
    ) throws Exception {
        mxBean.setDiagnosticMessageStatTooLongWaiting(tooLongWaiting);
        mxBean.setDiagnosticMessageStatTooLongProcessing(tooLongProcessing);

        imitateLongWaiting(ignite);

        imitateLongProcessing(ignite);

        slowMsgLongWaitLogListener.reset();
        slowMsgLongProcLogListener.reset();

        for (int i = 0; i < 3; i++)
            ignite.context().io().dumpProcessedMessagesStats();

        assertEquals(tooLongWaiting != 0 && tooLongWaiting != Integer.MAX_VALUE, slowMsgLongWaitLogListener.check());
        assertEquals(tooLongProcessing != 0 && tooLongProcessing != Integer.MAX_VALUE, slowMsgLongProcLogListener.check());
    }

    /** */
    @Test
    public void testLogWarnings() throws Exception {
        IgniteEx ignite = startGrids(3);

        DiagnosticMXBean mxBean = getMxBean(ignite.name(), "Diagnostic", DiagnosticMXBean.class, DiagnosticMXBeanImpl.class);

        mxBean.setDiagnosticMessageStatsEnabled(true);

        testLogWarningsWithJmxSettings(ignite, mxBean, Integer.MAX_VALUE, Integer.MAX_VALUE);
        testLogWarningsWithJmxSettings(ignite, mxBean, LONG_WAIT_TEST_THRESHOLD, Integer.MAX_VALUE);
        testLogWarningsWithJmxSettings(ignite, mxBean, Integer.MAX_VALUE, LONG_PROC_TEST_THRESHOLD);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_STARVATION_CHECK_INTERVAL, value = "3000")
    public void testLogWarningsOnTimeout() throws Exception {
        IgniteEx ignite = startGrids(3);

        DiagnosticMXBean mxBean = getMxBean(ignite.name(), "Diagnostic", DiagnosticMXBean.class, DiagnosticMXBeanImpl.class);

        mxBean.setDiagnosticMessageStatTooLongWaiting(LONG_WAIT_TEST_THRESHOLD);

        mxBean.setDiagnosticMessageStatTooLongProcessing(LONG_PROC_TEST_THRESHOLD);

        // Check that there are warnings in log.
        mxBean.setDiagnosticMessageStatsEnabled(true);

        slowMsgLongWaitLogListener.reset();
        slowMsgLongProcLogListener.reset();

        imitateLongWaiting(ignite);

        imitateLongProcessing(ignite);

        doSleep(3100);

        assertTrue(slowMsgLongWaitLogListener.check());
        assertTrue(slowMsgLongProcLogListener.check());

        // Check that there are no warnings in log if diagnostincs is disabled.
        mxBean.setDiagnosticMessageStatsEnabled(false);

        slowMsgLongWaitLogListener.reset();
        slowMsgLongProcLogListener.reset();

        imitateLongWaiting(ignite);

        imitateLongProcessing(ignite);

        doSleep(3100);

        assertFalse(slowMsgLongWaitLogListener.check());
        assertFalse(slowMsgLongProcLogListener.check());
    }

    /**
     * Test communication SPI that can delay some types of messages.
     */
    private class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                if (slowPrepare && (msg0 instanceof GridDhtTxPrepareRequest || msg0 instanceof GridDhtTxPrepareResponse))
                    doSleep(LONG_PROC_TEST_THRESHOLD + 10);
            }

            super.sendMessage(node, msg, ackClosure);
        }
    }
}
