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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.DiagnosticMXBeanImpl;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.mxbean.DiagnosticMXBean;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MESSAGE_STATS_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_STAT_TOO_LONG_PROCESSING;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_STAT_TOO_LONG_WAITING;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.util.lang.GridFunc.t;

/**
 *
 */
public class MessageStatsTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "test";

    /** */
    private static final String CLIENT = "client";

    /** */
    private LogListener slowMsgLogListener = LogListener.matches("Slow message").build();

    /** */
    private final ListeningTestLogger testLog = new ListeningTestLogger(false, log());

    /** */
    private volatile boolean slowPrepare = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.startsWith(CLIENT)) {
            cfg.setClientMode(true);
        }
        else {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(1);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);

            cfg.setCacheConfiguration(ccfg);
        }

        cfg.setMetricExporterSpi(new JmxMetricExporterSpi());

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        testLog.registerListener(slowMsgLogListener);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testStats() throws Exception {
        IgniteEx ignite = startGrids(2);

        DiagnosticMXBean mxBean =
            getMxBean(ignite.name(), "Diagnostic", DiagnosticMXBean.class, DiagnosticMXBeanImpl.class);

        assertTrue(mxBean.getDiagnosticMessageStatsEnabled());

        int testTooLongProcessing = 3725;

        mxBean.setDiagnosticMessageStatTooLongProcessing(testTooLongProcessing);

        Ignite newSrv = startGrid();

        DiagnosticMXBean newSrvMxBean =
            getMxBean(newSrv.name(), "Diagnostic", DiagnosticMXBean.class, DiagnosticMXBeanImpl.class);

        assertEquals(testTooLongProcessing, newSrvMxBean.getDiagnosticMessageStatTooLongProcessing());

        mxBean.setDiagnosticMessageStatTooLongProcessing(250);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(CACHE_NAME);

        cache.put(1, 1);

        slowPrepare = true;

        doInTransaction(ignite, () -> {
            cache.put(1, cache.get(1) + 1);

            return null;
        });

        slowPrepare = false;

        ignite.context().io().dumpProcessedMessagesStats();

        assertTrue(slowMsgLogListener.check());
    }

    /**
     *
     *
     * @param sysProperty
     * @param sysPropertyVal
     * @param srvCnt
     * @param clientCnt
     * @param checks
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

        List<DiagnosticMXBean> diagnosticMXBeans = new LinkedList<>();

        for (int i = 0; i < srvCnt + clientCnt; i++)
            diagnosticMXBeans.add(getMxBean(grid(0).name(), "Diagnostic", DiagnosticMXBean.class, DiagnosticMXBeanImpl.class));

        DiagnosticMXBean nodeMxBean = diagnosticMXBeans.get(0);

        for (IgniteBiTuple<Boolean, Consumer<DiagnosticMXBean>> check : checks) {
            if (check.get1()) {
                for (DiagnosticMXBean mxBean : diagnosticMXBeans)
                    check.get2().accept(mxBean);
            } else
                check.get2().accept(nodeMxBean);
        }

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
        Consumer<DiagnosticMXBean> afterSet = mxBean -> assertEquals(20, mxBean.getDiagnosticMessageStatTooLongProcessing());

        testJmx(IGNITE_STAT_TOO_LONG_PROCESSING, "10", 1, 0, t(false, afterStart), t(false, set20), t(false, afterSet));
    }

    /** */
    @Test
    public void testJmxStatsTooLongProcessingMultipleNode() throws Exception {
        Consumer<DiagnosticMXBean> afterStart = mxBean -> assertEquals(10, mxBean.getDiagnosticMessageStatTooLongProcessing());
        Consumer<DiagnosticMXBean> set20 = mxBean -> mxBean.setDiagnosticMessageStatTooLongProcessing(20);
        Consumer<DiagnosticMXBean> afterSet = mxBean -> assertEquals(20, mxBean.getDiagnosticMessageStatTooLongProcessing());

        testJmx(IGNITE_STAT_TOO_LONG_PROCESSING, "10", 2, 2, t(true, afterStart), t(false, set20), t(true, afterSet));
    }

    /** */
    @Test
    public void testJmxStatsTooLongWaitingOneNode() throws Exception {
        Consumer<DiagnosticMXBean> afterStart = mxBean -> assertEquals(10, mxBean.getDiagnosticMessageStatTooLongWaiting());
        Consumer<DiagnosticMXBean> set20 = mxBean -> mxBean.setDiagnosticMessageStatTooLongWaiting(20);
        Consumer<DiagnosticMXBean> afterSet = mxBean -> assertEquals(20, mxBean.getDiagnosticMessageStatTooLongWaiting());

        testJmx(IGNITE_STAT_TOO_LONG_WAITING, "10", 1, 0, t(false, afterStart), t(false, set20), t(false, afterSet));
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
     *
     */
    private class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                if (slowPrepare && msg0 instanceof GridNearTxPrepareRequest)
                    doSleep(500);
            }

            super.sendMessage(node, msg, ackClosure);
        }
    }
}
