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

package org.apache.ignite.internal.processors.cache.msgtimelogging;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.LongStream;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.metric.HistogramMetric;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiMBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_COMM_SPI_TIME_HIST_BOUNDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_MESSAGES_TIME_LOGGING;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 *
 */
public abstract class GridCacheMessagesTimeLoggingAbstractTest extends GridCommonAbstractTest {
    /** Grid count. */
    protected static final int GRID_CNT = 3;

    /**
     *
     */
    abstract void setEnabledParam();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);

        ccfg.setBackups(2);

        cfg.setCacheConfiguration(ccfg);

        cfg.setCommunicationSpi(new RecordingSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        setEnabledParam();

        startGrids(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        System.clearProperty(IGNITE_ENABLE_MESSAGES_TIME_LOGGING);
        System.clearProperty(IGNITE_COMM_SPI_TIME_HIST_BOUNDS);
    }

    /**
     *
     */
    protected void checkOutcomingEventsNum(Class reqClass, Class respClass) throws MalformedObjectNameException {
        checkEventsNum(0, 1, reqClass, respClass);
    }

    /**
     * Compares sent events number with histogram entries number.
     * Fails if these numbers differ.
     */
    private void checkEventsNum(int sourceIdx,
        int targetIdx,
        Class reqClass,
        Class respClass
    )
        throws MalformedObjectNameException
    {
        RecordingSpi spi = (RecordingSpi)grid(sourceIdx).configuration().getCommunicationSpi();

        HistogramMetric metric = getMetric(sourceIdx, targetIdx, respClass);
        assertNotNull("HistogramMetric not found", metric);

        String metricName = metricName(grid(targetIdx).localNode().id(), reqClass);

        long sum = LongStream.of(metric.value()).sum();

        Integer eventsNum = spi.classesMap.get(metricName(grid(targetIdx).localNode().id(), reqClass));
        assertNotNull("Value " + metricName + " not found in classesMap", eventsNum);

        assertEquals("Unexpected metric data amount for " + respClass + ": " + sum + ". Events num: " + eventsNum,
            sum, (long)eventsNum);
    }

    /**
     * @param sourceNodeIdx Index of node that stores metric.
     * @param targetNodeIdx Index of node where requests are sent.
     * @param respCls Metric request class.
     * @return {@code HistogramMetric} for {@code respCls}.
     */
    @Nullable public HistogramMetric getMetric(
        int sourceNodeIdx,
        int targetNodeIdx,
        Class respCls
    )
        throws MalformedObjectNameException
    {
        return getMetric(sourceNodeIdx, grid(targetNodeIdx).localNode().id(), respCls);
    }

    /**
     * @param srcNodeIdx Index of node that stores metric.
     * @param targetNodeId Id of node where requests are sent.
     * @param respCls Metric request class.
     * @return {@code HistogramMetric} for {@code respCls}.
     */
    @Nullable public HistogramMetric getMetric(int srcNodeIdx,
        UUID targetNodeId,
        Class respCls
    )
        throws MalformedObjectNameException
    {
        TcpCommunicationSpiMBean mbean = mbean(srcNodeIdx);

        if (mbean == null)
            return null;

        Map<UUID, Map<String, HistogramMetric>> nodeMap = mbean.getOutMetricsByNodeByMsgClass();

        assertNotNull(nodeMap);

        Map<String, HistogramMetric> clsNameMap = nodeMap.get(targetNodeId);

        if (clsNameMap == null)
            return null;

        return clsNameMap.get(respCls.getName());
    }

    /**
     * Gets TcpCommunicationSpiMBean for given node.
     *
     * @param nodeIdx Node index.
     * @return MBean instance.
     */
    protected TcpCommunicationSpiMBean mbean(int nodeIdx) throws MalformedObjectNameException {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(nodeIdx), "SPIs",
            RecordingSpi.class.getSimpleName());

        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

        if (mbeanServer.isRegistered(mbeanName))
            return MBeanServerInvocationHandler.newProxyInstance(mbeanServer, mbeanName, TcpCommunicationSpiMBean.class,
                true);
        else
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return null;
    }

    /**
     *
     */
    protected static String metricName(UUID nodeId, Class msgClass) {
        return nodeId + "." + msgClass.getSimpleName();
    }

    /**
     *
     */
    protected void populateCache(IgniteCache<Integer, Integer> cache) {
        Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < 20; ++i) {
            cache.put(i, i);
            map.put(i + 20, i * 2);
        }

        cache.putAll(map);
    }

    /**
     * Counts sent messages num per message class.
     */
    protected static class RecordingSpi extends TcpCommunicationSpi {
        /** */
        private Map<String, Integer> classesMap = new HashMap<>();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            recordMessage(node, msg);

            super.sendMessage(node, msg);
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            recordMessage(node, msg);

            super.sendMessage(node, msg, ackC);
        }

        /**
         *
         */
        private void recordMessage(ClusterNode node, Message msg) {
            if (!node.isLocal()) {
                Message msg0 = msg;

                if (msg instanceof GridIoMessage)
                    msg0 = ((GridIoMessage)msg).message();

                classesMap.merge(metricName(node.id(), msg0.getClass()), 1, Integer::sum);
            }
        }

        /**  */
        public Map<String, Integer> getClassesMap() {
            return classesMap;
        }
    }
}
