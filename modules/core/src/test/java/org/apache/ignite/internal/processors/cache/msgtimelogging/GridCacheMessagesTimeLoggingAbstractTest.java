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
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
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

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener.metricName;

/**
 *
 */
public abstract class GridCacheMessagesTimeLoggingAbstractTest extends GridCommonAbstractTest {
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
    }

    /**
     *
     */
    protected void checkOutcomingEventsNum(Class msgClass) throws MalformedObjectNameException {
        checkEventsNum(0, grid(1), msgClass, true);
    }

    /**
     *
     */
    protected void checkIncomingEventsNum(Class msgClass) throws MalformedObjectNameException {
        checkEventsNum(0, grid(1), msgClass, false);
    }

    /**
     * Compares sent events number with histogram entries number.
     * Fails if these numbers differ.
     */
    private void checkEventsNum(int sourceIdx, IgniteEx target, Class msgClass, boolean outcoming) throws MalformedObjectNameException {
        RecordingSpi spi = (RecordingSpi)grid(sourceIdx).configuration().getCommunicationSpi();

        HistogramMetric metric = getMetric(sourceIdx, target, msgClass, outcoming);
        assertNotNull("HistogramMetric not found", metric);

        String metricName = metricName(target.localNode().id(), msgClass);

        long sum = LongStream.of(metric.value()).sum();

        Integer eventsNum = spi.classesMap.get(metricName);
        assertNotNull("Value " + metricName + " not found in classesMap", eventsNum);

        assertTrue("Unexpected metric data amount for " + msgClass + ": " + sum + ". Events num: " + eventsNum, sum == eventsNum);
    }

    /**
     * @param sourceNodeIdx Index of node that stores metric.
     * @param targetNode Node where requests are sent.
     * @param msgClass Metric request class.
     * @return {@code HistogramMetric} for {@code msgClass}.
     */
    @Nullable public HistogramMetric getMetric(int sourceNodeIdx, IgniteEx targetNode, Class msgClass, boolean outcoming) throws MalformedObjectNameException {
        try {
            TcpCommunicationSpiMBean mbean = mbean(sourceNodeIdx);

            Map<UUID, Map<Class<? extends Message>, HistogramMetric>> nodeMap = outcoming ? mbean.getOutMetricsByNodeByMsgClass() : mbean.getInMetricsByNodeByMsgClass();

            Map<Class<? extends Message>, HistogramMetric> classMap = nodeMap.get(targetNode.localNode().id());

            return classMap.get(msgClass);
        } catch (NullPointerException e) {
            return null;
        }
    }

    /**
     * Gets TcpCommunicationSpiMBean for given node.
     *
     * @param nodeIdx Node index.
     * @return MBean instance.
     */
    private TcpCommunicationSpiMBean mbean(int nodeIdx) throws MalformedObjectNameException {
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
     * Counts sent messages num per message class.
     */
    private static class RecordingSpi extends TcpCommunicationSpi {
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
    }
}
