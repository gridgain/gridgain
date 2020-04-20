/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DUMP_TX_COLLISIONS_INTERVAL;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/** */
public class TxWithKeyContentionSelfTest extends GridCommonAbstractTest {
    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId("NODE_" + name.substring(name.length() - 1));

        if (client)
            cfg.setClientMode(true);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(20 * 1024 * 1024)
                )
        );

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        cfg.setCacheConfiguration(getCacheConfiguration(DEFAULT_CACHE_NAME));

        if (client){
            cfg.setConsistentId("Client");

            cfg.setClientMode(client);
        }

        return cfg;
    }

    /** */
    protected TransactionConcurrency getConcurrency() {
       return PESSIMISTIC;
    }

    /** */
    protected TransactionIsolation getIsolation() {
        return READ_COMMITTED;
    }

    /** */
    protected CacheConfiguration<?, ?> getCacheConfiguration(String name) {
        return
            new CacheConfiguration<>(name)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setAffinity(new RendezvousAffinityFunction(false, 16))
                .setBackups(2)
                .setStatisticsEnabled(true);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testPessimisticRepeatableReadRollbacksNoData() throws Exception {
        testKeyCollisionsMetric(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testPessimisticSerializableRollbacksNoData() throws Exception {
        testKeyCollisionsMetric(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testOptimisticSuspendedReadCommittedTxTimeoutRollbacks() throws Exception {
        testKeyCollisionsMetric(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testOptimisticSuspendedRepeatableReadTxTimeoutRollbacks() throws Exception {
        testKeyCollisionsMetric(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testOptimisticSuspendedSerializableTxTimeoutRollbacks() throws Exception {
        testKeyCollisionsMetric(OPTIMISTIC, SERIALIZABLE);
    }

    /** Tests metric correct results while tx collisions occured. */
    private void testKeyCollisionsMetric(TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        Ignite ig = startGridsMultiThreaded(3);

        int contCnt = (int)U.staticField(IgniteTxManager.class, "COLLISIONS_QUEUE_THRESHOLD") * 2;

        ig.cluster().active(true);

        client = true;

        Ignite cl = startGrid();

        IgniteTransactions txMgr = cl.transactions();

        IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

        IgniteCache<Integer, Integer> cache0 = cl.cache(DEFAULT_CACHE_NAME);

        for (int i = 0 ; i < 4; ++i)
            cache.put(i, i);

        final Integer keyId = primaryKey(cache);

        CountDownLatch startOneKeyTx = new CountDownLatch(1);

        CountDownLatch neadLockReq = new CountDownLatch(contCnt);

        for (Ignite ig0 : G.allGrids()) {
            TestRecordingCommunicationSpi commSpi0 =
                (TestRecordingCommunicationSpi)ig0.configuration().getCommunicationSpi();

            commSpi0.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    if (msg instanceof GridDhtTxFinishRequest)
                        return true;

                    if (msg instanceof GridNearTxPrepareResponse)
                        startOneKeyTx.countDown();

                    if (msg instanceof GridNearLockRequest)
                        neadLockReq.countDown();

                    return false;
                }
            });
        }

        IgniteInternalFuture f = GridTestUtils.runAsync(() -> {
            try (Transaction tx2 = txMgr.txStart(concurrency, isolation)) {
                cache0.put(keyId, 0);
                tx2.commit();
            }
        });

        startOneKeyTx.await();

        GridCompoundFuture<?, ?> finishFut = new GridCompoundFuture<>();

        for (int i = 1; i < contCnt; ++i) {
            int i0 = i;
            IgniteInternalFuture f0 = GridTestUtils.runAsync(() -> {
                try (Transaction tx = txMgr.txStart(concurrency, isolation)) {
                    cache0.put(keyId, i0);

                    tx.commit();
                }
            });

            finishFut.add(f0);
        }

        finishFut.markInitialized();

        neadLockReq.await();

        for (Ignite ig0 : G.allGrids()) {
            TestRecordingCommunicationSpi commSpi0 =
                (TestRecordingCommunicationSpi)ig0.configuration().getCommunicationSpi();

            commSpi0.stopBlock();
        }

        IgniteTxManager txManager = ((IgniteEx) ig).context().cache().context().tm();

        U.invoke(IgniteTxManager.class, txManager, "collectTxCollisionsInfo");

        CacheMetrics metrics = ig.cache(DEFAULT_CACHE_NAME).localMetrics();

        String coll1 = metrics.getTxKeyCollisions();

        String coll2 = metrics.getTxKeyCollisions();

        // check idempotent
        assertEquals(coll1, coll2);

        assertTrue(coll1.contains("queueSize"));

        f.get();

        finishFut.get();
    }

    /**
     * Gets CacheGroupMetricsMXBean for given node and group name.
     *
     * @param nodeIdx Node index.
     * @param cacheName Cache name.
     * @return MBean instance.
     */
    private CacheMetricsMXBean mxBean(int nodeIdx, String cacheName, Class<? extends CacheMetricsMXBean> clazz)
        throws MalformedObjectNameException {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(nodeIdx), cacheName,
            clazz.getName());

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, CacheMetricsMXBean.class,
            true);
    }
}
