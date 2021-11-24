/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS_OVERRIDE;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionState.PREPARING;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;

/**
 *
 */
public class TxRecoveryNeighbourcastTest extends GridCommonAbstractTest {
    private static int attrv = 0;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        Map<String, String> attrs = new HashMap<>();

        if (!igniteInstanceName.startsWith("client"))
            attrs.put(ATTR_MACS_OVERRIDE, "95:03:5c:1e:3a:e0");

        attrs.put("opt_attr", String.valueOf(attrv));

        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)
                )
            )
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setUserAttributes(attrs);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** */
    @Test
    public void test() throws Exception {
        final int NODES_CNT = 3;

        IgniteEx ignite = startGrids(NODES_CNT);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteEx client = startClientGrid("client");

        IgniteCache<Integer, Integer> cache = client.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(NODES_CNT - 1)
        );

        List<Integer> primaryKeys = primaryKeys(ignite.cache(DEFAULT_CACHE_NAME), 10, 0);

        for (Integer k : primaryKeys)
            cache.put(k, k);

        spi(ignite).blockMessages((node, msg) -> msg instanceof GridDhtTxFinishRequest);
        spi(client).blockMessages((node, msg) -> msg instanceof GridCacheTxRecoveryResponse);
        spi(grid(1)).blockMessages((node, msg) -> {
            if (msg instanceof GridCacheTxRecoveryResponse) {
                GridCacheTxRecoveryResponse m = (GridCacheTxRecoveryResponse) msg;

                if (!m.success())
                    return false;

                GridCacheTxRecoveryResponse resp =
                    new GridCacheTxRecoveryResponse(m.version(), m.futureId(), m.miniId(), false, m.addDepInfo);

                try {
                    grid(1).context().io().sendToGridTopic(node.id(), GridTopic.TOPIC_CACHE, resp, SYSTEM_POOL);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }

                return true;
            }
            else
                return false;
        });

        IgniteInternalFuture fut = runMultiThreadedAsync(() -> {
            try (Transaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {
                int key = primaryKeys.get(ThreadLocalRandom.current().nextInt(10));

                Integer v = cache.get(key);

                cache.put(key, v == null ? 0 : v);

                tx.commit();
            }
        }, 1, "testTx");

        doSleep(500);

        log.info("zzz stopping grid");

        stopGrid(0);
        stopGrid("client");

        awaitPartitionMapExchange();

        fut.get();

        doSleep(500);
    }

    /** */
    @Test
    public void test0() throws Exception {
        final int NODES_CNT = 3;

        IgniteEx ignite = startGrids(NODES_CNT);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteEx client = startClientGrid("client");

        IgniteCache<Integer, Integer> cache = client.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(NODES_CNT - 1)
        );

        AtomicInteger cntr = new AtomicInteger();
        AtomicBoolean stopped = new AtomicBoolean();

        IgniteInternalFuture fut = runMultiThreadedAsync(() -> {
            while (!stopped.get()) {
                try (Transaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {
                    int i = cntr.incrementAndGet();

                    cache.put(i, i);

                    tx.commit();
                }
            }
        }, 10, "testTx");

        doSleep(3000);

        stopGrid(0);

        awaitPartitionMapExchange();

        stopped.set(true);

        try {
            fut.get();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        doSleep(500);
    }

    final CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        CacheWriteSynchronizationMode syncMode,
        int backups,
        int parts) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(syncMode);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, parts));

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }

    private List<Integer> singlePartKeys(IgniteCache<Object, Object> primaryCache, int size, int part) throws Exception {
        Ignite ignite = primaryCache.unwrap(Ignite.class);

        List<Integer> res = new ArrayList<>();

        final Affinity<Object> aff = ignite.affinity(primaryCache.getName());

        final ClusterNode node = ignite.cluster().localNode();

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return aff.primaryPartitions(node).length > 0;
            }
        }, 5000));

        int cnt = 0;

        for (int key = 0; key < aff.partitions() * size * 10; key++) {
            if (aff.partition(key) == part) {
                res.add(key);

                if (++cnt == size)
                    break;
            }
        }

        assertEquals(size, res.size());

        return res;
    }

    private long getUpdateCounter(IgniteEx node, Integer key) {
        int partId = node.cachex(DEFAULT_CACHE_NAME).context().affinity().partition(key);

        GridDhtLocalPartition part = node.cachex(DEFAULT_CACHE_NAME).context().dht().topology().localPartition(partId);

        assert part != null;

        return part.updateCounter();
    }

    @Test
    public void checkUpdateCountersGapsClosed() throws Exception {
        int srvCnt = 4;

        startGridsMultiThreaded(srvCnt);

        IgniteEx nearNode = grid(srvCnt - 1);

        IgniteCache<Object, Object> cache = nearNode.createCache(
            cacheConfiguration(PARTITIONED, FULL_SYNC, srvCnt - 1, srvCnt)
                .setIndexedTypes(Integer.class, Integer.class));

        IgniteEx primary = grid(0);

        Affinity<Object> aff = nearNode.affinity(cache.getName());

        int[] nearBackupParts = aff.backupPartitions(nearNode.localNode());

        int[] primaryParts = aff.primaryPartitions(primary.localNode());

        Collection<Integer> nearSet = new HashSet<>();

        for (int part : nearBackupParts)
            nearSet.add(part);

        Collection<Integer> primarySet = new HashSet<>();

        for (int part : primaryParts)
            primarySet.add(part);

        // We need backup partitions on the near node.
        nearSet.retainAll(primarySet);

        List<Integer> keys = singlePartKeys(primary.cache(DEFAULT_CACHE_NAME), 20, nearSet.iterator().next());

        int range = 3;

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        List<CacheEntryEvent> arrivedEvts = new ArrayList<>();

        CountDownLatch latch = new CountDownLatch(range * 2);

        qry.setLocalListener(new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent e : evts) {
                    arrivedEvts.add(e);

                    latch.countDown();
                }
            }
        });

        QueryCursor<Cache.Entry<Integer, Integer>> cur = nearNode.cache(DEFAULT_CACHE_NAME).query(qry);

        // prevent first transaction prepare on backups
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(primary);

        spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            private final AtomicInteger limiter = new AtomicInteger();

            @Override public boolean apply(ClusterNode node, Message msg) {
                if (msg instanceof GridDhtTxPrepareRequest)
                    return limiter.getAndIncrement() < srvCnt - 1;

                return false;
            }
        });

        Transaction txA = primary.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

        for (int i = 0; i < range; i++)
            primary.cache(DEFAULT_CACHE_NAME).put(keys.get(i), 2);

        txA.commitAsync();

        GridTestUtils.runAsync(() -> {
            try (Transaction tx = primary.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                for (int i = range; i < range * 2; i++)
                    primary.cache(DEFAULT_CACHE_NAME).put(keys.get(i), 1);

                tx.commit();
            }
        }).get();

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return primary.context().cache().context().tm().activeTransactions().stream().allMatch(tx -> tx.state() == PREPARING);
            }
        }, 3_000);

        GridTestUtils.runAsync(() -> {
            try (Transaction txB = primary.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                for (int i = range * 2; i < range * 3; i++)
                    primary.cache(DEFAULT_CACHE_NAME).put(keys.get(i), 3);

                txB.commit();
            }
        }).get();

        long primaryUpdCntr = getUpdateCounter(primary, keys.get(0));

        //assertEquals(range * 3, primaryUpdCntr);

        // drop primary
        stopGrid(primary.name());

        // Wait all txs are rolled back.
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                boolean allRolledBack = true;

                for (int i = 1; i < srvCnt; i++) {
                    boolean rolledBack = grid(i).context().cache().context().tm().activeTransactions().stream().allMatch(tx -> tx.state() == ROLLED_BACK);

                    allRolledBack &= rolledBack;
                }

                return allRolledBack;
            }
        }, 3_000);

        for (int i = 1; i < srvCnt; i++) {
            IgniteCache backupCache = grid(i).cache(DEFAULT_CACHE_NAME);

            int size = backupCache.query(new SqlFieldsQuery("select * from Integer")).getAll().size();

            long backupCntr = getUpdateCounter(grid(i), keys.get(0));

            assertEquals(range * 2, size);
            assertEquals(primaryUpdCntr, backupCntr);
        }

        assertTrue(latch.await(5, SECONDS));

        assertEquals(range * 2, arrivedEvts.size());

        cur.close();
    }
}
