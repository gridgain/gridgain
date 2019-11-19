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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.client.util.GridConcurrentHashSet;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;

/**
 */
public class AtomicUpdateCounterStateTest extends GridCommonAbstractTest {
    /** IP finder. */
    protected static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int MB = 1024 * 1024;

    /** */
    private int backups = 1;

    /** */
    private static final int PARTS = 32;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureDetectionTimeout(1000000000L);
        cfg.setClientFailureDetectionTimeout(1000000000L);

        cfg.setConsistentId(igniteInstanceName);
        //cfg.setFailureHandler(new StopNodeFailureHandler());
        //cfg.setRebalanceThreadPoolSize(4); // Necessary to reproduce some issues.

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        boolean client = igniteInstanceName.startsWith("client");

        cfg.setClientMode(client);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC).
            setBackups(backups).
            setAtomicityMode(CacheAtomicityMode.ATOMIC).
            setAffinity(new RendezvousAffinityFunction(false, PARTS)));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().
            setWalHistorySize(1000).
            setWalSegmentSize(8 * MB).setWalMode(LOG_ONLY).setPageSize(1024).
            setCheckpointFrequency(MILLISECONDS.convert(365, DAYS)). // All checkpoints will be manual.
            setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(persistenceEnabled()).
            setInitialSize(100 * MB).setMaxSize(100 * MB)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /**
     * Test scenario:
     * 1. Start atomic update.
     * 2. Delay message to one of backups.
     * 3. Stop primary node.
     *
     * Success: backups are in sync.
     */
    @Test
    public void testPrimaryFail() throws Exception {
        doTestPrimaryFail();
    }

    /**
     * Test scenario:
     *
     * 1. Start two atomic updates.
     */
    @Test
    public void testSinglePutReorderBackupFail() throws Exception {
        doTestBackupFail();
    }

    @Test
    public void testPutAllReorderBackupFailNearPrimary() throws Exception {
        doTestReorderBackupRestartPutAll(new Supplier<Ignite>() {
            @Override public Ignite get() {
                return grid(0);
            }
        });
    }

    @Test
    public void testPutAllReorderBackupFailNearClient() throws Exception {
        doTestReorderBackupRestartPutAll(new Supplier<Ignite>() {
            @Override public Ignite get() {
                return grid("client");
            }
        });
    }

    private void doTestBackupFail() throws Exception {
        backups = 1;

        try {
            IgniteEx crd = startGrids(backups + 1);

            crd.cluster().active(true);

            IgniteEx client = startGrid("client");

            assertNotNull(client.cache(DEFAULT_CACHE_NAME));

            IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

            final int part = crd.affinity(DEFAULT_CACHE_NAME).primaryPartitions(crd.localNode())[1];

            List<Integer> keys = partitionKeys(cache, part, 10, 0);

            Set<Thread> senderThreads = new GridConcurrentHashSet<>();

            TestRecordingCommunicationSpi.spi(crd).blockMessages((node, msg) -> {
                if (msg instanceof GridDhtAtomicSingleUpdateRequest) {
                    senderThreads.add(Thread.currentThread());

                    GridDhtAtomicSingleUpdateRequest r = (GridDhtAtomicSingleUpdateRequest)msg;

                    return r.updateCntr == 1;
                }

                return false;
            });

            IgniteInternalFuture putFut = GridTestUtils.runAsync(() -> {
                try {
                    cache.put(keys.get(0), keys.get(0));
                }
                catch (Exception e) {
                    assertTrue(X.hasCause(e, NodeStoppingException.class));
                }
            });

            TestRecordingCommunicationSpi.spi(crd).waitForBlocked();

            // Update out of counter order.
            cache.put(keys.get(1), keys.get(1));

            forceCheckpoint();

            // IMPORTANT: reordering is possible only if messages are send to backup from different threads.
            assertEquals(2, senderThreads.size());

            stopGrid(1);

            TestRecordingCommunicationSpi.spi(crd).stopBlock();

            putFut.get();

            startGrid(1);

            awaitPartitionMapExchange();

            assertCountersSame(part, false);

            assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
        }
        finally {
            stopAllGrids();
        }
    }

    private void doTestReorderBackupRestartPutAll(Supplier<Ignite> nodeSupplier) throws Exception {
        backups = 1;

        try {
            IgniteEx crd = startGrids(backups + 1);

            crd.cluster().active(true);

            IgniteEx client = startGrid("client");

            assertNotNull(client.cache(DEFAULT_CACHE_NAME));

            List<Integer> keys = primaryKeys(crd.cache(DEFAULT_CACHE_NAME), 10, 0);

            Set<Thread> senderThreads = new GridConcurrentHashSet<>();

            TestRecordingCommunicationSpi.spi(crd).blockMessages((node, msg) -> {
                if (msg instanceof GridDhtAtomicUpdateRequest) {
                    senderThreads.add(Thread.currentThread());

                    GridDhtAtomicUpdateRequest r = (GridDhtAtomicUpdateRequest)msg;

                    return r.updateCounter(1) == 1;
                }

                return false;
            });

            IgniteInternalFuture putFut = GridTestUtils.runAsync(() -> {
                try {
                    Map<Integer, Integer> m0 = new LinkedHashMap<>();
                    m0.put(keys.get(0), keys.get(0));
                    m0.put(keys.get(2), keys.get(2));

                    nodeSupplier.get().cache(DEFAULT_CACHE_NAME).putAll(m0);
                }
                catch (Exception e) {
                    assertTrue(X.hasCause(e, NodeStoppingException.class));
                }
            });

            TestRecordingCommunicationSpi.spi(crd).waitForBlocked();

            // Update out of counter order.
            Map<Integer, Integer> m1 = new LinkedHashMap<>();
            m1.put(keys.get(1), keys.get(1));
            m1.put(keys.get(2) + PARTS, keys.get(2) + PARTS);

            nodeSupplier.get().cache(DEFAULT_CACHE_NAME).putAll(m1);

            forceCheckpoint();

            // IMPORTANT: reordering is possible only if messages are send to backup from different threads.
            assertEquals(2, senderThreads.size());

            stopGrid(1);

            TestRecordingCommunicationSpi.spi(crd).stopBlock();

            putFut.get();

            startGrid(1);

            awaitPartitionMapExchange();

            assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
        }
        finally {
            stopAllGrids();
        }
    }

    private void doTestPrimaryFail() throws Exception {
        backups = 2;

        try {
            IgniteEx crd = startGrids(backups + 1);

            crd.cluster().active(true);

            IgniteEx client = startGrid("client");

            assertNotNull(client.cache(DEFAULT_CACHE_NAME));

            IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

            final int part = crd.affinity(DEFAULT_CACHE_NAME).primaryPartitions(crd.localNode())[1];

            List<Integer> keys = partitionKeys(cache, part, 10, 0);

            TestRecordingCommunicationSpi.spi(crd).blockMessages((node, msg) -> {
                return msg instanceof GridDhtAtomicSingleUpdateRequest;
            });

            IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
                try {
                    cache.put(keys.get(0), keys.get(0));
                }
                catch (Exception e) {
                    assertTrue(X.hasCause(e, NodeStoppingException.class));
                }
            });

            TestRecordingCommunicationSpi.spi(crd).waitForBlocked(2);

            TestRecordingCommunicationSpi.spi(crd).stopBlock(true, new IgnitePredicate<T2<ClusterNode, GridIoMessage>>() {
                boolean first = true;

                @Override public boolean apply(T2<ClusterNode, GridIoMessage> objects) {
                    if (first) {
                        first = false;

                        return true;
                    }

                    return false;
                }
            });

            assertTrue(TestRecordingCommunicationSpi.spi(crd).hasBlockedMessages());

            doSleep(1000);

            crd.close();

            fut.get();

            assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));
        }
        finally {
            stopAllGrids();
        }
    }

    @Test
    public void testSinglePrimaryPutAll() throws Exception {
        backups = 1;

        try {
            IgniteEx crd = startGrids(2);

            crd.cluster().active(true);

            IgniteEx client = startGrid("client");

            Map<Integer, Integer> data = new LinkedHashMap<>();

            for (int i = 0; i < PARTS; i++)
                data.put(i, i);

            client.cache(DEFAULT_CACHE_NAME).putAll(data);

            for (int i = 0; i < PARTS; i++)
                assertEquals(i, client.cache(DEFAULT_CACHE_NAME).get(i));

            GridDhtLocalPartition part = crd.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(0);

            assertEquals(0, part.internalSize());

            assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

            checkFutures();

            client.cache(DEFAULT_CACHE_NAME).removeAll(data.keySet());

            part = crd.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(0);

            assertEquals(0, part.dataStore().fullSize());

            assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

            checkFutures();
        }
        finally {
            stopAllGrids();
        }
    }

    @Test
    public void testSinglePrimaryPutGet() throws Exception {
        backups = 1;

        try {
            IgniteEx crd = startGrids(2);

            crd.cluster().active(true);

            IgniteEx client = startGrid("client");

            client.cache(DEFAULT_CACHE_NAME).put(9, 0);

            assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

            Object prev = client.cache(DEFAULT_CACHE_NAME).getAndPut(9, 1);

            assertEquals(0, prev);

            assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

            GridDhtLocalPartition part = crd.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(0);

            assertEquals(0, part.internalSize());

            assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

            checkFutures();

            client.cache(DEFAULT_CACHE_NAME).remove(0);

            part = crd.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(0);

            assertEquals(0, part.dataStore().fullSize());

            assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

            checkFutures();
        }
        finally {
            stopAllGrids();
        }
    }

    @Test
    public void testLoad() throws Exception {
        backups = 1;

        try {
            IgniteEx crd = startGrids(2);

            crd.cluster().active(true);

            IgniteEx client = startGrid("client");

            Random r = new Random(1000);

            List<Integer> keys = IntStream.range(0, 1000).boxed().collect(Collectors.toList());

            IgniteInternalFuture<?> fut = doRandomUpdates(r, keys, client.cache(DEFAULT_CACHE_NAME), U.currentTimeMillis() + 20_000);

            fut.get();

            assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

            checkFutures();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @return {@code True} if persistence is enabled for tests.
     */
    private boolean persistenceEnabled() {
        return true;
    }

    /**
     * @param r Random.
     * @param primaryKeys Primary keys.
     * @param cache Cache.
     * @param stop Stop.
     */
    private IgniteInternalFuture<?> doRandomUpdates(
        Random r,
        List<Integer> primaryKeys,
        IgniteCache<Object, Object> cache,
        long stop) throws Exception {
        LongAdder puts = new LongAdder();
        LongAdder removes = new LongAdder();

        final int max = 100;

        return multithreadedAsync(() -> {
            //while (U.currentTimeMillis() < stop) {
                int rangeStart = r.nextInt(primaryKeys.size() - max);
                int range = 5 + r.nextInt(max - 5);

                List<Integer> keys = primaryKeys.subList(rangeStart, rangeStart + range);

                List<Integer> insertedKeys = new ArrayList<>();
                List<Integer> rmvKeys = new ArrayList<>();

                boolean batch = r.nextFloat() > 0.3;

                for (Integer key : keys) {
                    if (!batch)
                        cache.put(key, key);

                    insertedKeys.add(key);

                    puts.increment();
                }

                if (batch)
                    cache.putAll(insertedKeys.stream().collect(Collectors.toMap(k -> k, v -> v, (o, o2) -> o, LinkedHashMap::new)));

                for (Integer key : keys) {
                    boolean rmv = r.nextFloat() < 0.4;
                    if (rmv) {
                        key = insertedKeys.get(r.nextInt(insertedKeys.size()));

                        if (!batch)
                            cache.remove(key);

                        rmvKeys.add(key);

                        removes.increment();
                    }
                }

                if (batch)
                    cache.removeAll(new LinkedHashSet<Object>(rmvKeys));
            //}

            log.info("Atomic: puts=" + puts.sum() + ", removes=" + removes.sum() + ", size=" + cache.size());

        }, 1, "atomic-update-thread");
    }
}
