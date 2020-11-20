package org.apache.ignite.internal.processors.cache.transactions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.CommunicationListener;
import org.apache.ignite.spi.communication.GridTestMessage;
import org.apache.ignite.spi.discovery.tcp.BlockTcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;


public class TxPartitionCounterStateConsistencyFuzzTest extends TxPartitionCounterStateAbstractTest {

    /**
     *
     */
    public static final int PARTITION_ID = 0;

    /**
     *
     */
    public static final int SERVER_NODES = 3;

    /**
     *
     */
    protected TcpDiscoverySpi customDiscoSpi;

    /**
     * {@inheritDoc}
     */
    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (customDiscoSpi != null) {
            cfg.setDiscoverySpi(customDiscoSpi);

            customDiscoSpi = null;
        }

        return cfg;
    }

    /**
     * Tests reproduces the problem: deferred removal queue should never be cleared during rebalance OR rebalanced
     * entries could undo deletion causing inconsistency.
     */
    @Test
    public void testPartitionConsistencyDuringRebalanceAndConcurrentUpdates_RemoveQueueCleared() throws Exception {

        customDiscoSpi = new BlockTcpDiscoverySpi().setIpFinder(IP_FINDER);

        Field rndAddrsField = U.findField(BlockTcpDiscoverySpi.class, "skipAddrsRandomization");
        assertNotNull(rndAddrsField);
        rndAddrsField.set(customDiscoSpi, true);


        backups = 2;

        Ignite prim = startGridsMultiThreaded(SERVER_NODES);

        int[] primaryParts = prim.affinity(DEFAULT_CACHE_NAME).primaryPartitions(prim.cluster().localNode());

        List<Integer> keys = partitionKeys(prim.cache(DEFAULT_CACHE_NAME), primaryParts[0], 2, 0);

        prim.cache(DEFAULT_CACHE_NAME).put(keys.get(0), keys.get(0));

        forceCheckpoint();

        List<Ignite> backups = backupNodes(keys.get(0), DEFAULT_CACHE_NAME);

        assertFalse(backups.contains(prim));

        stopGrid(true, backups.get(0).name());

        prim.cache(DEFAULT_CACHE_NAME).put(keys.get(0), keys.get(0));

        TestRecordingCommunicationSpi spiPrimary = TestRecordingCommunicationSpi.spi(backups.get(1));
        TestRecordingCommunicationSpi spiBack = TestRecordingCommunicationSpi.spi(backups.get(1));

        spiBack.setListener(new TestListener());


        spiBack.blockMessages((node, msg) -> msg instanceof GridDhtPartitionSupplyMessage);

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            try {
                GridTestUtils.waitForCondition(() -> spiBack.hasBlockedMessages(), 30 * 10_000);
            } catch (Exception e) {
                fail(X.getFullStackTrace(e));
            }

            customDiscoSpi.disconnect();

            prim.cache(DEFAULT_CACHE_NAME).remove(keys.get(0));

            doSleep(1000);

            customDiscoSpi.clientReconnect();

            // Ensure queue cleanup is triggered before releasing supply message.
            spiBack.stopBlock();
        });

        startGrid(backups.get(0).name());

        awaitPartitionMapExchange();


        Random r = new Random();
        AtomicBoolean stop = new AtomicBoolean();

        final IgniteInternalFuture<?> fut1 = doRandomUpdates(r,
                prim,
                IntStream.range(0, 1000).boxed().collect(toList()),
                prim.cache(DEFAULT_CACHE_NAME),
                stop::get);

        fut.get();
        fut1.get();

        assertPartitionsSame(idleVerify(prim, DEFAULT_CACHE_NAME));

        assertCountersSame(PARTITION_ID, true);
    }


    private class TestListener implements CommunicationListener<Message> {
        /**
         *
         */
        private GridConcurrentHashSet<Long> msgIds = new GridConcurrentHashSet<>();

        /**
         *
         */
        private AtomicInteger rcvCnt = new AtomicInteger();

        /**
         * {@inheritDoc}
         */
        @Override
        public void onMessage(UUID nodeId, Message msg, IgniteRunnable msgC) {
            info("Test listener received message: " + msg);

            assertTrue("Unexpected message: " + msg, msg instanceof GridTestMessage);

            GridTestMessage msg0 = (GridTestMessage) msg;

            assertTrue("Duplicated message received: " + msg0, msgIds.add(msg0.getMsgId()));

            rcvCnt.incrementAndGet();

            msgC.run();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onDisconnected(UUID nodeId) {
            // No-op.
        }
    }


    protected IgniteInternalFuture<?> doRandomUpdates(Random r, Ignite near, List<Integer> primaryKeys,
                                                      IgniteCache<Object, Object> cache, BooleanSupplier stopClo) throws Exception {
        LongAdder puts = new LongAdder();
        LongAdder removes = new LongAdder();

        final int max = 100;

        return multithreadedAsync(() -> {
            while (!stopClo.getAsBoolean()) {
                int rangeStart = r.nextInt(primaryKeys.size() - max);
                int range = 5 + r.nextInt(max - 5);

                List<Integer> keys = primaryKeys.subList(rangeStart, rangeStart + range);

                try (Transaction tx = near.transactions().txStart(concurrency(), REPEATABLE_READ, 0, 0)) {
                    List<Integer> insertedKeys = new ArrayList<>();

                    for (Integer key : keys) {
                        cache.put(key, key);
                        insertedKeys.add(key);

                        puts.increment();

                        boolean rmv = r.nextFloat() < 0.4;
                        if (rmv) {
                            key = insertedKeys.get(r.nextInt(insertedKeys.size()));

                            cache.remove(key);

                            insertedKeys.remove(key);

                            removes.increment();
                        }
                    }

                    if (r.nextFloat() < 0.1)
                        tx.rollback();
                    else
                        tx.commit();
                } catch (Exception e) {
                    assertTrue(X.getFullStackTrace(e), X.hasCause(e, ClusterTopologyException.class) ||
                            X.hasCause(e, ClusterTopologyCheckedException.class) ||
                            X.hasCause(e, TransactionRollbackException.class) ||
                            X.hasCause(e, CacheInvalidStateException.class));
                }
            }

            log.info("TX: puts=" + puts.sum() + ", removes=" + removes.sum() + ", size=" + cache.size());

        }, Runtime.getRuntime().availableProcessors() * 2, "tx-update-thread");
    }


}
