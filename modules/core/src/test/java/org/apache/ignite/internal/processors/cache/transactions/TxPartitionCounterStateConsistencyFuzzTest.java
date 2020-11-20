package org.apache.ignite.internal.processors.cache.transactions;

import java.lang.reflect.Field;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.BlockTcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;


public class TxPartitionCounterStateConsistencyFuzzTest extends TxPartitionCounterStateAbstractTest {

    /** */
    public static final int PARTITION_ID = 0;

    /** */
    public static final int SERVER_NODES = 3;

    /** */
    protected TcpDiscoverySpi customDiscoSpi;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
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

        TestRecordingCommunicationSpi spiBack = TestRecordingCommunicationSpi.spi(backups.get(1));

        spiBack.blockMessages((node, msg) -> msg instanceof GridDhtPartitionSupplyMessage);

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            try {
                GridTestUtils.waitForCondition(() -> spiBack.hasBlockedMessages(), 30 * 10_000);
            }
            catch (Exception e) {
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

        fut.get();

        assertPartitionsSame(idleVerify(prim, DEFAULT_CACHE_NAME));

        assertCountersSame(PARTITION_ID, true);
    }



}
