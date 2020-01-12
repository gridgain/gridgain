package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.List;

public class CacheRebalanceOnCompatibleTopologyTest extends GridCommonAbstractTest {
    /** Persistent cache. */
    public static final String PERSISTENT = "persistent";

    /** Volatile cache. */
    public static final String VOLATILE = "volatile";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setFailureDetectionTimeout(100000000L);
        cfg.setClientFailureDetectionTimeout(100000000L);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setWalSegmentSize(4 * 1024 * 1024)
                .setCheckpointFrequency(100000000L)
                .setPageSize(1024)
                .setDataRegionConfigurations(
                        new DataRegionConfiguration().setName(PERSISTENT).setPersistenceEnabled(true).setMaxSize(50 * 1024 * 1024),
                        new DataRegionConfiguration().setName(VOLATILE).setMaxSize(100 * 1024 * 1024)));

        cfg.setCacheConfiguration(
                new CacheConfiguration(PERSISTENT).setDataRegionName(PERSISTENT).setBackups(1).
                        setAffinity(new RendezvousAffinityFunction(false, 64)),
                new CacheConfiguration(VOLATILE).setRebalanceOrder(10).setDataRegionName(VOLATILE).setBackups(1).
                        setAffinity(new RendezvousAffinityFunction(false, 64))
        );

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
     * @throws Exception If failed.
     */
    @Test
    public void testMixedCaches() throws Exception {
        try {
            IgniteEx crd = startGrids(2);
            crd.cluster().active(true);

            stopGrid(1);

            int movingPart = 0;

            crd.cache(PERSISTENT).put(0, 0);
            crd.cache(VOLATILE).put(movingPart, 0);

            List<Integer> moving = movingKeysAfterJoin(crd, VOLATILE, 1);

            assertEquals(movingPart , moving.get(0).intValue());

            TestRecordingCommunicationSpi.spi(crd).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                    if (msg instanceof GridDhtPartitionSupplyMessage) {
                        GridDhtPartitionSupplyMessage m = (GridDhtPartitionSupplyMessage) msg;

                        return m.groupId() == CU.cacheId(PERSISTENT);
                    }

                    return false;
                }
            });

            startGrid(1);

            TestRecordingCommunicationSpi.spi(crd).waitForBlocked();

            startGrid(2);

            TestRecordingCommunicationSpi.spi(crd).stopBlock();

            awaitPartitionMapExchange();

            printPartitionState(crd.cache(VOLATILE));

            crd.cluster().active(false);
        }
        finally {
            stopAllGrids();
        }
    }
}
