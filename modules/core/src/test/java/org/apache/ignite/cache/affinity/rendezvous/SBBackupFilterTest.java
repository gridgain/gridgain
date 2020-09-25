package org.apache.ignite.cache.affinity.rendezvous;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionBackupFilterAbstractSelfTest;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import java.util.Collection;
import java.util.Objects;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

public class SBBackupFilterTest extends AffinityFunctionBackupFilterAbstractSelfTest {

    @Override
    protected AffinityFunction affinityFunction() {
        RendezvousAffinityFunction aff = new RendezvousAffinityFunction(false);

        aff.setBackupFilter(backupFilter);

        return aff;
    }

    @Override
    protected AffinityFunction affinityFunctionWithAffinityBackupFilter(String attributeName) {
        RendezvousAffinityFunction aff = new RendezvousAffinityFunction(false);

        aff.setAffinityBackupFilter(new ClusterNodeAttributeColocatedBackupFilter(attributeName));

        return aff;
    }

    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration ic = super.getConfiguration(igniteInstanceName);
        //TODO: fix [0]
        CacheConfiguration cacheCfg = ic.getCacheConfiguration()[0];
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
        cacheCfg.setBackups(4);
        return ic;
    }

    @Test
    @Override
    public void testPartitionDistributionWithAffinityBackupFilter() throws Exception {
        int nodes = 4;

        int CACHE_SIZE = 10000;

        try {

            startGrids(nodes);

            grid(0).cluster().baselineAutoAdjustEnabled(false);
            grid(0).cluster().active(true);

            for (int j = 0; j < CACHE_SIZE; j++)
                grid(0).cache(DEFAULT_CACHE_NAME).put(j, "Value" + j);

            awaitPartitionMapExchange();

            stopGrid(0);
            stopGrid(3);

            awaitPartitionMapExchange();

            log.info("Test lost partitions");

            assertStatePartitions(2, GridDhtPartitionState.OWNING);
            assertStatePartitions(1, GridDhtPartitionState.OWNING);

            startGrid(0);
            startGrid(3);

            awaitPartitionMapExchange();

            assertStatePartitions(0, GridDhtPartitionState.OWNING);
            assertStatePartitions(1, GridDhtPartitionState.OWNING);
            assertStatePartitions(2, GridDhtPartitionState.OWNING);
            assertStatePartitions(3, GridDhtPartitionState.OWNING);

            stopGrid(1);
            stopGrid(2);

            assertStatePartitions(0, GridDhtPartitionState.OWNING);
            assertStatePartitions(3, GridDhtPartitionState.OWNING);


            startGrid(1);
            startGrid(2);

            awaitPartitionMapExchange();

            assertStatePartitions(0, GridDhtPartitionState.OWNING);
            assertStatePartitions(1, GridDhtPartitionState.OWNING);
            assertStatePartitions(2, GridDhtPartitionState.OWNING);
            assertStatePartitions(3, GridDhtPartitionState.OWNING);
        }
        finally {
            stopAllGrids();
        }
    }

    @Test
    public void testPartitionDistributionWithAffinityBackupFilterSB() throws Exception {
        int nodes = 40;

        int CACHE_SIZE = 400000;

        try {

            startGrids(nodes);

            grid(0).cluster().active(true);

            for (int j = 0; j < CACHE_SIZE; j++)
                grid(0).cache(DEFAULT_CACHE_NAME).put(j, "Value" + j);

            awaitPartitionMapExchange();

            stopGrid(0);
            stopGrid(3);

            awaitPartitionMapExchange();

            log.info("Test lost partitions");

            startGrid(1);
            startGrid(2);

            awaitPartitionMapExchange();

            assertStatePartitions(0, GridDhtPartitionState.OWNING);
            assertStatePartitions(1, GridDhtPartitionState.OWNING);
            assertStatePartitions(2, GridDhtPartitionState.OWNING);
            assertStatePartitions(3, GridDhtPartitionState.OWNING);
        }
        finally {
            stopAllGrids();
        }
    }

    private void assertStatePartitions(int idx, GridDhtPartitionState state) {
        assertTrue(Objects.requireNonNull(
                grid(idx).cachex(DEFAULT_CACHE_NAME)).context().topology().localPartitions().stream().allMatch(
                p -> p.state() == state));
    }

}
