package org.apache.ignite.cache.affinity.rendezvous;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;

public class ChangeBackupFilterTest extends GridCommonAbstractTest {

    public static final String CELL_ATTR = "CELL";
    public static final String MEM_REGION_NAME = "mem";

    private boolean startWithCache = false;
    private boolean startWithCacheAnotherBackupFilter = false;

    private String cell;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
                .setConsistentId(igniteInstanceName)
                .setDataStorageConfiguration(new DataStorageConfiguration()
                        .setDataRegionConfigurations(new DataRegionConfiguration()
                                .setName(MEM_REGION_NAME))
                        .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                                .setMaxSize(100L * 1024 * 1024)
                                .setPersistenceEnabled(true)));

        if (startWithCache) {

            IgniteBiPredicate<ClusterNode, List<ClusterNode>> affBackupFilter = null;

            if(startWithCacheAnotherBackupFilter)
                affBackupFilter = new ClusterNodeAttributeFakeBackupFilter();
            else
                affBackupFilter = new ClusterNodeAttributeColocatedBackupFilter(CELL_ATTR);

            cfg.setUserAttributes(Collections.singletonMap(CELL_ATTR, cell))
                    .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME + "_" + MEM_REGION_NAME)
                                    .setDataRegionName(MEM_REGION_NAME),
                            new CacheConfiguration(DEFAULT_CACHE_NAME)
                                    .setBackups(2)
                                    .setAffinity(new RendezvousAffinityFunction(false, 16)
                                            .setAffinityBackupFilter(affBackupFilter)));
        }
        else {
            cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME + "_" + MEM_REGION_NAME)
                    .setDataRegionName(MEM_REGION_NAME));
        }

        return cfg;
    }

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    @Test
    public void test() throws Exception {
        startWithCache = false;

        Ignite ignite0 = startGrids(4);

        ignite0.cluster().active(true);

        awaitPartitionMapExchange();

        stopAllGrids();

        startWithCache = true;

        cell = "1";

        IgniteEx ignite0_c1 = startGrid(0);
        IgniteEx ignite1_c1 = startGrid(1);

        cell = "2";

        IgniteEx ignite2_c2 = startGrid(2);
        IgniteEx ignite3_c2 = startGrid(3);

        ignite0_c1.cluster().active(true);

        awaitPartitionMapExchange();

        IgniteCache cache = ignite1_c1.cache(DEFAULT_CACHE_NAME);
        Affinity aff = ignite1_c1.affinity(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 1_000; i++)
            cache.put(i, i);

        awaitPartitionMapExchange();

        stopAllGrids();

        log.info("Start with another filter");

        startWithCacheAnotherBackupFilter = true;

        cell = "1";

        ignite0_c1 = startGrid(0);
        ignite1_c1 = startGrid(1);

        cell = "2";

        ignite2_c2 = startGrid(2);
        ignite3_c2 = startGrid(3);

        log.info("Activate with another filter");

        ignite0_c1.cluster().active(true);

        log.info("Activate done another filter");

        awaitPartitionMapExchange();

        checkData(ignite0_c1);

        log.info("Test completed.");
    }

    private void printDistribution(Ignite ignite, String cacheName) {
        Affinity aff = ignite.affinity(cacheName);

        log.info("Distribution for cache " + cacheName);

        for (ClusterNode node : ignite.cluster().forServers().nodes()) {
            int[] parts = aff.allPartitions(node);

            log.info(node.consistentId() + ": " + S.compact(Arrays.stream(parts).boxed().collect(Collectors.toList())));
        }
    }

    private void stopDataCenter(int i2, int i3) throws Exception {
        ignite(i2).close();

        Thread.sleep(1_000);

        ignite(i3).close();

        cleanPersistenceDir(getTestIgniteInstanceName(i2));
        cleanPersistenceDir(getTestIgniteInstanceName(i3));
    }

    private void checkData(IgniteEx ignite0_c1) throws Exception {
        printDistribution(ignite0_c1, DEFAULT_CACHE_NAME);

        waitForRebalancing();
//        awaitPartitionMapExchange();

        IgniteCache cache = ignite0_c1.cache(DEFAULT_CACHE_NAME);
        Affinity aff = ignite0_c1.affinity(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 1_000; i++) {
            assertNotNull("The entry was lost [key=" + i +
                            ", p=" + aff.partition(i) +
                            ", node=" + aff.mapKeyToNode(i).attributes().get(ATTR_IGNITE_INSTANCE_NAME) + ']',
                    cache.get(i));
        }
    }

    protected void waitForRebalancing() throws IgniteCheckedException {
        for (Ignite ignite : G.allGrids())
            waitForRebalancing((IgniteEx)ignite, null);
    }

    /**
     * @param ignite Node.
     * @param top Topology version.
     * @throws IgniteCheckedException If failed.
     */
    protected void waitForRebalancing(IgniteEx ignite, AffinityTopologyVersion top) throws IgniteCheckedException {
        if (ignite.configuration().isClientMode())
            return;

        try {
            awaitPartitionMapExchange(false, false,
                    Collections.singleton(ignite.cluster().localNode()));
        } catch (InterruptedException e) {
            throw new IgniteCheckedException("Waiting of rebalance was interrupted.", e);
        }
    }

    private static class ClusterNodeAttributeFakeBackupFilter implements IgniteBiPredicate<ClusterNode, List<ClusterNode>> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean apply(ClusterNode clusterNode, List<ClusterNode> clusterNodes) {
            return false;
        }

    }

    private static class ClusterNodeAttributeColocatedBackupFilter implements IgniteBiPredicate<ClusterNode, List<ClusterNode>> {
        private static final long serialVersionUID = 1L;
        private final String attributeName;

        public ClusterNodeAttributeColocatedBackupFilter(String attributeName) {
            this.attributeName = attributeName;
        }

        @Override
        public boolean apply(ClusterNode candidate, List<ClusterNode> previouslySelected) {
            Iterator var3 = previouslySelected.iterator();

            if (candidate.attribute(this.attributeName) == null)
                System.err.println("Candidate node attr is " + candidate.attribute(this.attributeName));

            if (var3.hasNext()) {
                ClusterNode node = (ClusterNode)var3.next();

                if (node.attribute(this.attributeName) == null)
                    System.err.println("Attr is null!");

                return Objects.equals(candidate.attribute(this.attributeName), node.attribute(this.attributeName));
            }
            else {
                System.err.println("There is no primary here.");

                return true;
            }
        }
    }
}