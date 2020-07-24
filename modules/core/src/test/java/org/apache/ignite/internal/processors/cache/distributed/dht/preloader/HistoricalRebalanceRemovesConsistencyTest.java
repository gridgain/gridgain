package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.resource.DependencyResolver;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.TestDependencyResolver;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_REMOVED_ENTRIES_TTL;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;

/**
 * Checks that historical rebalancing of removes doesn't break partition consistency.
 */
@WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
@WithSystemProperty(key = IGNITE_CACHE_REMOVED_ENTRIES_TTL, value = "500")
@RunWith(Parameterized.class)
public class HistoricalRebalanceRemovesConsistencyTest extends GridCommonAbstractTest {
    /** Initial keys. */
    private static final int INITIAL_KEYS = 5000;

    /** Put remove keys. */
    private static final int PUT_REMOVE_KEYS = 2000;

    /** Atomicity mode. */
    @Parameterized.Parameter()
    public CacheAtomicityMode atomicityMode;

    /**
     * @return List of versions pairs to test.
     */
    @Parameterized.Parameters(name = "atomicityMode = {0}")
    public static Collection<Object[]> testData() {
        List<Object[]> res = new ArrayList<>();

        res.add(new Object[] {CacheAtomicityMode.ATOMIC});
        res.add(new Object[] {CacheAtomicityMode.TRANSACTIONAL});

        return res;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setCacheConfiguration(new CacheConfiguration()
            .setAffinity(new RendezvousAffinityFunction(false, 8))
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        // Allows to reproduce issues with concurrent entry updates
        cfg.setRebalanceThreadPoolSize(4);

        cfg.setRebalanceBatchSize(1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Performs historical rebalancing of load with same key put-remove pattern.
     * Checks that:
     * 1. Partitions are not diverged after rebalancing is finished.
     * 2. There is no cache entries memory leak.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPutRemoveReorderingConsistency() throws Exception {
        IgniteEx grid = startGrids(2);

        grid.cluster().state(ClusterState.ACTIVE);

        IgniteInternalCache<Integer, Integer> cache = grid.cachex(DEFAULT_CACHE_NAME);

        for (int i = 0; i < INITIAL_KEYS; i++)
            cache.put(i, i);

        forceCheckpoint();

        stopGrid(1);

        for (int i = INITIAL_KEYS; i < INITIAL_KEYS + PUT_REMOVE_KEYS; i++) {
            cache.put(i, i);

            cache.remove(i, i);
        }

        List<ConcurrentMap<?, ?>> locPartMaps = Collections.synchronizedList(new ArrayList<>());

        TestDependencyResolver rslvr = new TestDependencyResolver(new DependencyResolver() {
            @Override public <T> T resolve(T instance) {
                if (instance instanceof GridCacheConcurrentMap.CacheMapHolder) {
                    GridCacheConcurrentMap.CacheMapHolder hld = (GridCacheConcurrentMap.CacheMapHolder)instance;

                    if (DEFAULT_CACHE_NAME.equals(hld.cctx.name()))
                        locPartMaps.add(hld.map);
                }

                return instance;
            }
        });

        startGrid(1, rslvr);

        awaitPartitionMapExchange(true, true, null);

        assertEquals(grid(1).cachex(DEFAULT_CACHE_NAME).context().topology().localPartitions().size(),
            locPartMaps.size());

        assertTrue("Memory leak detected: some cache entries linger on heap",
            GridTestUtils.waitForCondition(() -> locPartMaps.stream().allMatch(Map::isEmpty), 10_000));

        assertPartitionsSame(idleVerify(grid, DEFAULT_CACHE_NAME));
    }
}
