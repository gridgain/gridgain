package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class NodeJoinWithNewCachesTest extends GridCommonAbstractTest {
    /** Initial nodes. */
    private static final int INITIAL_NODES = 2;

    /** Caches. */
    private boolean spawnNewCaches;

    /** Client. */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(256 * 1024 * 1024)
                    .setPersistenceEnabled(false))
        );

        int cachesCnt = INITIAL_NODES;

        if (spawnNewCaches)
            cachesCnt += getTestIgniteInstanceIndex(igniteInstanceName) + 1;

        cfg.setCacheConfiguration(cacheConfiguration("cache-", cachesCnt));

        cfg.setClientMode(client);

        cfg.setActiveOnStart(false);

        return cfg;
    }

    CacheConfiguration[] cacheConfiguration(String prefix, int number) {
        return cacheConfiguration(prefix, null ,number);
    }

    CacheConfiguration[] cacheConfiguration(String prefix, String groupName, int number) {
        CacheConfiguration[] ccfgs = new CacheConfiguration[number];
        for (int i = 0; i < number; i++) {
            ccfgs[i] = new CacheConfiguration(prefix + i)
                .setGroupName(groupName)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setBackups(1);
        }
        return ccfgs;
    }

    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test(timeout = 10005000L)
    public void testNodeJoin() throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(INITIAL_NODES);

        crd.cluster().active(true);

        awaitPartitionMapExchange();

        spawnNewCaches = true;

        startGrid(2);

        int ALL_NODES = INITIAL_NODES + 1;

        for (int nodeIdx = ALL_NODES - 1; nodeIdx >= 0; nodeIdx--)
            for (int cacheId = INITIAL_NODES + ALL_NODES - 1; cacheId >= 0; cacheId--)
                grid(nodeIdx).cache("cache-" + cacheId).get(0);
    }

    @Test(timeout = 10005000L)
    public void testMultipleNodeJoin() throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(INITIAL_NODES);

        crd.cluster().active(true);

        awaitPartitionMapExchange();

        spawnNewCaches = true;

        startGridsMultiThreaded(INITIAL_NODES, 3);

        int ALL_NODES = INITIAL_NODES + 3;

        for (int nodeIdx = ALL_NODES - 1; nodeIdx >= 0; nodeIdx--)
            for (int cacheId = INITIAL_NODES + ALL_NODES - 1; cacheId >= 0; cacheId--)
                grid(nodeIdx).cache("cache-" + cacheId).get(0);
    }

    @Test(timeout = 10005000L)
    public void testMultipleNodeJoinClient() throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(INITIAL_NODES);

        crd.cluster().active(true);

        awaitPartitionMapExchange();

        spawnNewCaches = true;

        client = true;

        startGridsMultiThreaded(INITIAL_NODES, 3);

        int ALL_NODES = INITIAL_NODES + 3;

        for (int nodeIdx = ALL_NODES - 1; nodeIdx >= 0; nodeIdx--)
            for (int cacheId = INITIAL_NODES + ALL_NODES - 1; cacheId >= 0; cacheId--)
                grid(nodeIdx).cache("cache-" + cacheId).get(0);
    }
}
