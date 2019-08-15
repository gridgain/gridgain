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
    /** Caches. */
    private int caches = 5;

    private boolean client = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(256 * 1024 * 1024))
        );

        cfg.setCacheConfiguration(cacheConfiguration("cache-", caches));


        cfg.setClientMode(client);

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
        IgniteEx crd = startGrids(2);

        crd.cluster().active(true);

        awaitPartitionMapExchange();

        caches = 7;

        IgniteEx grid = startGrid(2);

        grid.cache("cache-6").get(0);

        awaitPartitionMapExchange();

        int k = 2;
    }

    @Test(timeout = 10005000L)
    public void testMultipleNodeJoin() throws Exception {
        IgniteEx crd = startGrids(2);

        crd.cluster().active(true);

        awaitPartitionMapExchange();

        caches = 7;

        IgniteEx grid = (IgniteEx) startGridsMultiThreaded(2, 3);

        for (int i = 2; i < 2 + 3; i++)
            grid(i).cache("cache-6").get(0);
    }

    @Test(timeout = 10005000L)
    public void testMultipleNodeJoinClient() throws Exception {
        IgniteEx crd = startGrids(2);

        crd.cluster().active(true);

        awaitPartitionMapExchange();

        caches = 7;

        client = true;

        startGridsMultiThreaded(2, 3);

        for (int i = 2; i < 2 + 3; i++)
            grid(i).cache("cache-6").get(0);
    }
}
