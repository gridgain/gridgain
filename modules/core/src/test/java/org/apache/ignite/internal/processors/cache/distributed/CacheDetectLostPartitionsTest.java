package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class CacheDetectLostPartitionsTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_CACHE_NAME = "testcache";

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Test detect lost partitions on a client node when the cache init after partitions was lost.
     * @throws Exception
     */
    @Test
    public void testDetectLostPartitionedOnClient() throws Exception {
        IgniteEx ig = startGrids(2);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = ig.getOrCreateCache(
            new CacheConfiguration<>(TEST_CACHE_NAME)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE)
        );

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        IgniteEx client = startClientGrid(2);

        stopGrid(1);

        assertFalse(client.cache(TEST_CACHE_NAME)
            .lostPartitions()
            .isEmpty());
    }
}
