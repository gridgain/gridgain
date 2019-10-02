package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 */
public class CacheRebalanceUncheckedErrorHandlingTest extends GridCommonAbstractTest {
//    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
//        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
//    }

    @Test
    public void testRebalanceUncheckedError() throws Exception {
        IgniteEx ignite0 = startGrid(new IgniteConfiguration().setIgniteInstanceName("ignite0"));

        IgniteCache<Integer, Integer> cache = ignite0.getOrCreateCache(DEFAULT_CACHE_NAME);

        IgniteDataStreamer<Integer, Integer> streamer = ignite0.dataStreamer(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100_000; i++)
            streamer.addData(i, i);

        streamer.flush();

        IgniteEx ignite1 = startGrid(new IgniteConfiguration().setIgniteInstanceName("ignite1")
            .setIncludeEventTypes(EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED));

        ignite1.events().localListen(e -> {
            throw new Error();
        }, EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED);

        awaitPartitionMapExchange();
    }
}
