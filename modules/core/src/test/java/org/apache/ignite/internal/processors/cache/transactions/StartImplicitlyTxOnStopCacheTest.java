package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class StartImplicitlyTxOnStopCacheTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        startGrid(0);

        IgniteEx client1 = startClientGrid("client1");

        IgniteCache<Object, Object> cache = client1.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        TestRecordingCommunicationSpi commSpiClient1 = TestRecordingCommunicationSpi.spi(client1);

        commSpiClient1.blockMessages(GridNearTxPrepareRequest.class, getTestIgniteInstanceName(0));

        commSpiClient1.record((node, msg) -> msg instanceof GridDhtPartitionsSingleMessage);

        IgniteInternalFuture runTxFut = GridTestUtils.runAsync(() -> cache.put(100, 100));

        IgniteInternalFuture destroyCacheFut = GridTestUtils.runAsync(() ->
            client1.destroyCache(DEFAULT_CACHE_NAME));

        commSpiClient1.waitForBlocked();

        commSpiClient1.waitForRecorded();

        commSpiClient1.stopBlock();

        destroyCacheFut.get();

        try {
            runTxFut.get();
        }
        catch (Exception e) {
            log.error("Exception during implicitly transaction.", e);
        }

        awaitPartitionMapExchange();
    }
}
