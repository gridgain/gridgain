package org.apache.ignite.internal.processors.cache.distributed;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.twostep.PartitionReservationManager;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class ReservationTest extends GridCommonAbstractTest {
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setFailureDetectionTimeout(1000000L);
        cfg.setClientFailureDetectionTimeout(1000000L);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setIndexedTypes(Integer.class, Integer.class)
            .setAffinity(new RendezvousAffinityFunction(false, 32)).setBackups(1));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    @Test
    public void test() throws Exception {
        IgniteEx crd = startGrids(2);

        awaitPartitionMapExchange();

        for (int i = 0; i < 32 * 5; i++)
            crd.cache(DEFAULT_CACHE_NAME).put(i, i);

        IgniteConfiguration cfg2 = getConfiguration(getTestIgniteInstanceName(2));
        TestRecordingCommunicationSpi spi2 = (TestRecordingCommunicationSpi) cfg2.getCommunicationSpi();

        spi2.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                if (msg instanceof GridDhtPartitionsSingleMessage) {
                    GridDhtPartitionsSingleMessage tmp = (GridDhtPartitionsSingleMessage) msg;

                    return tmp.exchangeId() == null && tmp.partitions().containsKey(CU.cacheId(DEFAULT_CACHE_NAME));
                }

                return false;
            }
        });

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> run = multithreadedAsync(new Runnable() {
            @Override public void run() {
                while(!stop.get()) {
                    SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM Integer");
                    //qry.setPageSize(1);

                    try {
                        List<List<?>> rows = grid(0).cache(DEFAULT_CACHE_NAME).query(qry).getAll();

                        assertEquals(32 * 5, rows.size());
                    } catch (Exception e) {
                        // Ignore.
                    }
                }
            }
        }, 1, "qry-thread");

        doSleep(100);

        IgniteInternalFuture<Void> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(cfg2);

                return null;
            }
        });

        spi2.waitForBlocked();

        doSleep(100);

        IgniteEx g3 = startGrid(3);

        spi2.stopBlock();

        stop.set(true);

        awaitPartitionMapExchange();

        run.get();
        fut.get();
    }

    @Override protected long getTestTimeout() {
        return super.getTestTimeout() * 100000;
    }

    @Override protected long getPartitionMapExchangeTimeout() {
        return super.getPartitionMapExchangeTimeout() * 100000;
    }
}
