package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import javax.cache.Cache;
import javax.cache.expiry.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ScanQueryConcurrentUpdatesAbstractTest extends GridCommonAbstractTest {
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(4);
    }

    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    protected abstract IgniteCache<Integer, Integer> createCache(String cacheName, CacheMode cacheMode,
        Duration expiration);

    protected abstract void updateCache(IgniteCache<Integer, Integer> cache, int recordsNum);

    protected abstract void destroyCache(IgniteCache<Integer, Integer> cache);

    private void testStableDataset(IgniteCache<Integer, Integer> cache, int recordsNum) {
        int iterations = 1000;

        AtomicBoolean finished = new AtomicBoolean();

        try {
            updateCache(cache, recordsNum);
            GridTestUtils.runAsync(() -> {
                while (!finished.get())
                    updateCache(cache, recordsNum);
            });

            for (int i = 0; i < iterations; i++) {
                List<Cache.Entry<Integer, Integer>> res = cache.query(new ScanQuery<Integer, Integer>()).getAll();

                assertEquals("Unexpected query result size.", recordsNum, res.size());

                for (Cache.Entry<Integer, Integer> e : res)
                    assertEquals(e.getKey(), e.getValue());
            }
        }
        finally {
            finished.set(true);
            destroyCache(cache);
        }
    }

    private void testExpiringDataset(IgniteCache<Integer, Integer> cache) {
        int iterations = 100;
        int recordsNum = 100;

        try {
            for (int i = 0; i < iterations; i++) {
                updateCache(cache, recordsNum);
                List<Cache.Entry<Integer, Integer>> res = cache.query(new ScanQuery<Integer, Integer>()).getAll();

                assertTrue("Query result set is too big: " + res.size(), res.size() <= recordsNum);

                for (Cache.Entry<Integer, Integer> e : res)
                    assertEquals(e.getKey(), e.getValue());
            }
        }
        finally {
            destroyCache(cache);
        }
    }

    @Test
    public void testReplicatedOneRecordLongExpiry() {
        testStableDataset(createCache("replicated_long_expiry",
            CacheMode.REPLICATED, Duration.ONE_HOUR), 1);
    }

    @Test
    public void testReplicatedManyRecordsLongExpiry() {
        testStableDataset(createCache("replicated_long_expiry",
            CacheMode.REPLICATED, Duration.ONE_HOUR), 1000);
    }

    @Test
    public void testReplicatedOneRecordNoExpiry() {
        testStableDataset(createCache("replicated_no_expiry",
            CacheMode.REPLICATED, null), 1);
    }

    @Test
    public void testReplicatedManyRecordsNoExpiry() {
        testStableDataset(createCache("replicated_no_expiry",
            CacheMode.REPLICATED, null), 1000);
    }

    @Test
    public void testPartitionedOneRecordLongExpiry() {
        testStableDataset(createCache("partitioned_long_expiry",
            CacheMode.PARTITIONED, Duration.ONE_HOUR), 1);
    }

    @Test
    public void testPartitionedManyRecordsLongExpiry() {
        testStableDataset(createCache("partitioned_long_expiry",
            CacheMode.PARTITIONED, Duration.ONE_HOUR), 1000);
    }

    @Test
    public void testPartitionedOneRecordNoExpiry() {
        testStableDataset(createCache("partitioned_no_expiry",
            CacheMode.PARTITIONED, null), 1);
    }

    @Test
    public void testPartitionedManyRecordsNoExpiry() {
        testStableDataset(createCache("partitioned_no_expiry",
            CacheMode.PARTITIONED, null), 1000);
    }

    @Test
    public void testPartitionedShortExpiry() {
        testExpiringDataset(createCache("partitioned_short_expiry",
            CacheMode.PARTITIONED, new Duration(TimeUnit.MILLISECONDS, 1)));
    }

    @Test
    public void testReplicatedShortExpiry() {
        testExpiringDataset(createCache("partitioned_short_expiry",
            CacheMode.REPLICATED, new Duration(TimeUnit.MILLISECONDS, 1)));
    }
}
