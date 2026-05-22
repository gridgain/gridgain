/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client.thin;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.datastreamer.ClientDataStreamer;
import org.apache.ignite.client.datastreamer.DataStreamerClientOptions;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.stream.StreamReceiver;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests for {@link ClientDataStreamer} over TCP thin client protocol.
 */
public abstract class AbstractDataStreamerClientTest extends AbstractThinClientTest {
    /** Number of server nodes. */
    private static final int GRID_CNT = 3;

    /** Cache name used by most tests. */
    private static final String CACHE_NAME = "streamerCache";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        grid(0).cache(CACHE_NAME).removeAll();
    }

    // ==================== Basic functionality ====================

    /**
     * Tests basic streaming with default options.
     */
    @Test
    public void testBasicStreaming() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCache<Integer, String> cache = client.getOrCreateCache(CACHE_NAME);

            try (ClientDataStreamer<Integer, String> streamer = client.dataStreamer(CACHE_NAME)) {
                assertEquals(CACHE_NAME, streamer.cacheName());

                streamer.add(1, "1");
                streamer.add(2, "2");
            }

            assertEquals("1", cache.get(1));
            assertEquals("2", cache.get(2));
        }
    }

    /**
     * Tests that {@link ClientDataStreamer#options()} reflects the provided configuration.
     */
    @Test
    public void testOptionsHaveCorrectDefaults() throws Exception {
        try (IgniteClient client = startClient(0)) {
            try (ClientDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME)) {
                DataStreamerClientOptions<Integer, Integer> opts = streamer.options();

                assertEquals(DataStreamerClientOptions.DFLT_PER_NODE_BUFFER_SIZE, opts.getPerNodeBufferSize());
                assertEquals(DataStreamerClientOptions.DFLT_PER_NODE_PARALLEL_OPERATIONS, opts.getPerNodeParallelOperations());
                assertEquals(Runtime.getRuntime().availableProcessors() * 4, opts.getPerNodeParallelOperations());
                assertEquals(0L, opts.getAutoFlushInterval());
                assertNull(opts.getReceiver());
                assertFalse(opts.isAllowOverwrite());
                assertFalse(opts.isSkipStore());
                assertFalse(opts.isReceiverKeepBinary());
            }
        }
    }

    /**
     * Tests combined add and remove operations with {@code allowOverwrite=true}.
     */
    @Test
    public void testAddRemoveOverwrite() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache(CACHE_NAME);

            for (int i = 1; i <= 10; i++)
                cache.put(i, i + 1);

            DataStreamerClientOptions<Integer, Integer> opts = new DataStreamerClientOptions<Integer, Integer>()
                .setAllowOverwrite(true);

            try (ClientDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME, opts)) {
                streamer.add(1, 11);
                streamer.add(20, 20);

                for (int key : new int[]{2, 4, 6, 7, 8, 9})
                    streamer.remove(key);

                streamer.add(10, null); // null value = remove
            }

            assertEquals(11, (int)cache.get(1));
            assertEquals(20, (int)cache.get(20));
            assertEquals(4, cache.size());
            assertTrue(cache.containsKey(1));
            assertTrue(cache.containsKey(3));
            assertTrue(cache.containsKey(5));
            assertTrue(cache.containsKey(20));
        }
    }

    /**
     * Tests that the buffer triggers an automatic flush when it reaches {@code perNodeBufferSize}.
     *
     * <p>With partition awareness enabled, each entry may be routed to a different per-partition batch.
     * Using {@code perNodeBufferSize=1} guarantees an immediate flush for every entry regardless of routing.</p>
     */
    @Test
    public void testAutoFlushOnFullBufferSmall() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache(CACHE_NAME);

            DataStreamerClientOptions<Integer, Integer> opts = new DataStreamerClientOptions<Integer, Integer>()
                .setPerNodeBufferSize(1);

            try (ClientDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME, opts)) {
                streamer.add(1, 1);
                assertTrue("Buffer flush timed out for key 1", waitForCondition(() -> cache.containsKey(1), 5_000));

                streamer.add(2, 2);
                assertTrue("Buffer flush timed out for key 2", waitForCondition(() -> cache.containsKey(2), 5_000));

                streamer.add(3, 3);
                assertTrue("Buffer flush timed out for key 3", waitForCondition(() -> cache.containsKey(3), 5_000));
            }
        }
    }

    /**
     * Tests that the buffer triggers an automatic flush when it reaches {@code perNodeBufferSize}.
     */
    @Test
    public void testAutoFlushOnFullBufferBig() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache(CACHE_NAME);

            DataStreamerClientOptions<Integer, Integer> opts = new DataStreamerClientOptions<Integer, Integer>()
                    .setPerNodeBufferSize(3);

            // Ensure all keys go to the same partition and thus the same batch.
            List<Integer> keys = partitionKeys(grid(0).getOrCreateCache(CACHE_NAME), 1, 3, 3);

            try (ClientDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME, opts)) {
                streamer.add(keys.get(0), 1);
                assertEquals(0, cache.size());

                streamer.add(keys.get(1), 2);
                assertEquals(0, cache.size());

                streamer.add(keys.get(2), 3);
                assertTrue("Buffer flush timed out", waitForCondition(() -> cache.size() == 3, 5_000));
            }
        }
    }

    /**
     * Tests explicit flush: data is visible after each {@link ClientDataStreamer#flush()} call.
     */
    @Test
    public void testManualFlush() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache(CACHE_NAME);

            try (ClientDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME)) {
                streamer.add(1, 1);
                streamer.add(2, 2);

                streamer.flush();

                assertEquals(2, cache.size());
                assertEquals(1, (int)cache.get(1));
                assertEquals(2, (int)cache.get(2));

                streamer.add(3, 3);
                streamer.flush();

                assertEquals(3, cache.size());
                assertEquals(3, (int)cache.get(3));
            }
        }
    }

    // ==================== Close / cancel ====================

    /**
     * Tests that closing a streamer with no data added has no side effects.
     */
    @Test
    public void testCloseWithNoDataAdded() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache(CACHE_NAME);

            try (ClientDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME)) {
                // No-op.
            }

            assertEquals(0, cache.size());
        }
    }

    /**
     * Tests that {@code close(cancel=false)} flushes buffered data before closing.
     */
    @Test
    public void testCloseFlushesData() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache(CACHE_NAME);

            ClientDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME);

            streamer.add(1, 1);
            streamer.add(2, 2);
            streamer.close(false);

            assertTrue(streamer.isClosed());
            assertEquals(2, cache.size());
            assertEquals(1, (int)cache.get(1));
            assertEquals(2, (int)cache.get(2));
        }
    }

    /**
     * Tests that {@code close(cancel=true)} discards buffered data.
     */
    @Test
    public void testCloseCancelDiscardsBufferedData() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache(CACHE_NAME);

            try (ClientDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME)) {
                streamer.add(1, 1);
                streamer.add(2, 2);
                streamer.close(true);
            }

            assertEquals(0, cache.size());
        }
    }

    /**
     * Tests that multiple close calls are allowed and idempotent.
     */
    @Test
    public void testMultipleCloseCallsAreAllowed() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache(CACHE_NAME);

            ClientDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME);

            streamer.add(1, 2);
            streamer.close(false);

            streamer.close(true);
            streamer.close(false);
            streamer.closeAsync(true).get();
            streamer.closeAsync(false).get();

            assertEquals(2, (int)cache.get(1));
        }
    }

    // ==================== Error handling ====================

    /**
     * Tests that {@link ClientDataStreamer#remove} throws when {@code allowOverwrite=false}.
     */
    @Test
    public void testRemoveNoAllowOverwriteThrows() throws Exception {
        try (IgniteClient client = startClient(0)) {
            try (ClientDataStreamer<Integer, String> streamer = client.dataStreamer(CACHE_NAME)) {
                GridTestUtils.assertThrowsAnyCause(
                    log,
                    () -> { streamer.remove(1); return null; },
                    ClientException.class,
                    "DataStreamer can't remove data when AllowOverwrite is false."
                );
            }
        }
    }

    /**
     * Tests that add and remove throw after the streamer is closed.
     */
    @Test
    public void testOperationsThrowWhenStreamerIsClosed() throws Exception {
        try (IgniteClient client = startClient(0)) {
            DataStreamerClientOptions<Integer, Integer> opts = new DataStreamerClientOptions<Integer, Integer>()
                .setAllowOverwrite(true);

            ClientDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME, opts);
            streamer.close(true);

            assertTrue(streamer.isClosed());

            GridTestUtils.assertThrowsAnyCause(log, () -> { streamer.add(1, 1); return null; },
                ClientException.class, "Data streamer is stopped.");

            GridTestUtils.assertThrowsAnyCause(log, () -> { streamer.remove(1); return null; },
                ClientException.class, "Data streamer is stopped.");
        }
    }

    /**
     * Tests that flush throws a meaningful exception when the cache does not exist.
     */
    @Test
    public void testFlushThrowsWhenCacheDoesNotExist() throws Exception {
        try (IgniteClient client = startClient(0)) {
            try (ClientDataStreamer<Integer, Integer> streamer = client.dataStreamer("no-such-cache")) {
                streamer.add(1, 1);

                GridTestUtils.assertThrowsAnyCause(
                    log,
                    () -> { streamer.flush(); return null; },
                    ClientException.class,
                    "Cache does not exist"
                );
            }
        }
    }

    // ==================== Performance / concurrency ====================

    /**
     * Tests streaming a large list of entries to verify multi-batch correctness.
     */
    @Test
    public void testStreamLargeList() throws Exception {
        final int count = 50_000;

        try (IgniteClient client = startClient(0)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache(CACHE_NAME);

            try (ClientDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME)) {
                for (int k = 0; k < count; k++)
                    streamer.add(k, -k);
            }

            assertEquals(count, cache.size());
            assertEquals(-2, (int)cache.get(2));
            assertEquals(-200, (int)cache.get(200));
        }
    }

    /**
     * Tests concurrent streaming from multiple threads.
     */
    @Test
    public void testStreamMultithreaded() throws Exception {
        final int count = 100_000;

        try (IgniteClient client = startClient(0)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache(CACHE_NAME);

            AtomicInteger id = new AtomicInteger();

            try (ClientDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME)) {
                GridTestUtils.runMultiThreaded(() -> {
                    while (true) {
                        int key = id.incrementAndGet();

                        if (key > count)
                            break;

                        streamer.add(key, key + 2);
                    }
                }, 8, "streamer-thread");
            }

            assertEquals(count, cache.size());
            assertEquals(4, (int)cache.get(2));
            assertEquals(22, (int)cache.get(20));
        }
    }

    /**
     * Tests that data is flushed periodically when {@code autoFlushInterval} is set.
     */
    @Test
    public void testAutoFlushInterval() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache(CACHE_NAME);

            DataStreamerClientOptions<Integer, Integer> opts = new DataStreamerClientOptions<Integer, Integer>()
                .setAutoFlushInterval(100);

            try (ClientDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME, opts)) {
                streamer.add(1, 1);
                assertTrue("Auto-flush timed out for key 1", waitForCondition(() -> cache.containsKey(1), 5_000));

                streamer.add(2, 2);
                assertTrue("Auto-flush timed out for key 2", waitForCondition(() -> cache.containsKey(2), 5_000));
            }
        }
    }

    // ==================== Stream receiver ====================

    /**
     * Tests streaming with a custom {@link StreamReceiver} that modifies each entry.
     */
    @Test
    public void testStreamReceiver() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache(CACHE_NAME);

            DataStreamerClientOptions<Integer, Integer> opts = new DataStreamerClientOptions<Integer, Integer>()
                .setReceiver(new StreamReceiverAddOne());

            try (ClientDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME, opts)) {
                streamer.add(1, 1);
            }

            assertEquals(2, (int)cache.get(1));
        }
    }

    // ==================== Store interaction ====================

    /**
     * Tests that {@code skipStore=true} prevents write-through to the underlying cache store.
     */
    @Test
    public void testSkipStoreDoesNotInvokeCacheStore() throws Exception {
        String storeCacheName = "streamerCacheWithStore";

        CountingCacheStore.writeCount = new AtomicInteger();

        grid(0).getOrCreateCache(new CacheConfiguration<Integer, Integer>(storeCacheName)
            .setCacheStoreFactory(FactoryBuilder.factoryOf(CountingCacheStore.class))
            .setWriteThrough(true));

        try (IgniteClient client = startClient(0)) {
            DataStreamerClientOptions<Integer, Integer> opts = new DataStreamerClientOptions<Integer, Integer>()
                .setSkipStore(true)
                .setAllowOverwrite(true);

            try (ClientDataStreamer<Integer, Integer> streamer = client.dataStreamer(storeCacheName, opts)) {
                for (int i = 1; i <= 300; i++)
                    streamer.add(i, -i);
            }

            assertEquals(300, grid(0).cache(storeCacheName).size());
            assertEquals(0, CountingCacheStore.writeCount.get());
        }

        grid(0).destroyCache(storeCacheName);
    }

    // ==================== Inner classes ====================

    /** Stream receiver that increments each entry value by 1. */
    private static class StreamReceiverAddOne implements StreamReceiver<Integer, Integer> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void receive(IgniteCache<Integer, Integer> cache,
            Collection<Map.Entry<Integer, Integer>> entries) {
            for (Map.Entry<Integer, Integer> e : entries)
                cache.put(e.getKey(), e.getValue() + 1);
        }
    }

    /** Cache store that counts write invocations; uses shared static state for cross-instance visibility. */
    public static class CountingCacheStore extends CacheStoreAdapter<Integer, Integer> {
        /** */
        public static volatile AtomicInteger writeCount = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public Integer load(Integer key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) {
            writeCount.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {}
    }
}
