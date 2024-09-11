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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Test correct clean up cache configuration data after destroying cache.
 */
public class IgnitePdsDestroyCacheTest extends IgnitePdsDestroyCacheAbstractTest {
    /** */
    @Override public IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        return super.getConfiguration(instanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }
    
    /**
     *  Test destroy non grouped caches.
     *
     *  @throws Exception If failed.
     */
    @Test
    public void testDestroyCaches() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startCachesDynamically(ignite);

        checkDestroyCaches(ignite);
    }

    /**
     *  Test destroy grouped caches.
     *
     *  @throws Exception If failed.
     */
    @Test
    public void testDestroyGroupCaches() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite);

        checkDestroyCaches(ignite);
    }

    /**
     * Test destroy caches abruptly with checkpoints.
     *
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8717")
    @Test
    public void testDestroyCachesAbruptly() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startCachesDynamically(ignite);

        checkDestroyCachesAbruptly(ignite);
    }

    /**
     * Test destroy group caches abruptly with checkpoints.
     *
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8717")
    @Test
    public void testDestroyGroupCachesAbruptly() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite);

        checkDestroyCachesAbruptly(ignite);
    }

    /**
     * Tests if a checkpoint is not blocked forever by concurrent cache destroying (DHT).
     */
    @Test
    public void testDestroyCacheOperationNotBlockingCheckpointTest() throws Exception {
        doTestDestroyCacheOperationNotBlockingCheckpointTest(false);
    }

    /**
     * Tests if a checkpoint is not blocked forever by concurrent cache destroying (local).
     */
    @Test
    public void testDestroyCacheOperationNotBlockingCheckpointTest_LocalCache() throws Exception {
        doTestDestroyCacheOperationNotBlockingCheckpointTest(true);
    }

    /**
     * Tests cache destry with hudge dirty pages.
     */
    @Test
    public void testDestroyCache() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        ignite.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setGroupName("gr1"));
        ignite.getOrCreateCache(new CacheConfiguration<>("cache2").setGroupName("gr1"));

        try (IgniteDataStreamer<Object, Object> streamer2 = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            PageMemoryEx pageMemory = (PageMemoryEx)ignite.cachex(DEFAULT_CACHE_NAME).context().dataRegion().pageMemory();

            long totalPages = pageMemory.totalPages();

            for (int i = 0; i <= totalPages; i++)
                streamer2.addData(i, new byte[pageMemory.pageSize() / 2]);
        }

        ignite.destroyCache(DEFAULT_CACHE_NAME);
    }

    /**
     * Tests partitioned cache destry with hudge dirty pages.
     */
    @Test
    public void testDestroyCacheNotThrowsOOMPartitioned() throws Exception {
        doTestDestroyCacheNotThrowsOOM(false);
    }

    /**
     * Tests local cache destry with hudge dirty pages.
     */
    @Test
    public void testDestroyCacheNotThrowsOOMLocal() throws Exception {
        doTestDestroyCacheNotThrowsOOM(true);
    }

    /** */
    public void doTestDestroyCacheNotThrowsOOM(boolean loc) throws Exception {
        Field batchField = U.findField(IgniteCacheOffheapManagerImpl.class, "BATCH_SIZE");

        int batchSize = batchField.getInt(null);

        int pageSize = 1024;

        int partitions = 32;

        DataStorageConfiguration ds = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(batchSize * pageSize * partitions)
                .setPersistenceEnabled(true))
            .setPageSize(pageSize);

        int payLoadSize = pageSize * 3 / 4;

        IgniteConfiguration cfg = getConfiguration().setDataStorageConfiguration(ds);

        final IgniteEx ignite = startGrid(optimize(cfg));

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite, loc);

        PageMemoryEx pageMemory = (PageMemoryEx) ignite.cachex(cacheName(0)).context().dataRegion().pageMemory();

        IgniteInternalFuture<?> ldrFut = runAsync(() -> {
            IgniteCache<Object, byte[]> c1 = ignite.cache(cacheName(0));

            long totalPages = pageMemory.totalPages();

            for (int i = 0; i <= totalPages; i++)
                c1.put(i, new byte[payLoadSize]);
        });

        CountDownLatch cpStart = new CountDownLatch(1);

        GridCacheDatabaseSharedManager dbMgr = ((GridCacheDatabaseSharedManager)ignite.context()
            .cache().context().database());

        CheckpointListener lsnr = new CheckpointListener() {
            @Override public void onMarkCheckpointBegin(Context ctx) {
                /* No-op. */
            }

            @Override public void onCheckpointBegin(Context ctx) {
                cpStart.countDown();
            }

            @Override public void beforeCheckpointBegin(Context ctx) {
                /* No-op. */
            }
        };

        dbMgr.addCheckpointListener(lsnr);

        ldrFut.get();

        cpStart.await();

        dbMgr.removeCheckpointListener(lsnr);

        IgniteInternalFuture<?> delFut = runAsync(() -> {
            if (loc)
                ignite.cache(cacheName(0)).close();
            else
                ignite.destroyCache(cacheName(0));
        });

        delFut.get(20_000);
    }

    /**
     *
     */
    private void doTestDestroyCacheOperationNotBlockingCheckpointTest(boolean loc) throws Exception {
        final IgniteEx ignite = startGrids(1);

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite, loc);

        loadCaches(ignite, !loc);

        // It's important to clear cache in group having > 1 caches.
        final String cacheName = cacheName(0);
        final CacheGroupContext grp = ignite.cachex(cacheName).context().group();

        final IgniteCacheOffheapManager offheap = grp.offheap();

        IgniteCacheOffheapManager mgr = Mockito.spy(offheap);

        final CountDownLatch checkpointLocked = new CountDownLatch(1);
        final CountDownLatch cpFutCreated = new CountDownLatch(1);
        final CountDownLatch realMtdCalled = new CountDownLatch(1);
        final CountDownLatch checked = new CountDownLatch(1);

        Mockito.doAnswer(invocation -> {
            checkpointLocked.countDown();

            assertTrue(U.await(cpFutCreated, 30, TimeUnit.SECONDS));

            Object ret = invocation.callRealMethod();

            // After calling clearing code cp future must be eventually completed and cp read lock reacquired.
            realMtdCalled.countDown();

            // Wait for checkpoint future while holding lock.
            U.awaitQuiet(checked);

            return ret;
        }).when(mgr).stopCache(Mockito.anyInt(), Mockito.anyBoolean());

        final Field field = U.findField(CacheGroupContext.class, "offheapMgr");
        field.set(grp, mgr);

        final IgniteInternalFuture<Object> fut = runAsync(() -> {
            assertTrue(U.await(checkpointLocked, 30, TimeUnit.SECONDS));

            // Trigger checkpoint while holding checkpoint read lock on cache destroy.
            final IgniteInternalFuture cpFut = ignite.context().cache().context().database().wakeupForCheckpoint("test");

            assertFalse(cpFut.isDone());

            cpFutCreated.countDown();

            assertTrue(U.await(realMtdCalled, 30, TimeUnit.SECONDS));

            try {
                cpFut.get(3_000); // Future must be completed after cache clearing but before releasing checkpoint lock.
            }
            finally {
                checked.countDown();
            }

            return null;
        });

        if (loc)
            ignite.cache(cacheName).close();
        else
            ignite.destroyCache(cacheName);

        fut.get();
    }

    /**
     * Tests correctness of concurrent cache destroy and implicit tx`s.
     */
    @Test
    public void cacheDestroyWithConcImplicitTx() throws Exception {
        final IgniteEx crd = (IgniteEx)startGridsMultiThreaded(3);

        crd.cluster().state(ClusterState.ACTIVE);

        crd.createCache(new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setBackups(1).setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).setGroupName("test"));

        // Cache group with multiple caches are important here, in this case cache removals are not so rapid.
        crd.createCache(new CacheConfiguration(DEFAULT_CACHE_NAME + "_1")
            .setBackups(1).setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).setGroupName("test"));

        Set<Integer> pkeys = new TreeSet<>();
        try (final IgniteDataStreamer<Object, Object> streamer = crd.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 100; i++) {
                streamer.addData(i, i);

                if (crd.affinity(DEFAULT_CACHE_NAME).isPrimary(crd.localNode(), i))
                    pkeys.add(i);
            }
        }

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(crd);

        spi.blockMessages(GridDhtTxPrepareRequest.class, getTestIgniteInstanceName(1));

        List<IgniteFuture<Boolean>> asyncRmFut = new ArrayList<>(100);

        for (Integer pkey : pkeys)
            asyncRmFut.add(crd.cache(DEFAULT_CACHE_NAME).removeAsync(pkey));

        spi.blockMessages(GridDhtPartitionsFullMessage.class, getTestIgniteInstanceName(1));

        IgniteInternalFuture destr = GridTestUtils.runAsync(() -> grid(1).destroyCache(DEFAULT_CACHE_NAME));

        spi.waitForBlocked();

        spi.stopBlock(true, (msg) -> msg.ioMessage().message() instanceof GridDhtPartitionsFullMessage);

        spi.stopBlock();

        destr.get();

        // A little bit untipattern approach here, just because of async remapping, check
        // GridNearOptimisticTxPrepareFutureAdapter.prepareOnTopology.
        // With redefined Failure handler we still need the same approach: wait some time and checks that it not raises.
        assertFalse(GridTestUtils.waitForCondition(() -> G.allGrids().size() < 3, 5_000));

        try {
            asyncRmFut.forEach(f -> f.get(getTestTimeout() / 2));
        }
        catch (CacheException ignore) {
            // No op.
        }
    }

    @Test
    public void cleanupCacheDirectory() throws Exception {
        IgniteEx ignite = startGrids(1);
        ignite.cluster().state(ClusterState.ACTIVE);

        ignite.createCache(new CacheConfiguration(DEFAULT_CACHE_NAME));
        IgniteCache cache = ignite.cache(DEFAULT_CACHE_NAME);
        cache.put(30000, 30000);

        cache.destroy();

        assertFalse(new File(((FilePageStoreManager)ignite.context().cache().context().pageStore()).workDir(), "cache-" + DEFAULT_CACHE_NAME).exists());
    }
}
