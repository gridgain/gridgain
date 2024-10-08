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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntry;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointMarkersStorage;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.ReentrantReadWriteLockWithTracking;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DEFAULT_DISK_PAGE_COMPRESSION;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;
import static org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl.DATAREGION_METRICS_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntryType.END;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForAllFutures;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Test TTL worker with persistence enabled
 */
public class IgnitePdsWithTtlTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME_ATOMIC = "expirable-cache-atomic";

    /** */
    private static final String CACHE_NAME_ATOMIC_NON_PERSISTENT = "expirable-non-persistent-cache-atomic";

    /** */
    private static final String CACHE_NAME_TX = "expirable-cache-tx";

    /** */
    private static final String CACHE_NAME_LOCAL_ATOMIC = "expirable-cache-local-atomic";

    /** */
    private static final String CACHE_NAME_LOCAL_TX = "expirable-cache-local-tx";

    /** */
    private static final String CACHE_NAME_NEAR_ATOMIC = "expirable-cache-near-atomic";

    /** */
    private static final String CACHE_NAME_NEAR_TX = "expirable-cache-near-tx";

    /** */
    private static final String NON_PERSISTENT_DATA_REGION = "non-persistent-region";

    /** */
    public static final int PART_SIZE = 2;

    /** */
    private static final int EXPIRATION_TIMEOUT = 10;

    /** */
    public static final int ENTRIES = 50_000;

    /** */
    public static final int SMALL_ENTRIES = 10;

    /** */
    private static final int WORKLOAD_THREADS_CNT = 16;

    /** Fail. */
    private volatile boolean fail;

    /** Checkpoint frequency. */
    private long checkpointFreq = DataStorageConfiguration.DFLT_CHECKPOINT_FREQ;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EXPIRATION);

        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        //protection if test failed to finish, e.g. by error
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataRegionConfiguration dfltRegion = new DataRegionConfiguration()
            .setMaxSize(2L * 1024 * 1024 * 1024)
            .setPersistenceEnabled(true);

        DataRegionConfiguration nonPersistentRegion = new DataRegionConfiguration()
            .setName(NON_PERSISTENT_DATA_REGION)
            .setMaxSize(2L * 1024 * 1024 * 1024)
            .setPersistenceEnabled(false);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(dfltRegion)
                .setDataRegionConfigurations(nonPersistentRegion)
                .setWalMode(WALMode.LOG_ONLY)
                .setCheckpointFrequency(checkpointFreq));

        cfg.setCacheConfiguration(
            getCacheConfiguration(CACHE_NAME_ATOMIC).setAtomicityMode(ATOMIC),
            getCacheConfiguration(CACHE_NAME_TX).setAtomicityMode(TRANSACTIONAL),
            getCacheConfiguration(CACHE_NAME_LOCAL_ATOMIC).setAtomicityMode(ATOMIC).setCacheMode(CacheMode.LOCAL),
            getCacheConfiguration(CACHE_NAME_LOCAL_TX).setAtomicityMode(TRANSACTIONAL).setCacheMode(CacheMode.LOCAL),
            getCacheConfiguration(CACHE_NAME_NEAR_ATOMIC).setAtomicityMode(ATOMIC)
                .setNearConfiguration(new NearCacheConfiguration<>()),
            getCacheConfiguration(CACHE_NAME_NEAR_TX).setAtomicityMode(TRANSACTIONAL)
                .setNearConfiguration(new NearCacheConfiguration<>())
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new NoOpFailureHandler() {
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                fail = true;

                return super.handle(ignite, failureCtx);
            }
        };
    }

    /**
     * Returns a new cache configuration with the given name and {@code GROUP_NAME} group.
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Object> getCacheConfiguration(String name) {
        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<>();

        ccfg.setName(name);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, PART_SIZE));
        ccfg.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, EXPIRATION_TIMEOUT)));
        ccfg.setEagerTtl(true);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        return ccfg;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testTtlIsApplied() throws Exception {
        loadAndWaitForCleanup(false);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testTtlIsAppliedForMultipleCaches() throws Exception {
        IgniteEx srv = startGrid(0);
        srv.cluster().state(ACTIVE);

        int cacheCnt = 2;

        // Create a new caches in the same group.
        // It is important that initially created cache CACHE_NAME remains empty.
        for (int i = 0; i < cacheCnt; ++i) {
            String cacheName = CACHE_NAME_ATOMIC + "-" + i;

            srv.getOrCreateCache(getCacheConfiguration(cacheName));

            fillCache(srv.cache(cacheName));
        }

        waitAndCheckExpired(srv, srv.cache(CACHE_NAME_ATOMIC + "-" + (cacheCnt - 1)));

        srv.cluster().state(ACTIVE);

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testTtlIsAppliedAfterRestart() throws Exception {
        loadAndWaitForCleanup(true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPutOpsIntoCacheWithExpirationConcurrentlyWithCheckpointCompleteSuccessfully() throws Exception {
        IgniteEx ig0 = startGrid(0);

        ig0.cluster().state(ACTIVE);

        IgniteCache<Object, Object> cache = ig0.getOrCreateCache(CACHE_NAME_ATOMIC);

        AtomicBoolean timeoutReached = new AtomicBoolean(false);

        CheckpointManager checkpointManager = U.field(ig0.context().cache().context().database(), "checkpointManager");

        IgniteInternalFuture<?> ldrFut = runMultiThreadedAsync(() -> {
            while (!timeoutReached.get()) {
                Map<Object, Object> map = new TreeMap<>();

                for (int i = 0; i < ENTRIES; i++)
                    map.put(i, i);

                cache.putAll(map);
            }
        }, WORKLOAD_THREADS_CNT, "loader");

        IgniteInternalFuture<?> updaterFut = runMultiThreadedAsync(() -> {
            while (!timeoutReached.get()) {
                for (int i = 0; i < SMALL_ENTRIES; i++)
                    cache.put(i, i * 10);
            }
        }, WORKLOAD_THREADS_CNT, "updater");

        IgniteInternalFuture<?> cpWriteLockUnlockFut = runAsync(() -> {
            Object checkpointReadWriteLock = U.field(
                checkpointManager.checkpointTimeoutLock(), "checkpointReadWriteLock"
            );

            ReentrantReadWriteLockWithTracking lock = U.field(checkpointReadWriteLock, "checkpointLock");

            while (!timeoutReached.get()) {
                try {
                    lock.writeLock().lockInterruptibly();

                    doSleep(30);
                }
                catch (InterruptedException ignored) {
                    break;
                }
                finally {
                    lock.writeLock().unlock();
                }

                doSleep(30);
            }
        }, "cp-write-lock-holder");

        doSleep(10_000);

        timeoutReached.set(true);

        waitForAllFutures(cpWriteLockUnlockFut, ldrFut, updaterFut);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testConcurrentPutOpsToCacheWithExpirationCompleteSuccesfully() throws Exception {
        final AtomicBoolean end = new AtomicBoolean();

        final IgniteEx srv = startGrids(3);

        srv.cluster().state(ACTIVE);

        // Start high workload.
        IgniteInternalFuture<?> loadFut = runMultiThreadedAsync(() -> {
            List<IgniteCache<Object, Object>> caches = F.asList(
                srv.cache(CACHE_NAME_ATOMIC),
                srv.cache(CACHE_NAME_TX),
                srv.cache(CACHE_NAME_LOCAL_ATOMIC),
                srv.cache(CACHE_NAME_LOCAL_TX),
                srv.cache(CACHE_NAME_NEAR_ATOMIC),
                srv.cache(CACHE_NAME_NEAR_TX)
            );

            while (!end.get() && !fail) {
                for (IgniteCache<Object, Object> cache : caches) {
                    for (int i = 0; i < SMALL_ENTRIES; i++)
                        cache.put(i, new byte[1024]);

                    cache.putAll(new TreeMap<>(F.asMap(0, new byte[1024], 1, new byte[1024])));

                    for (int i = 0; i < SMALL_ENTRIES; i++)
                        cache.get(i);

                    cache.getAll(new TreeSet<>(F.asList(0, 1)));
                }
            }
        }, WORKLOAD_THREADS_CNT, "high-workload");

        try {
            // Let's wait some time.
            loadFut.get(10, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            assertFalse("Failure handler was called. See log above.", fail);

            assertTrue(X.hasCause(e, IgniteFutureTimeoutCheckedException.class));
        }
        finally {
            end.set(true);
        }

        assertFalse("Failure handler was called. See log above.", fail);
    }

    /**
     * @throws Exception if failed.
     */
    private void loadAndWaitForCleanup(boolean restartGrid) throws Exception {
        IgniteEx srv = startGrid(0);

        srv.cluster().state(ACTIVE);

        fillCache(srv.cache(CACHE_NAME_ATOMIC));

        if (restartGrid) {
            srv.context().cache().context().database().waitForCheckpoint("test-checkpoint");

            stopGrid(0);
            srv = startGrid(0);
            srv.cluster().state(ACTIVE);
        }

        final IgniteCache<Integer, byte[]> cache = srv.cache(CACHE_NAME_ATOMIC);

        printStatistics((IgniteCacheProxy)cache, "After restart from LFS");

        waitAndCheckExpired(srv, cache);

        srv.cluster().state(ACTIVE);

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRebalancingWithTtlExpirable() throws Exception {
        IgniteEx srv = startGrid(0);

        srv.cluster().baselineAutoAdjustEnabled(false);
        srv.cluster().state(ACTIVE);

        fillCache(srv.cache(CACHE_NAME_ATOMIC));

        srv = startGrid(1);

        //causes rebalancing start
        srv.cluster().setBaselineTopology(srv.cluster().topologyVersion());

        final IgniteCache<Integer, byte[]> cache = srv.cache(CACHE_NAME_ATOMIC);

        printStatistics((IgniteCacheProxy)cache, "After rebalancing start");

        waitAndCheckExpired(srv, cache);

        srv.cluster().state(INACTIVE);

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testStartStopAfterRebalanceWithTtlExpirable() throws Exception {
        try {
            IgniteEx srv = startGrid(0);

            srv.cluster().baselineAutoAdjustEnabled(false);

            startGrid(1);
            srv.cluster().state(ACTIVE);

            ExpiryPolicy plc = CreatedExpiryPolicy.factoryOf(Duration.ONE_DAY).create();

            IgniteCache<Integer, byte[]> cache0 = srv.cache(CACHE_NAME_ATOMIC);

            fillCache(cache0.withExpiryPolicy(plc));

            srv = startGrid(2);

            IgniteCache<Integer, byte[]> cache = srv.cache(CACHE_NAME_ATOMIC);

            //causes rebalancing start
            srv.cluster().setBaselineTopology(srv.cluster().topologyVersion());

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return Boolean.TRUE.equals(cache.rebalance().get()) && cache.localSizeLong(CachePeekMode.ALL) > 0;
                }
            }, 20_000);

            //check if pds is consistent
            stopGrid(0);
            startGrid(0);

            stopGrid(1);
            startGrid(1);

            srv.cluster().state(INACTIVE);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Tests that expiration of large entries (that require more than one data pages)
     * work correctly after node stopping in the midle of checkpoint.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExpirationLargeEntriesAfterNodeRestart() throws Exception {
        if (System.getProperty(IGNITE_DEFAULT_DISK_PAGE_COMPRESSION) != null) {
            log.info("Test is skipped because of " + IGNITE_DEFAULT_DISK_PAGE_COMPRESSION + " property is set.");

            return;
        }

        IgniteEx srv = startGrid(0);

        srv.cluster().state(ACTIVE);

        String nodeFolderName = srv.context().pdsFolderResolver().resolveFolders().folderName();

        IgniteCache<Integer, byte[]> c = srv.getOrCreateCache(CACHE_NAME_ATOMIC);

        // Insert a new value to the cache.
        // It assumed that the entry will be spit to two pages. Moreover, the time to live value will be split on both pages.
        c.withExpiryPolicy(new TouchedExpiryPolicy(new Duration(MILLISECONDS, EXPIRATION_TIMEOUT * 10)))
            .put(12, new byte[1024 * 4 - 100]);

        // Touch the entry to update the time to live value.
        c.withExpiryPolicy(new TouchedExpiryPolicy(new Duration(SECONDS, EXPIRATION_TIMEOUT))).touch(12);

        // Create a checkpoint and remove checkpoint end marker in order to emulate stopping the node in the middle of checkpoint.
        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager) srv.context().cache().context().database();

        IgniteInternalFuture<?> cpFinishFut = dbMgr.forceCheckpoint("test checkpoint").futureFor(FINISHED);

        cpFinishFut = cpFinishFut.chain(fut -> {
            CheckpointEntry cpEntry = dbMgr.checkpointHistory().lastCheckpoint();

            String cpEndFileName = CheckpointMarkersStorage.checkpointFileName(cpEntry, END);

            GridFutureAdapter<Void> fut0 = new GridFutureAdapter<>();

            try {
                Files.delete(Paths.get(dbMgr.checkpointDirectory().getAbsolutePath(), cpEndFileName));
            }
            catch (IOException e) {
                fut0.onDone(new IgniteException("Failed to remove checkpoint end marker.", e));
            }
            finally {
                fut0.onDone();
            }

            return fut0;
        });

        cpFinishFut.get(10, SECONDS);

        stopGrid(0, true);

        // Check that WAL contains two physical records related to the entry.
        verifyDataPageFragmentUpdateRecords(nodeFolderName, 2);

        // Restart the node.
        srv = startGrid(0);

        c = srv.getOrCreateCache(CACHE_NAME_ATOMIC);

        // Check that the entry was expired.
        waitAndCheckExpired(srv, c);
    }

    /**
     * Tests that cache entries (cache related to non persistent region) correctly expired.
     *
     * @throws Exception
     */
    @Test
    public void testExpirationNonPersistentRegion() throws Exception {
        IgniteEx srv = startGrid(0);

        srv.cluster().baselineAutoAdjustEnabled(false);
        srv.cluster().state(ACTIVE);

        CacheConfiguration<?, ?> cfg =
            getCacheConfiguration(CACHE_NAME_ATOMIC_NON_PERSISTENT)
                .setAtomicityMode(ATOMIC)
                .setDataRegionName(NON_PERSISTENT_DATA_REGION);

        srv.getOrCreateCache(cfg);

        IgniteCache<Integer, byte[]> nonPersistentCache = srv.cache(CACHE_NAME_ATOMIC_NON_PERSISTENT);

        fillCache(nonPersistentCache);

        waitAndCheckExpired(srv, nonPersistentCache);

        stopAllGrids();

        assertFalse("Failure handler should not be triggered.", fail);
    }

    /**
     * Tests that reading a large entry does not cause the whole entry to be rewritten to hte PDS.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReadingLargeEntriesDoNotRewriteEntry() throws Exception {
        // Disable checkpoints on timeout.
        checkpointFreq = 600_000;

        Ignite crd = startGridsMultiThreaded(2);

        crd.cluster().state(ACTIVE);

        String cacheName = CACHE_NAME_ATOMIC + "-dirty-pages-test";
        CacheConfiguration<Integer, Object> ccfg = getCacheConfiguration(cacheName)
            .setAtomicityMode(ATOMIC)
            .setBackups(1)
            .setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, 600_000)));

        IgniteCache<Integer, Object> cache = crd.getOrCreateCache(ccfg);

        awaitPartitionMapExchange();

        Integer pk = 12;

        log.info("Put entry and set expiration time on access [key=" + pk + ']');

        // Put large value (more than 10 pages, for instance).
        cache.put(pk, new byte[12 * DataStorageConfiguration.DFLT_PAGE_SIZE]);

        // Force checkpoint to write all pages to disk and clean dirty flag.
        grid(0).context().cache().context().database().forceCheckpoint("test-checkpoint").futureFor(FINISHED).get();
        grid(1).context().cache().context().database().forceCheckpoint("test-checkpoint").futureFor(FINISHED).get();

        LongMetric dirtyPages0 = grid(0)
            .context()
            .metric()
            .registry(MetricUtils.metricName(DATAREGION_METRICS_PREFIX, "default"))
            .findMetric("DirtyPages");

        LongMetric dirtyPages1 = grid(1)
            .context()
            .metric()
            .registry(MetricUtils.metricName(DATAREGION_METRICS_PREFIX, "default"))
            .findMetric("DirtyPages");

        assertEquals(0, dirtyPages0.value());
        assertEquals(0, dirtyPages1.value());

        // Set expiration time on access.
        cache.get(pk);

        boolean success = waitForCondition(() -> {
            long dirtyPagesVal0 = dirtyPages0.value();
            long dirtyPagesVal1 = dirtyPages1.value();

            // Check that dirty pages must be greater than 0 and
            // less than or equal to 3 (1 page - head, and 2 pages at maximum that hold ttl value).
            boolean res = dirtyPagesVal0 > 0 && dirtyPagesVal0 <= 3
                && dirtyPagesVal1 > 0 && dirtyPagesVal1 <= 3;

            if (!res)
                log.info("[dirtyPages0=" + dirtyPages0.value() + ", dirtyPages1=" + dirtyPages1.value() + ']');

            return res;
        }, 10_000);

        assertTrue("Number of dirty pages should be greater than 0 and less than or equal to 3 [" +
            "dirtyPages0=" + dirtyPages0.value() + ", dirtyPages1=" + dirtyPages1.value() + ']', success);
    }

    /**
     *
     */
    protected void fillCache(IgniteCache<Integer, byte[]> cache) {
        cache.putAll(new TreeMap<Integer, byte[]>() {{
            for (int i = 0; i < ENTRIES; i++)
                put(i, new byte[1024]);
        }});

        //Touch entries.
        for (int i = 0; i < ENTRIES; i++)
            cache.get(i); // touch entries

        printStatistics((IgniteCacheProxy)cache, "After cache puts");
    }

    /**
     *
     */
    protected void waitAndCheckExpired(
        IgniteEx srv,
        final IgniteCache<Integer, byte[]> cache
    ) throws IgniteCheckedException {
        boolean awaited = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return cache.size() == 0;
            }
        }, TimeUnit.SECONDS.toMillis(EXPIRATION_TIMEOUT + EXPIRATION_TIMEOUT / 2));

        assertTrue("Cache is not empty. size=" + cache.size(), awaited);

        printStatistics((IgniteCacheProxy)cache, "After timeout");

        GridCacheSharedContext ctx = srv.context().cache().context();
        GridCacheContext cctx = ctx.cacheContext(CU.cacheId(CACHE_NAME_ATOMIC));

        // Check partitions through internal API.
        for (int partId = 0; partId < PART_SIZE; ++partId) {
            GridDhtLocalPartition locPart = cctx.dht().topology().localPartition(partId);

            if (locPart == null)
                continue;

            IgniteCacheOffheapManager.CacheDataStore dataStore =
                ctx.cache().cacheGroup(CU.cacheId(CACHE_NAME_ATOMIC)).offheap().dataStore(locPart);

            GridCursor cur = dataStore.cursor();

            assertFalse(cur.next());
            assertEquals(0, locPart.fullSize());
        }

        for (int i = 0; i < ENTRIES; i++)
            assertNull(cache.get(i));
    }

    /**
     *
     */
    private void printStatistics(IgniteCacheProxy cache, String msg) {
        System.out.println(msg + " {{");
        cache.context().printMemoryStats();
        System.out.println("}} " + msg);
    }

    /**
     * @param nodeFolderName Folder name
     * @param expRecNum Expected number of DATA_PAGE_FRAGMENTED_UPDATE_RECORDs.
     * @throws IgniteCheckedException If failed.
     */
    private void verifyDataPageFragmentUpdateRecords(String nodeFolderName, int expRecNum) throws IgniteCheckedException {
        String workDir = U.defaultWorkDirectory();
        File dbDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false);

        File walDir = new File(dbDir, "wal");
        File archiveDir = new File(walDir, "archive");

        File nodeWalDir = new File(walDir, nodeFolderName);
        File nodeArchiveDir = new File(archiveDir, nodeFolderName);

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(new NullLogger());

        IgniteWalIteratorFactory.IteratorParametersBuilder params = createIteratorParametersBuilder(workDir, nodeFolderName)
            .filesOrDirs(nodeWalDir, nodeArchiveDir);

        int recNum = 0;
        try (WALIterator iter = factory.iterator(params)) {
            while (iter.hasNextX()) {
                IgniteBiTuple<WALPointer, WALRecord> tup = iter.nextX();

                WALRecord walRecord = tup.get2();
                WALRecord.RecordType type = walRecord.type();

                switch (type) {
                    case DATA_PAGE_FRAGMENTED_UPDATE_RECORD:
                        recNum++;

                        break;

                    default:
                        // No-op.
                }
            }
        }

        assertEquals("Unexpected number of DATA_PAGE_FRAGMENTED_UPDATE_RECORDs", expRecNum, recNum);
    }

    /**
     * @param workDir Work directory.
     * @param nodeFolderName Subfolder name.
     * @return WAL iterator factory.
     * @throws IgniteCheckedException If failed.
     */
    @NotNull
    private IgniteWalIteratorFactory.IteratorParametersBuilder createIteratorParametersBuilder(
        String workDir,
        String nodeFolderName
    ) throws IgniteCheckedException {
        File binaryMeta = U.resolveWorkDirectory(workDir, DataStorageConfiguration.DFLT_BINARY_METADATA_PATH,
            false);
        File binaryMetaWithConsId = new File(binaryMeta, nodeFolderName);
        File marshallerMapping = U.resolveWorkDirectory(workDir, DataStorageConfiguration.DFLT_MARSHALLER_PATH, false);

        return new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .binaryMetadataFileStoreDir(binaryMetaWithConsId)
            .marshallerMappingFileStoreDir(marshallerMapping);
    }
}
