/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.processors.cache.GridCacheSharedTtlCleanupManager.DEFAULT_MIN_TOMBSTONE_TTL;
import static org.apache.ignite.internal.processors.cache.GridCacheSharedTtlCleanupManager.DEFAULT_TOMBSTONE_LIMIT;
import static org.apache.ignite.internal.processors.cache.GridCacheSharedTtlCleanupManager.TS_CLEANUP;
import static org.apache.ignite.internal.processors.cache.GridCacheSharedTtlCleanupManager.TS_LIMIT;
import static org.apache.ignite.internal.processors.cache.GridCacheSharedTtlCleanupManager.TS_TTL;

/**
 * Tests tombstone management using distributed properties.
 */
@RunWith(Parameterized.class)
@WithSystemProperty(key = "IGNITE_TTL_EXPIRE_BATCH_SIZE", value = "0") // Disable implicit clearing on cache op.
@WithSystemProperty(key = "CLEANUP_WORKER_SLEEP_INTERVAL", value = "100000000") // Disable background cleanup.
@WithSystemProperty(key = "IGNITE_UNWIND_THROTTLING_TIMEOUT", value = "0") // Disable unwind throttling.
public class TombstonesManagementTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter(value = 0)
    public CacheAtomicityMode atomicityMode;

    /** */
    @Parameterized.Parameter(value = 1)
    public boolean persistence;

    /**
     * @return List of test parameters.
     */
    @Parameterized.Parameters(name = "mode={0} persistence={1}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        params.add(new Object[]{ATOMIC, false});
        params.add(new Object[]{ATOMIC, true});
        params.add(new Object[]{TRANSACTIONAL, false});
        params.add(new Object[]{TRANSACTIONAL, true});

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClusterStateOnStart(ClusterState.INACTIVE);

        // Need at least 2 threads in pool to avoid deadlock on clearing.
        cfg.setRebalanceThreadPoolSize(1);
        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration().
            setWalSegmentSize(1024 * 1024).setMaxWalArchiveSize(4 * 1024 * 1024).setWalSegments(2);
        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(persistence).setMaxSize(200 * 1024 * 1024);
        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setBackups(1).
            setAtomicityMode(atomicityMode).
            setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC).
            setAffinity(new RendezvousAffinityFunction(persistence, 64)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLimit() throws Exception {
        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        int part0 = 0;
        cache.put(part0, 0);
        cache.remove(part0); // Create tombstone with default properties.

        int part1 = 64;
        cache.put(part1, 0);
        cache.remove(part1); // Create tombstone with default properties.

        long expDfltLimit = DEFAULT_TOMBSTONE_LIMIT;

        DistributedChangeableProperty<Serializable> tsLimit0 =
            crd.context().distributedConfiguration().property(TS_LIMIT);
        DistributedChangeableProperty<Serializable> tsLimit1 =
            grid(1).context().distributedConfiguration().property(TS_LIMIT);

        GridCacheContext<Object, Object> ctx0 = crd.cachex(DEFAULT_CACHE_NAME).context();
        GridCacheContext<Object, Object> ctx1 = grid(1).cachex(DEFAULT_CACHE_NAME).context();

        assertEquals(expDfltLimit, ctx0.shared().ttl().tombstonesLimit());
        assertEquals(expDfltLimit, ctx0.shared().ttl().tombstonesLimit());

        validateCache(ctx0.group(), part0, 2, 0);
        validateCache(ctx1.group(), part0, 2, 0);

        ctx0.ttl().expire(1); // Should do nothing, limit is not exceeded.
        ctx1.ttl().expire(1); // Should do nothing, limit is not exceeded.

        validateCache(ctx0.group(), part0, 2, 0);
        validateCache(ctx1.group(), part0, 2, 0);

        tsLimit0.propagate(1L);

        assertEquals(1L, tsLimit0.get());
        assertEquals(1L, tsLimit1.get());

        ctx0.ttl().expire(1); // Should forcefully remove tombstone because limit is expired.
        ctx1.ttl().expire(1); // Should forcefully remove tombstone because limit is expired.

        validateCache(ctx0.group(), part0, 1, 0);
        validateCache(ctx1.group(), part0, 1, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTTL() throws Exception {
        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        int part0 = 0;
        cache.put(part0, 0);
        cache.remove(part0); // Create tombstone with default properties.

        long expDfltTtl = DEFAULT_MIN_TOMBSTONE_TTL;

        DistributedChangeableProperty<Serializable> tsTtl0 =
            crd.context().distributedConfiguration().property(TS_TTL);
        DistributedChangeableProperty<Serializable> tsTtl1 =
            grid(1).context().distributedConfiguration().property(TS_TTL);

        GridCacheContext<Object, Object> ctx0 = crd.cachex(DEFAULT_CACHE_NAME).context();
        GridCacheContext<Object, Object> ctx1 = grid(1).cachex(DEFAULT_CACHE_NAME).context();

        assertEquals(expDfltTtl, ctx0.shared().ttl().tombstoneTTL());
        assertEquals(expDfltTtl, ctx1.shared().ttl().tombstoneTTL());

        validateCache(ctx0.group(), 0, 1, 0);
        validateCache(ctx1.group(), 0, 1, 0);

        ctx0.ttl().expire(1);
        ctx1.ttl().expire(1);

        validateCache(ctx0.group(), 0, 1, 0);
        validateCache(ctx1.group(), 0, 1, 0);

        tsTtl0.propagate(500L);

        assertEquals(500L, tsTtl0.get());
        assertEquals(500L, tsTtl1.get());

        int part1 = 1;
        cache.put(part1, 0);
        cache.remove(part1); // Create tombstone with small TTL.

        doSleep(600);

        ctx0.ttl().expire(2);
        ctx1.ttl().expire(2);

        validateCache(ctx0.group(), part0, 1, 0);
        validateCache(ctx1.group(), part0, 1, 0);

        validateCache(ctx0.group(), part1, 0, 0);
        validateCache(ctx1.group(), part1, 0, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCleanupSuspension() throws Exception {
        IgniteEx crd = startGrids(2);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        DistributedChangeableProperty<Serializable> tsCleanup0 =
            crd.context().distributedConfiguration().property(TS_CLEANUP);
        DistributedChangeableProperty<Serializable> tsCleanup1 =
            grid(1).context().distributedConfiguration().property(TS_CLEANUP);

        GridCacheContext<Object, Object> ctx0 = crd.cachex(DEFAULT_CACHE_NAME).context();
        GridCacheContext<Object, Object> ctx1 = grid(1).cachex(DEFAULT_CACHE_NAME).context();

        assertEquals(false, ctx0.shared().ttl().tombstoneCleanupSuspended());
        assertEquals(false, ctx1.shared().ttl().tombstoneCleanupSuspended());

        DistributedChangeableProperty<Serializable> tsTtl0 =
            crd.context().distributedConfiguration().property(TS_TTL);

        tsTtl0.propagate(500L); // Set small default TTL.

        int part0 = 0;
        cache.put(part0, 0);
        cache.remove(part0); // Create tombstone with small TTL.

        validateCache(ctx0.group(), part0, 1, 0);
        validateCache(ctx1.group(), part0, 1, 0);

        tsCleanup0.propagate(true); // Disable cleanup.

        doSleep(600);

        ctx0.ttl().expire(1); // Should not expire because it's disabled, despite of expired TTL.
        ctx1.ttl().expire(1);

        validateCache(ctx0.group(), part0, 1, 0);
        validateCache(ctx1.group(), part0, 1, 0);

        tsCleanup0.propagate(false);

        ctx0.ttl().expire(1);
        ctx1.ttl().expire(1);

        validateCache(ctx0.group(), part0, 0, 0);
        validateCache(ctx1.group(), part0, 0, 0);
    }

    /**
     * @param grpCtx ctx.
     * @param part Partition.
     * @param expTsCnt Expected timestamp count.
     * @param expDataCnt Expected data count.
     */
    private void validateCache(CacheGroupContext grpCtx, int part, int expTsCnt, int expDataCnt) throws IgniteCheckedException {
        List<CacheDataRow> tsRows = new ArrayList<>();
        grpCtx.offheap().partitionIterator(part, IgniteCacheOffheapManager.TOMBSTONES).forEach(tsRows::add);

        List<CacheDataRow> dataRows = new ArrayList<>();
        grpCtx.offheap().partitionIterator(part, IgniteCacheOffheapManager.DATA).forEach(dataRows::add);

        assertEquals(grpCtx.cacheOrGroupName() + " " + part, expTsCnt, tsRows.size());
        assertEquals(grpCtx.cacheOrGroupName() + " " + part, expDataCnt, dataRows.size());
    }
}
