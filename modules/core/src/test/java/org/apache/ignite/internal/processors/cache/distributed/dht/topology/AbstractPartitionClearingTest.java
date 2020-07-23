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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.resource.DependencyResolver;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class AbstractPartitionClearingTest extends GridCommonAbstractTest {
    /**
     *
     */
    private boolean persistence;

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureDetectionTimeout(1000000L);

        cfg.setConsistentId(igniteInstanceName);

        if (persistence) {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration().setWalSegmentSize(4 * 1024 * 1024);
            dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(persistence);
            cfg.setDataStorageConfiguration(dsCfg);
        }

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
     * @param persistence Persistence.
     * @param backups     Backups.
     * @param mode        Mode: 0 - sync on clearing start, 1 - sync in the middle of clearing
     * @throws Exception If failed.
     */
    protected void testOperationDuringEviction(boolean persistence, int mode, Runnable r) throws Exception {
        this.persistence = persistence;

        AtomicInteger holder = new AtomicInteger();
        CountDownLatch l1 = new CountDownLatch(1);
        CountDownLatch l2 = new CountDownLatch(1);

        IgniteEx g0 = startGrid(0, new DependencyResolver() {
            @Override public <T> T resolve(T instance) {
                if (instance instanceof GridDhtPartitionTopologyImpl) {
                    GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl) instance;

                    top.partitionFactory(new GridDhtPartitionTopologyImpl.PartitionFactory() {
                        @Override public GridDhtLocalPartition create(GridCacheSharedContext ctx, CacheGroupContext grp, int id, boolean recovery) {
                            return new GridDhtLocalPartition(ctx, grp, id, recovery) {
                                private boolean delayed;

                                @Override public IgniteInternalFuture<?> rent() {
                                    if (mode == 0)
                                        sync();

                                    return super.rent();
                                }

                                @Override protected long clearAll(EvictionContext evictionCtx) throws NodeStoppingException {
                                    EvictionContext spied = Mockito.spy(evictionCtx);

                                    Mockito.doAnswer(new Answer() {
                                        @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                                            if (!delayed && mode == 1) {
                                                sync();

                                                delayed = true;
                                            }

                                            return invocation.callRealMethod();
                                        }
                                    }).when(spied).shouldStop();

                                    return super.clearAll(spied);
                                }

                                /** */
                                private void sync() {
                                    if (holder.get() == id) {
                                        l1.countDown();

                                        try {
                                            assertTrue("Failed to wait for lock release", U.await(l2, 30_000, TimeUnit.MILLISECONDS));
                                        } catch (IgniteInterruptedCheckedException e) {
                                            fail(X.getFullStackTrace(e));
                                        }
                                    }
                                }
                            };
                        }
                    });
                }

                return instance;
            }
        });

        startGrid(1);

        if (persistence)
            g0.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange(true, true, null);

        IgniteCache<Object, Object> cache = g0.getOrCreateCache(cacheConfiguration());

        List<Integer> allEvicting = evictingPartitionsAfterJoin(g0, cache, 1024);
        int p0 = allEvicting.get(0);
        holder.set(p0);

        final int cnt = 5_000;

        List<Integer> keys = partitionKeys(g0.cache(DEFAULT_CACHE_NAME), p0, cnt, 0);

        try (IgniteDataStreamer<Object, Object> ds = g0.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (Integer key : keys)
                ds.addData(key, key);
        }

        IgniteEx joining = startGrid(2);

        if (persistence)
            resetBaselineTopology();

        assertTrue(U.await(l1, 30_000, TimeUnit.MILLISECONDS));

        r.run();

        l2.countDown();

        awaitPartitionMapExchange(true, true, null);
        assertPartitionsSame(idleVerify(g0, DEFAULT_CACHE_NAME));
    }

    /**
     *
     */
    protected CacheConfiguration cacheConfiguration() {
        return new CacheConfiguration<>(DEFAULT_CACHE_NAME).
            setCacheMode(CacheMode.PARTITIONED).
            setBackups(1).
            setAffinity(new RendezvousAffinityFunction(false, persistence ? 64 : 1024));
    }
}
