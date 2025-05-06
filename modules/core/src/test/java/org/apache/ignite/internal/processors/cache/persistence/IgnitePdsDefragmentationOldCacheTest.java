/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.maintenance.DefragmentationParameters.toStore;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertNotEquals;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;
import javax.cache.Cache.Entry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Rare case test that emulates defragmentation of old caches that used {@link PageIdAllocator#FLAG_DATA} instead of
 * {@link PageIdAllocator#FLAG_AUX} for partition tree roots.
 */
public class IgnitePdsDefragmentationOldCacheTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override
    protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override
    protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        DataRegionConfiguration drCfg = new DataRegionConfiguration().setPersistenceEnabled(true);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(drCfg);

        return super.getConfiguration(igniteInstanceName)
                .setDataStorageConfiguration(dsCfg);
    }

    /**  */
    @Test
    public void test() throws Exception {
        IgniteEx n = startGrid(0);
        n.cluster().state(ACTIVE);

        createAndPrepareCacheForDefragmentation(n, DEFAULT_CACHE_NAME);

        emulateOldCacheStructureRootPadeIds(n, DEFAULT_CACHE_NAME);

        doDefragmentationForCache(0, DEFAULT_CACHE_NAME);

        n = restartGrid(0);

        assertThat(readAllValuesSorted(getOrCreateCache(n, DEFAULT_CACHE_NAME)), contains(4, 5));
    }

    /** */
    private void emulateOldCacheStructureRootPadeIds(IgniteEx n, String cacheName) throws Exception {
        GridCacheContext<?, ?> grpCtx = n.context().cache().cache(cacheName).context();

        PageMemoryImpl pageMemory = (PageMemoryImpl) grpCtx.dataRegion().pageMemory();
        IgniteCacheDatabaseSharedManager database = grpCtx.shared().database();

        database.checkpointReadLock();

        try {
            long metaPageId = pageMemory.partitionMetaPageId(grpCtx.groupId(), 0);

            updatePage(
                    pageMemory,
                    grpCtx.groupId(),
                    metaPageId,
                    metaPageAddr -> changeFlagForPartitionMetaRootPages(pageMemory, grpCtx.groupId(), metaPageAddr)
            );
        } finally {
            database.checkpointReadUnlock();
        }

        forceCheckpoint();
    }

    /** */
    private static void changeFlagForPartitionMetaRootPages(PageMemoryImpl pageMemory, int grpId, long metaPageAddr) throws Exception {
        PagePartitionMetaIO pagePartitionMetaIO = PagePartitionMetaIO.VERSIONS.latest();

        long pendingTreeRootPageId = pagePartitionMetaIO.getPendingTreeRoot(metaPageAddr);
        long newPendingTreeRootPageId = changePageIdFlagInPage(pageMemory, grpId, pendingTreeRootPageId, FLAG_DATA);
        pagePartitionMetaIO.setPendingTreeRoot(metaPageAddr, newPendingTreeRootPageId);

        long reuseListRootPageId = pagePartitionMetaIO.getReuseListRoot(metaPageAddr);
        long newReuseListRootPageId = changePageIdFlagInPage(pageMemory, grpId, reuseListRootPageId, FLAG_DATA);
        pagePartitionMetaIO.setReuseListRoot(metaPageAddr, newReuseListRootPageId);

        long treeRootPageId = pagePartitionMetaIO.getTreeRoot(metaPageAddr);
        long newTreeRootPageId = changePageIdFlagInPage(pageMemory, grpId, treeRootPageId, FLAG_DATA);
        pagePartitionMetaIO.setTreeRoot(metaPageAddr, newTreeRootPageId);

        long metaStoreReuseListRootPageId = pagePartitionMetaIO.getPartitionMetaStoreReuseListRoot(metaPageAddr);
        long newMetaStoreReuseListRootPageId = changePageIdFlagInPage(pageMemory, grpId, metaStoreReuseListRootPageId, FLAG_DATA);
        pagePartitionMetaIO.setPartitionMetaStoreReuseListRoot(metaPageAddr, newMetaStoreReuseListRootPageId);
    }

    /** */
    private static long changePageIdFlagInPage(PageMemoryImpl pageMemory, int grpId, long pageId, byte newFlag) throws Exception {
        long newPageId = PageIdUtils.pageId(PageIdUtils.partId(pageId), newFlag, PageIdUtils.pageIndex(pageId));

        updatePage(pageMemory, grpId, pageId, pageAddr -> PageIO.setPageId(pageAddr, newPageId));

        return newPageId;
    }

    /** */
    private static void updatePage(PageMemoryImpl pageMemory, int grpId, long pageId, PageAddrConsumer pageUpdater) throws Exception {
        long page = pageMemory.acquirePage(grpId, pageId);

        try {
            long pageAddr = pageMemory.writeLock(grpId, pageId, page);

            assertNotEquals(0L, pageAddr);

            try {
                pageUpdater.apply(pageAddr);
            } finally {
                pageMemory.writeUnlock(grpId, pageId, page, true, true);
            }
        } finally {
            pageMemory.releasePage(grpId, pageId, page);
        }
    }

    /** */
    private IgniteEx restartGrid(int idx) throws Exception {
        stopGrid(idx);

        return startGrid(idx);
    }

    /** */
    private void doDefragmentationForCache(int nodeIdx, String cacheName) throws Exception {
        IgniteEx n = grid(nodeIdx);

        createDefragmentationMaintenanceRecord(n, cacheName);

        n = restartGrid(nodeIdx);

        waitForDefragmentation(n, TimeUnit.SECONDS.toMillis(30));
    }

    /** */
    private static void waitForDefragmentation(IgniteEx n, long timeoutMillis) throws IgniteCheckedException {
        ((GridCacheDatabaseSharedManager)n.context().cache().context().database())
                .defragmentationManager()
                .completionFuture()
                .get(timeoutMillis);
    }

    /** */
    private void createAndPrepareCacheForDefragmentation(IgniteEx n, String cacheName) throws Exception {
        IgniteCache<Integer, Integer> c = getOrCreateCache(n, cacheName);

        fillCache(c, 1, 2, 3, 4, 5);
        removeFromCache(c, 1, 2, 3);

        forceCheckpoint();
    }

    /** */
    private static void createDefragmentationMaintenanceRecord(IgniteEx n, String cacheName) throws IgniteCheckedException {
        MaintenanceRegistry mntcReg = n.context().maintenanceRegistry();

        mntcReg.registerMaintenanceTask(toStore(Arrays.asList(cacheName)));
    }

    /** */
    private static IgniteCache<Integer, Integer> getOrCreateCache(IgniteEx n, String cacheName) {
        return n.getOrCreateCache(
                new CacheConfiguration<Integer, Integer>(cacheName)
                        .setAffinity(new RendezvousAffinityFunction().setPartitions(1))
        );
    }

    /** */
    private static void fillCache(IgniteCache<Integer, Integer> c, int... values) {
        for (int value : values)
            c.put(value, value);
    }

    /** */
    private static void removeFromCache(IgniteCache<Integer, Integer> c, int... keys) {
        for (int key : keys)
            c.remove(key);
    }

    /** */
    private static List<Integer> readAllValuesSorted(IgniteCache<Integer, Integer> cache) {
        return StreamSupport.stream(cache.spliterator(), false)
                .map(Entry::getValue)
                .sorted()
                .collect(toList());
    }

    /** */
    private static PageMemoryImpl pageMemoryImpl(IgniteEx n, String cacheName) {
        DataRegion dataRegion = n.context().cache().cache(cacheName).context().dataRegion();

        return ((PageMemoryImpl)dataRegion.pageMemory());
    }

    /** */
    @FunctionalInterface
    private interface PageAddrConsumer {
        /** */
        void apply(long pageAddr) throws Exception;
    }
}
