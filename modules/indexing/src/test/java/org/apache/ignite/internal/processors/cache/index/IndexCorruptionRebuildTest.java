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
package org.apache.ignite.internal.processors.cache.index;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.H2TableDescriptor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.verify.ValidateIndexesClosure;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.stream.Collectors.toList;

/**
 * Test for the maintenance task that rebuild a corrupted index.
 */
public class IndexCorruptionRebuildTest extends GridCommonAbstractTest {
    /** */
    private static final String IDX_1_NAME = "FAIL_IDX";

    /** */
    private static final String IDX_2_NAME = "OK_IDX";

    /** */
    private static final String GRP_NAME = "cacheGrp";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        cfg.setCacheConfiguration(new CacheConfiguration<>()
            .setName(DEFAULT_CACHE_NAME)
            .setBackups(1)
        );

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setPersistenceEnabled(true)
        ));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        clearGridToStringClassCache();

        GridQueryProcessor.idxCls = null;

        super.afterTest();
    }

    /** */
    @Test
    public void testCorruptedTree() throws Exception {
        IgniteEx srv = startGrid(0);
        IgniteEx normalNode = startGrid(1);

        normalNode.cluster().state(ClusterState.ACTIVE);

        // Create SQL cache
        IgniteCache<Integer, Integer> cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.query(new SqlFieldsQuery("create table test (col1 varchar primary key, col2 varchar, col3 varchar) with " +
            "\"CACHE_GROUP=" + GRP_NAME + "\", \"BACKUPS=1\""));

        // Create two indexes
        cache.query(new SqlFieldsQuery("create index " + IDX_1_NAME + " on test(col2) INLINE_SIZE 0"));
        cache.query(new SqlFieldsQuery("create index " + IDX_2_NAME + " on test(col3) INLINE_SIZE 0"));

        for (int i = 0; i < 100; i++) {
            String value = "test" + i;

            cache.query(new SqlFieldsQuery("insert into test(col1, col2, col3) values (?1, ?2, ?3)")
                .setArgs(String.valueOf(i), value, value));
        }

        IgniteH2Indexing indexing = (IgniteH2Indexing) srv.context().query().getIndexing();

        String cacheName = "SQL_PUBLIC_TEST";

        PageMemoryEx mem = (PageMemoryEx)srv.context().cache().context().cacheContext(CU.cacheId(cacheName)).dataRegion().pageMemory();

        Collection<H2TableDescriptor> tables = indexing.schemaManager().tablesForCache(cacheName);

        // Corrupt first index
        for (H2TableDescriptor descriptor : tables) {
            H2TreeIndex index = (H2TreeIndex) descriptor.table().getIndex(IDX_1_NAME);
            int segments = index.segmentsCount();

            for (int segment = 0; segment < segments; segment++) {
                H2Tree tree = index.treeForRead(segment);

                GridCacheDatabaseSharedManager manager = dbMgr(srv);
                manager.checkpointReadLock();

                try {
                    corruptTreeRoot(mem, tree.groupId(), tree.getMetaPageId());
                }
                finally {
                    manager.checkpointReadUnlock();
                }
            }
        }

        // Check that index was indeed corrupted
        checkIndexCorruption(srv.context().maintenanceRegistry(), cache);

        stopGrid(0);

        // This time node should start in maintenance mode and first index should be rebuilt
        srv = startGrid(0);

        assertTrue(srv.context().maintenanceRegistry().isMaintenanceMode());

        stopGrid(0);

        GridQueryProcessor.idxCls = CaptureRebuildGridQueryIndexing.class;

        srv = startGrid(0);

        normalNode.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        CaptureRebuildGridQueryIndexing capturingIndex = (CaptureRebuildGridQueryIndexing) srv.context().query().getIndexing();

        // Check that index was not rebuild during the restart
        assertFalse(capturingIndex.didRebuildIndexes());

        // Validate indexes integrity
        validateIndexes(srv);
    }

    private static void validateIndexes(IgniteEx node) throws Exception {
        ValidateIndexesClosure clo = new ValidateIndexesClosure(
            () -> false,
            Collections.singleton(DEFAULT_CACHE_NAME),
            0,
            0,
            false,
            true
        );

        node.context().resource().injectGeneric(clo);

        VisorValidateIndexesJobResult call = clo.call();

        assertFalse(call.hasIssues());
    }

    private void checkIndexCorruption(MaintenanceRegistry registry, IgniteCache<Integer, Integer> cache) {
        List<SqlFieldsQuery> queries = Stream.of(
            new SqlFieldsQuery("select * from test where col2=?1").setArgs("test2"),
            new SqlFieldsQuery("insert into test(col1, col2, col3) values (?1, ?2, ?3)").setArgs("9998", "1", "1"),
            new SqlFieldsQuery("update test set col2=?2 where col1=?1").setArgs("3", "test3")
        ).collect(toList());

        assertFalse(registry.unregisterMaintenanceTask(IgniteH2Indexing.INDEX_REBUILD_MNTC_TASK_NAME));

        queries.forEach(query -> {
            try {
                cache.query(query).getAll();

                fail("Should've failed with CorruptedTreeException");
            }
            catch (CacheException e) {
                assertTrue(registry.unregisterMaintenanceTask(IgniteH2Indexing.INDEX_REBUILD_MNTC_TASK_NAME));
            }
        });

        // Execute query once again to create maintenance record
        try {
            cache.query(queries.get(0)).getAll();

            fail("Should've failed with CorruptedTreeException");
        }
        catch (CacheException e) {
            assertTrue(e.getMessage().contains(CorruptedTreeException.class.getName()));
        }
    }

    /** */
    private void corruptTreeRoot(PageMemoryEx pageMemory, int grpId, long metaPageId)
        throws IgniteCheckedException {
        long leafId = findFirstLeafId(grpId, metaPageId, pageMemory);

        if (leafId != 0L) {
            long leafPage = pageMemory.acquirePage(grpId, leafId);

            try {
                long leafPageAddr = pageMemory.writeLock(grpId, leafId, leafPage);

                try {
                    H2LeafIO io = PageIO.getBPlusIO(leafPageAddr);

                    for (int idx = 0; idx < io.getCount(leafPageAddr); idx++) {
                        PageUtils.putLong(leafPageAddr, io.offset(idx) + io.getPayloadSize(), 0);
                    }
                }
                finally {
                    pageMemory.writeUnlock(grpId, leafId, leafPage, true, true);
                }
            }
            finally {
                pageMemory.releasePage(grpId, leafId, leafPage);
            }
        }
    }

    /** */
    private long findFirstLeafId(int grpId, long metaPageId, PageMemoryEx partPageMemory) throws IgniteCheckedException {
        long metaPage = partPageMemory.acquirePage(grpId, metaPageId);

        try {
            long metaPageAddr = partPageMemory.readLock(grpId, metaPageId, metaPage);

            try {
                BPlusMetaIO metaIO = PageIO.getPageIO(metaPageAddr);

                return metaIO.getFirstPageId(metaPageAddr, 0);
            }
            finally {
                partPageMemory.readUnlock(grpId, metaPageId, metaPage);
            }
        }
        finally {
            partPageMemory.releasePage(grpId, metaPageId, metaPage);
        }
    }

    /**
     * IgniteH2Indexing that captures index rebuild operations.
     */
    public static class CaptureRebuildGridQueryIndexing extends IgniteH2Indexing {
        /**
         * Whether index rebuild happened.
         */
        private volatile boolean rebuiltIndexes;

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> rebuildIndexesFromHash(GridCacheContext cctx, boolean force) {
            IgniteInternalFuture<?> future = super.rebuildIndexesFromHash(cctx, force);
            rebuiltIndexes = future != null;
            return future;
        }

        /**
         * Get index rebuild flag.
         *
         * @return Whether index rebuild happened.
         */
        public boolean didRebuildIndexes() {
            return rebuiltIndexes;
        }
    }
}
