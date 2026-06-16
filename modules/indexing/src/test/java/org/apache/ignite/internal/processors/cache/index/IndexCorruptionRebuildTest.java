/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.localtask.DurableBackgroundTaskState;
import org.apache.ignite.internal.processors.localtask.DurableBackgroundTasksProcessor;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.DurableBackgroundCleanupIndexTreeTaskV2;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.maintenance.MaintenanceRebuildIndexTarget;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.verify.ValidateIndexesClosure;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.INDEX_REBUILD_MNTC_TASK_NAME;
import static org.apache.ignite.internal.processors.query.h2.maintenance.MaintenanceRebuildIndexUtils.parseMaintenanceTaskParameters;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Test for the maintenance task that rebuild a corrupted index.
 */
public class IndexCorruptionRebuildTest extends AbstractIndexCorruptionTest {
    /** */
    private static final String FAIL_IDX_1 = "FAIL_IDX_1";

    /** */
    private static final String OK_IDX = "OK_IDX";

    /** */
    private static final String FAIL_IDX_2 = "FAIL_IDX_2";

    /** */
    private static final String FAIL_IDX_3 = "FAIL_IDX_3";

    /** */
    private static final String CACHE_NAME_1 = "SQL_PUBLIC_TEST1";

    /** */
    private static final String CACHE_NAME_2 = "SQL_PUBLIC_TEST2";

    /** */
    private static final String TABLE_NAME_1 = "test1";

    /** */
    private static final String TABLE_NAME_2 = "test2";

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

    /** */
    @Test
    public void testCorruptedTree() throws Exception {
        IgniteEx srv = startGrid(0);
        IgniteEx normalNode = startGrid(1);

        normalNode.cluster().state(ClusterState.ACTIVE);

        // Create SQL cache
        IgniteCache<Integer, Integer> cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME);

        String qry = "create table %s (col1 varchar primary key, col2 varchar, col3 varchar, col4 varchar) with " +
            "\"BACKUPS=1\"";

        cache.query(new SqlFieldsQuery(String.format(qry, TABLE_NAME_1)));
        cache.query(new SqlFieldsQuery(String.format(qry, TABLE_NAME_2)));

        // Create indexes
        cache.query(new SqlFieldsQuery("create index " + FAIL_IDX_1 + " on test1(col2) INLINE_SIZE 0"));
        cache.query(new SqlFieldsQuery("create index " + OK_IDX + " on test1(col3) INLINE_SIZE 0"));
        cache.query(new SqlFieldsQuery("create index " + FAIL_IDX_2 + " on test1(col4) INLINE_SIZE 0"));
        cache.query(new SqlFieldsQuery("create index " + FAIL_IDX_3 + " on test2(col2) INLINE_SIZE 0"));

        for (int i = 0; i < 100; i++) {
            int counter = i;

            String value = "test" + i;

            String query = "insert into %s(col1, col2, col3, col4) values (?1, ?2, ?3, ?4)";

            Stream.of(TABLE_NAME_1, TABLE_NAME_2)
                .map(tableName ->
                    new SqlFieldsQuery(String.format(query, tableName))
                        .setArgs(String.valueOf(counter), value, value, value)
                ).forEach(cache::query);
        }

        IgniteH2Indexing indexing = (IgniteH2Indexing) srv.context().query().getIndexing();

        corruptIndex(srv, indexing, CACHE_NAME_1, FAIL_IDX_1);
        corruptIndex(srv, indexing, CACHE_NAME_1, FAIL_IDX_2);
        corruptIndex(srv, indexing, CACHE_NAME_2, FAIL_IDX_3);

        MaintenanceRegistry registry = srv.context().maintenanceRegistry();

        // Check that index was indeed corrupted
        checkIndexCorruption(registry, cache, TABLE_NAME_1, asList("col2", "col4"));
        checkIndexCorruption(registry, cache, TABLE_NAME_2, singletonList("col2"));

        MaintenanceTask task = registry.requestedTask(INDEX_REBUILD_MNTC_TASK_NAME);

        List<MaintenanceRebuildIndexTarget> targets = parseMaintenanceTaskParameters(task.parameters());

        // Map cache id -> set of corrupted indexes' names
        Map<Integer, Set<String>> rebuildMap = targets.stream().collect(Collectors.groupingBy(
            MaintenanceRebuildIndexTarget::cacheId,
            Collectors.mapping(MaintenanceRebuildIndexTarget::idxName, toSet())
        ));

        // Check that maintenance task contains all failed indexes
        checkCacheToCorruptedIndexMap(rebuildMap);

        stopGrid(0);

        // This time node should start in maintenance mode and first index should be rebuilt
        srv = startGrid(0);

        assertTrue(srv.context().maintenanceRegistry().isMaintenanceMode());

        Collection<DurableBackgroundTaskState<?>> durableTasks = tasks(srv.context().durableBackgroundTask()).values();

        Map<Integer, Set<String>> indexTasksByCache = durableTasks.stream().collect(Collectors.groupingBy(
            state -> CU.cacheId(((DurableBackgroundCleanupIndexTreeTaskV2) state.task()).cacheName()),
            Collectors.mapping(state -> ((DurableBackgroundCleanupIndexTreeTaskV2) state.task()).idxName(), toSet())
        ));

        checkCacheToCorruptedIndexMap(indexTasksByCache);

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

    /**
     * Checks that caches have certain corrupted indexes.
     *
     * @param cacheToCorruptedIndexMap Corrupted indexes map.
     */
    private void checkCacheToCorruptedIndexMap(Map<Integer, Set<String>> cacheToCorruptedIndexMap) {
        Set<String> idxsForCache1 = cacheToCorruptedIndexMap.get(CU.cacheId(CACHE_NAME_1));
        assertEquals(2, idxsForCache1.size());
        assertTrue(idxsForCache1.containsAll(asList(FAIL_IDX_1, FAIL_IDX_2)));

        Set<String> idxsForCache2 = cacheToCorruptedIndexMap.get(CU.cacheId(CACHE_NAME_2));
        assertEquals(1, idxsForCache2.size());
        assertTrue(idxsForCache2.contains(FAIL_IDX_3));
    }

    /** */
    private static void validateIndexes(IgniteEx node) throws Exception {
        ValidateIndexesClosure clo = new ValidateIndexesClosure(
            () -> false,
            null,
            0,
            0,
            false,
            true
        );

        node.context().resource().injectGeneric(clo);

        VisorValidateIndexesJobResult call = clo.call();

        assertFalse(call.hasIssues());
    }

    /**
     * Checks that index is corrupted.
     *
     * @param registry Maintenance registry.
     * @param cache Cache.
     * @param tableName Name of the table.
     * @param colNames Names of columns with presumably corrupted indexes.
     */
    private void checkIndexCorruption(
        MaintenanceRegistry registry,
        IgniteCache<Integer, Integer> cache,
        String tableName,
        List<String> colNames
    ) {
        List<SqlFieldsQuery> queries = colNames.stream().map(colName ->
            new SqlFieldsQuery(String.format("select * from %s where %s=?1", tableName, colName)).setArgs("test2")
        ).collect(toList());

        queries.forEach(query -> {
            try {
                cache.query(query).getAll();

                fail("Should've failed with CorruptedTreeException");
            }
            catch (CacheException e) {
                MaintenanceTask task = registry.requestedTask(INDEX_REBUILD_MNTC_TASK_NAME);

                assertNotNull(task);
            }
        });
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
         * @return Whether index rebuild happened.
         */
        public boolean didRebuildIndexes() {
            return rebuiltIndexes;
        }
    }

    /**
     * @param proc Durable background task processor.
     * @return Durable tasks.
     */
    private Map<String, DurableBackgroundTaskState<?>> tasks(DurableBackgroundTasksProcessor proc) {
        return getFieldValue(proc, "tasks");
    }
}
