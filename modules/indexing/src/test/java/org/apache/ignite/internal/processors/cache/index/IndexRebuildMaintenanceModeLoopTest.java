/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

import java.util.ArrayList;
import java.util.List;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.INDEX_REBUILD_MNTC_TASK_NAME;

/**
 * Tests the following scenario: when an index is corrupted, the node schedules {@code indexRebuildMaintenanceTask} and
 * halts. On restart in maintenance mode the cache is started in <em>recovery mode</em> BEFORE the rebuild action runs.
 * If recovery-mode startup re-touches the corrupted leaf (e.g. via WAL replay of an un-checkpointed insert),
 * {@link CorruptedTreeException} fires, the node triggers the failure manager and dies.
 */
public class IndexRebuildMaintenanceModeLoopTest extends AbstractIndexCorruptionTest {
    private static final String CACHE_NAME = "SQL_PUBLIC_TEST1";

    private static final String TABLE_NAME = "test1";

    private static final String FAIL_IDX = "FAIL_IDX";

    private final RecordingFailureHandler failureHnd = new RecordingFailureHandler();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        cfg.setCacheConfiguration(new CacheConfiguration<>()
                .setName(DEFAULT_CACHE_NAME)
                .setBackups(0)
        );

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                // Long checkpoint frequency so only our explicit forceCheckpoint() calls run.
                .setCheckpointFrequency(60 * 60 * 1000L)
                .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setPersistenceEnabled(true)
                ));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return failureHnd;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        failureHnd.reset();
    }

    /** */
    @Test
    public void testCorruptionInMaintenanceModeStartupDoesNotEscalateCriticalFailure() throws Exception {
        IgniteEx srv = startGrid(0);

        srv.cluster().state(ClusterState.ACTIVE);

        // Used only as a SQL query handle; the TEST1 table is (varchar, varchar), not Integer/Integer.
        IgniteCache<?, ?> cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.query(new SqlFieldsQuery(
                "create table " + TABLE_NAME + " (col1 varchar primary key, col2 varchar) with \"BACKUPS=0\""));
        cache.query(new SqlFieldsQuery("create index " + FAIL_IDX + " on " + TABLE_NAME + "(col2) INLINE_SIZE 0"));

        // Initial data — enough rows to populate at least one leaf.
        for (int i = 0; i < 100; i++) {
            cache.query(new SqlFieldsQuery(
                    "insert into " + TABLE_NAME + "(col1, col2) values (?1, ?2)")
                    .setArgs(String.valueOf(i), "v" + i));
        }

        forceCheckpoint(srv);

        // Corrupt the secondary index's leaf pages in-memory.
        IgniteH2Indexing indexing = (IgniteH2Indexing) srv.context().query().getIndexing();
        corruptIndex(srv, indexing, CACHE_NAME, FAIL_IDX);

        // Persist the corruption to disk.
        forceCheckpoint(srv);

        MaintenanceRegistry registry = srv.context().maintenanceRegistry();

        // First detection: select that traverses the corrupted leaf. This registers the
        // maintenance task and fires processFailure(CRITICAL_ERROR).
        try {
            cache.query(new SqlFieldsQuery("select * from " + TABLE_NAME + " where col2=?1").setArgs("v50")).getAll();

            fail("Expected CorruptedTreeException from corrupted index");
        } catch (CacheException ignored) {
            // Expected.
        }

        assertNotNull(
                "Maintenance task should be registered after first corruption detection",
                registry.requestedTask(INDEX_REBUILD_MNTC_TASK_NAME)
        );

        // Drive a few more inserts into WAL so recovery-mode startup has work to replay that
        // touches the corrupted index leaves. Inserts may themselves throw — swallow and continue.
        for (int i = 100; i < 110; i++) {
            try {
                cache.query(new SqlFieldsQuery(
                        "insert into " + TABLE_NAME + "(col1, col2) values (?1, ?2)")
                        .setArgs(String.valueOf(i), "v" + i));
            } catch (CacheException ignored) {
                // Expected if the corrupted leaf intercepts the insert.
            }
        }

        // Suppress the final checkpoint on stop so WAL retains the post-checkpoint inserts.
        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager) srv.context().cache().context().database();
        db.getCheckpointer().skipCheckpointOnNodeStop(true);

        stopGrid(0);

        failureHnd.reset();

        srv = startGrid(0);

        assertTrue(
                "Node should have entered maintenance mode on second start",
                srv.context().maintenanceRegistry().isMaintenanceMode()
        );

        List<FailureContext> critical = failureHnd.criticalFailures();

        assertTrue(
                "CRITICAL_ERROR raised during maintenance-mode startup. Recorded: " + critical,
                critical.isEmpty()
        );

        // Third boot: maintenance complete, cluster activates normally; verify the
        // rebuilt index is actually populated by querying via the corrupted column.
        stopGrid(0);

        failureHnd.reset();

        srv = startGrid(0);

        srv.cluster().state(ClusterState.ACTIVE);

        assertFalse(
                "Maintenance mode should be cleared after the rebuild action completed",
                srv.context().maintenanceRegistry().isMaintenanceMode()
        );

        IgniteCache<?, ?> rebuiltCache = srv.getOrCreateCache(DEFAULT_CACHE_NAME);
        List<List<?>> rows = rebuiltCache.query(new SqlFieldsQuery(
                "select col1 from " + TABLE_NAME + " where col2=?1").setArgs("v50")).getAll();

        assertEquals("Rebuilt index must be populated — expected exactly one row matching col2='v50'",
                1, rows.size());

        assertTrue(
                "No critical failures expected during normal activation after rebuild. Recorded: " +
                        failureHnd.criticalFailures(),
                failureHnd.criticalFailures().isEmpty()
        );
    }

    /**
     * Records every failure passed to {@link #onFailure}; never invalidates the node.
     */
    private static class RecordingFailureHandler extends AbstractFailureHandler {
        /** */
        private final List<FailureContext> failures = new ArrayList<>();

        /** {@inheritDoc} */
        @Override protected synchronized boolean handle(Ignite ignite, FailureContext failureCtx) {
            failures.add(failureCtx);

            return false;
        }

        /** */
        synchronized void reset() {
            failures.clear();
        }

        /** */
        synchronized List<FailureContext> criticalFailures() {
            List<FailureContext> res = new ArrayList<>();

            for (FailureContext f : failures) {
                if (f.type() == FailureType.CRITICAL_ERROR)
                    res.add(f);
            }

            return res;
        }
    }
}
