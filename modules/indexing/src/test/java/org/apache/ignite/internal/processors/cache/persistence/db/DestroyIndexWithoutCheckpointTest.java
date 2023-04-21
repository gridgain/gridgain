/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.localtask.DurableBackgroundTaskState;
import org.apache.ignite.internal.processors.localtask.DurableBackgroundTasksProcessor;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Tests destruction of the index without checkpoint afterwards.
 * In this scenario, index destruction will start again on restart (after PME). The index root page will be found
 * because we search for indexes after binary restore and index rename is a logical record.
 * Previous destruction will also be started after logical recovery (because durable task is read from the metastorage).
 * This way we will have two tasks destroying the same index, which leads to assertion error or SIGSEGV (if assertions
 * are disabled).
 */
@WithSystemProperty(key = IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP, value = "true")
public class DestroyIndexWithoutCheckpointTest extends GridCommonAbstractTest {
    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(gridName);

        DataStorageConfiguration cfg = new DataStorageConfiguration();

        cfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setPersistenceEnabled(true));

        configuration.setDataStorageConfiguration(cfg);

        configuration.setGridLogger(listeningLog);

        return configuration;
    }

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

    static String tableName(String cacheName) {
        return cacheName + "_table";
    }

    static String createTable(String cacheName, boolean multipleSegments) {
        return "CREATE TABLE IF NOT EXISTS " + tableName(cacheName) + " (\n" +
            "    ID VARCHAR NOT NULL,\n" +
            "    NAME VARCHAR NOT NULL,\n" +
            "    PRIMARY KEY (ID)\n" +
            ")\n" +
            "WITH \"\n" +
            "    CACHE_NAME=" + cacheName + "\n" +
            (multipleSegments ? ",PARALLELISM=2" : "") +
            "\";";
    }

    static String insertQuery(String cacheName) {
        return "INSERT INTO " + tableName(cacheName) + "(ID, NAME) VALUES(?, ?)";
    }

    static SqlFieldsQuery insertQuery(String cacheName, int id) {
        return new SqlFieldsQuery(insertQuery(cacheName)).setArgs(id, "name-" + id);
    }

    @Test
    public void testMultipleSegments() throws Exception {
        test(true);
    }

    @Test
    public void test() throws Exception {
        test(false);
    }

    private void test(boolean multipleSegments) throws Exception {
        // Listen for failure of the drop index tasks.
        LogListener lsnrBackgroundFailure = LogListener.matches(
            "Could not execute durable background task: drop-sql-index-test-"
        ).times(0).build();

        // Listen for failure of the drop index tasks.
        LogListener lsnrConcurrentDestruction = LogListener.matches(
            "Tree is being concurrently destroyed"
        ).times(0).build();

        listeningLog.registerListener(lsnrBackgroundFailure);
        listeningLog.registerListener(lsnrConcurrentDestruction);

        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        // Cache for SQL queries.
        IgniteCache<Object, Object> queryCache = ignite.getOrCreateCache("queryCache");

        String cacheName = "test";

        // Create "test" cache.
        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(cacheName);

        // Put data.
        cache.put(1, 1);

        // Create table over "test" cache. This triggers index rebuild and subsequently drop of
        // older indexes.
        queryCache.query(new SqlFieldsQuery(createTable(cacheName, multipleSegments))).getAll();

        // Add a record so that on logical restore index rename would be first and then this record.
        // This way we try to trigger a write to an index that is already renamed.
        queryCache.query(insertQuery("test", 100)).getAll();

        // Restart grid.
        stopGrid(0, true);

        ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteEx finalIgnite = ignite;

        // Wait until completion of all the background durable tasks.
        assertTrue(GridTestUtils.waitForCondition(
            () -> {
                Collection<DurableBackgroundTaskState<?>> tasks =
                    tasks(finalIgnite.context().durableBackgroundTask()).values();

                // Tasks whose states are saved in the metastorage are only purged on checkpoint, so
                // check if all tasks are completed.
                return tasks.stream().allMatch(state -> state.state() == DurableBackgroundTaskState.State.COMPLETED);
            },
            TimeUnit.SECONDS.toMillis(5)
        ));

        // Check that there were no errors in background tasks.
        assertTrue(lsnrBackgroundFailure.check());

        // Check that there were no concurrent tree destructions.
        assertTrue(lsnrConcurrentDestruction.check());
    }

    private Map<String, DurableBackgroundTaskState<?>> tasks(DurableBackgroundTasksProcessor proc) {
        return getFieldValue(proc, "tasks");
    }
}
