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

package org.apache.ignite.internal.metric;

import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.processors.query.RunningQueryManager;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.TransactionDuplicateKeyException;
import org.junit.Test;

/**
 * Tests for statistics of user initiated queries execution, that can be runned without grid restart.
 *
 * @see RunningQueryManager
 */
public class SqlStatisticsUserQueriesFastTest extends UserQueriesTestBase {
    /** Cache with a tested table, created and filled only once. */
    private static IgniteCache cache;

    /**
     * Setup.
     */
    @Override protected void beforeTestsStarted() throws Exception {
        SuspendQuerySqlFunctions.refresh();

        startGrids(2);

        cache = createCacheFrom(grid(REDUCER_IDX));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Check that one distributed query execution causes only success metric increment only on the reducer node. Various
     * (not all) queries tested : native/h2 parsed; select, ddl, dml, fast delete, update with subselect.
     */
    @Test
    public void testSmokeDdlDml() throws Exception {
        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlQuery(String.class, "ID < 5")).getAll(),
            "success");

        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlFieldsQuery("SELECT * FROM TAB")).getAll(),
            "success");

        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlFieldsQuery("CREATE INDEX myidx ON TAB(ID)")).getAll(),
            "success");

        assertMetricsIncrementedOnlyOnReducer(() -> GridTestUtils.assertThrows(
            log,
            () -> cache.query(new SqlFieldsQuery("CREATE INDEX myidx ON TAB(ID)")).getAll(),
            CacheException.class,
            "Index already exists"),
            "failed");

        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlFieldsQuery("DROP INDEX myidx")).getAll(),
            "success");


        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlFieldsQuery("CREATE TABLE ANOTHER_TAB (ID INT PRIMARY KEY, VAL VARCHAR)")
                .setSchema("PUBLIC")).getAll(), "success");

        assertMetricsIncrementedOnlyOnReducer(() -> GridTestUtils.assertThrows(
            log,
            () -> cache.query(new SqlFieldsQuery("CREATE TABLE ANOTHER_TAB (ID INT PRIMARY KEY, VAL VARCHAR)")
                .setSchema("PUBLIC")).getAll(),
            CacheException.class,
            "Table already exists"),
            "failed");


        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlFieldsQuery("DELETE FROM TAB WHERE ID = 5")).getAll(),
            "success");

        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlFieldsQuery("DELETE FROM TAB WHERE ID > (SELECT AVG(ID) FROM TAB WHERE ID < 20)")).getAll(),
            "success");

        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlFieldsQuery("INSERT INTO TAB VALUES(5, 'Name')")).getAll(),
            "success");

        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlFieldsQuery("MERGE INTO TAB(ID, NAME) VALUES(5, 'NewerName')")).getAll(),
            "success");

        assertMetricsIncrementedOnlyOnReducer(() -> GridTestUtils.assertThrowsAnyCause(
            log,
            () -> cache.query(new SqlFieldsQuery("INSERT INTO TAB VALUES(5, 'I wont be inserted')")).getAll(),
            TransactionDuplicateKeyException.class,
            "Duplicate key during INSERT"),
            "failed");
    }

    /**
     * Local queries should also be counted.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testIfLocalQuerySucceedsMetricIsUpdated() throws Exception {
        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlFieldsQuery("SELECT * FROM TAB WHERE ID < 100").setLocal(true)).getAll(),
            "success");

        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlQuery(String.class, "ID < 5").setLocal(true)).getAll(),
            "success");
    }

    /**
     * Check that unparseable query doesn't affect any metric value.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testUnparseableQueriesAreNotCounted() throws Exception {
        assertMetricsRemainTheSame(() -> {
            GridTestUtils.assertThrows(
                log,
                () -> cache.query(new SqlFieldsQuery("THIS IS NOT A SQL STATEMENT")).getAll(),
                CacheException.class,
                "Failed to parse query");

        });
    }

    /**
     * Check success metric in case of local select.
     *
     */
    @Test
    public void testLocalSelectSuccess() {
        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlFieldsQuery("SELECT * FROM TAB WHERE ID < 100").setLocal(true)).getAll(),
            "success");
    }

    /**
     * Check general failure metric if local select failed.
     *
     */
    @Test
    public void testLocalSelectFailed() {
        assertMetricsIncrementedOnlyOnReducer(() -> GridTestUtils.assertThrows(
            log,
            () -> cache.query(new SqlFieldsQuery("SELECT * FROM TAB WHERE ID = failFunction()").setLocal(true)).getAll(),
            CacheException.class,
            null),
            "failed");
    }

    /**
     * Check cancel metric if local select cancelled.
     *
     */
    @Test
    public void testLocalSelectCanceled() {
        assertMetricsIncrementedOnlyOnReducer(() ->
                startAndKillQuery(new SqlFieldsQuery("SELECT * FROM TAB WHERE ID <> suspendHook(ID)").setLocal(true)),
            "success",
            "failed",
            "canceled");
    }
}
