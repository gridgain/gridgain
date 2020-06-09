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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.RunningQueryManager;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.TransactionDuplicateKeyException;
import org.junit.Test;

import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;

/**
 * Tests for statistics of user initiated queries execution, that can be runned without grid restart.
 *
 * @see RunningQueryManager
 */
public class SqlStatisticsUserQueriesFastTest extends UserQueriesTestBase {
    /** Subdirectory with CSV files */
    private static final String CSV_FILE_SUBDIR = "/modules/indexing/src/test/resources/";

    /**
     * A CSV file with two records, that could NOT be inserted to the test table, because it have been generated for
     * different table.
     */
    private static final String COPY_CMD_BAD_FORMATED_FILE =
        Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload_bad.csv")).getAbsolutePath();

    /**
     * A CSV file with two records, that could be upload to the test table.
     */
    private static final String COPY_CMD_OK_FORMATED_FILE =
        Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload_ok.csv")).getAbsolutePath();

    /** Cache with a tested table, created and populated only once. */
    private static IgniteCache cache;

    /**
     * Setup.
     */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);

        cache = createCacheFrom(grid(REDUCER_IDX));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        SuspendQuerySqlFunctions.refresh();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Sanity check for selects.
     */
    @Test
    public void testSanitySelectSuccess() {
        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlFieldsQuery("SELECT * FROM TAB")).getAll(),
            "success");

        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlFieldsQuery("SELECT * FROM TAB WHERE ID = (SELECT AVG(ID) FROM TAB WHERE ID < 20)")).getAll(),
            "success");
    }

    /**
     * Check that metrics work for DDL statements.
     */
    @Test
    public void testDdlSuccess() {
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
    }

    /**
     * Check that metrics work for DML statements.
     */
    @Test
    public void testDmlSuccess() {
        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlFieldsQuery("DELETE FROM TAB WHERE ID = 5")).getAll(),
            "success");

        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlFieldsQuery("DELETE FROM TAB WHERE ID < (SELECT AVG(ID) FROM TAB WHERE ID < 20)")).getAll(),
            "success");

        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlFieldsQuery("INSERT INTO TAB VALUES(5, 'Name')")).getAll(),
            "success");

        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlFieldsQuery("MERGE INTO TAB(ID, NAME) VALUES(5, 'NewerName')")).getAll(),
            "success");

        assertMetricsIncrementedOnlyOnReducer(() -> GridTestUtils.assertThrowsAnyCause(
            log,
            () -> cache.query(new SqlFieldsQuery("INSERT INTO TAB VALUES(5, 'I will NOT be inserted')")).getAll(),
            TransactionDuplicateKeyException.class,
            "Duplicate key during INSERT"),
            "failed");
    }

    /**
     * Check that metrics work for statements in streaming mode.
     */
    @Test
    public void testStreaming() {
        final Integer okId = 42;
        final Integer badId = null;

        cache.query(new SqlFieldsQuery("DELETE FROM TAB WHERE ID = ?").setArgs(okId)).getAll();

        assertMetricsIncrementedOnlyOnReducer(
            () -> insertWithStreaming(okId, "Succesfully inserted name"),
            "success", "success");

        assertMetricsIncrementedOnlyOnReducer(() -> GridTestUtils.assertThrowsAnyCause(
            log,
            () -> insertWithStreaming(badId, "I will NOT be inserted"),
            BatchUpdateException.class,
            "Null value is not allowed for column"),
            "success", "failed");
    }

    /**
     * Insert row using streaming mode of the Thin JDBC client.
     *
     * @param id Id.
     * @param name Name.
     * @return update count.
     */
    private int insertWithStreaming(Integer id, String name) {
        try (Connection conn = GridTestUtils.connect(grid(REDUCER_IDX), null)) {
            conn.setSchema('"' + DEFAULT_CACHE_NAME + '"');

            try (Statement stat = conn.createStatement()) {
                stat.execute("SET STREAMING ON ALLOW_OVERWRITE OFF");

                try (PreparedStatement ins = conn.prepareStatement("INSERT INTO TAB VALUES(?, ?)")) {
                    ins.setObject(1, id);
                    ins.setString(2, name);

                    return ins.executeUpdate();
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Streaming upload failed", e);
        }
    }

    /**
     * Check that metrics work for COPY statement.
     */
    @Test
    public void testCopyComand() {
        cache.query(new SqlFieldsQuery("DELETE FROM TAB WHERE ID = 1 or ID = 2 ")).getAll();

        assertMetricsIncrementedOnlyOnReducer(
            () -> doCopyCommand(COPY_CMD_OK_FORMATED_FILE),
            "success");

        assertMetricsIncrementedOnlyOnReducer(() -> GridTestUtils.assertThrowsAnyCause(
            log,
            () -> doCopyCommand(COPY_CMD_BAD_FORMATED_FILE),
            SQLException.class,
            "Value conversion failed"),
            "failed");
    }

    /**
     * Perform copy command: upload file using thin jdbc client.
     *
     * @param pathToCsv Path to csv file to upload.
     */
    private int doCopyCommand(String pathToCsv) {
        try (Connection conn = GridTestUtils.connect(grid(REDUCER_IDX), null)) {
            conn.setSchema('"' + DEFAULT_CACHE_NAME + '"');

            try (Statement copy = conn.createStatement()) {
                return copy.executeUpdate("copy from '" + pathToCsv + "' into TAB (ID, NAME) format csv");
            }
        }
        catch (Exception e) {
            throw new RuntimeException("COPY upload from " + pathToCsv + " failed", e);
        }
    }

    /**
     * Sanity test for deprecated, but still supported by metrics, sql queries.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testSanityDeprecatedSqlQueryMetrics() throws Exception {
        assertMetricsIncrementedOnlyOnReducer(
            () -> cache.query(new SqlQuery(String.class, "ID < 5").setLocal(false)).getAll(),
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
     */
    @Test
    public void testLocalSelectFailed() {
        assertMetricsIncrementedOnlyOnReducer(() -> {
                try {
                    cache.query(new SqlFieldsQuery("SELECT * FROM TAB WHERE ID = failFunction()").setLocal(true)).getAll();
                    fail("Exception must be thrown");
                }
                catch (CacheException | IgniteSQLException e) {
                    // Expected exception.
                }
            },
            "failed");
    }

    /**
     * Check general failure metric if local select failed.
     */
    @Test
    public void testLocalLazySelectFailedOnIterator() {
        assertMetricsIncrementedOnlyOnReducer(() -> {
                try (FieldsQueryCursor<List<?>> cur = cache.query(
                    new SqlFieldsQuery("SELECT * FROM TAB WHERE ID = failFunction()")
                        .setLocal(true)
                        .setLazy(true))) {
                    Iterator<List<?>> it = cur.iterator();

                    it.next();

                    fail("Exception must be thrown");
                }
                catch (CacheException | IgniteSQLException e) {
                    // Expected exception.
                }
            },
            "failed");
    }

    /**
     * Check cancel metric if local select cancelled.
     */
    @Test
    public void testLocalSelectCanceled() {
        assertMetricsIncrementedOnlyOnReducer(() ->
                startAndKillQuery(new SqlFieldsQuery("SELECT * FROM TAB WHERE ID <> suspendHook(ID)").setLocal(true)),
            "success",
            "failed",
            "canceled");
    }

    /**
     * Check cancel metric if local select cancelled.
     */
    @Test
    public void testLocalLazySelectCanceledOnIterator() {
        assertMetricsIncrementedOnlyOnReducer(() -> {
                IgniteInternalFuture qryCanceled = runAsyncX(() -> GridTestUtils.assertThrowsAnyCause(
                    log,
                    () -> {
                        Iterator<List<?>> it = jcache(REDUCER_IDX).query(new SqlFieldsQuery("SELECT * FROM TAB WHERE ID <> suspendHook(ID)")
                            .setLocal(true)
                            .setLazy(true)).iterator();

                        while (it.hasNext())
                            it.next();

                        return null;
                    },
                    QueryCancelledException.class,
                    null)
                );

                try {
                    SuspendQuerySqlFunctions.awaitQueryStopsInTheMiddle();

                    // We perform async kill and hope it does it's job in some time.
                    killAsyncAllQueriesOn(REDUCER_IDX);

                    TimeUnit.SECONDS.sleep(WAIT_FOR_KILL_SEC);

                    SuspendQuerySqlFunctions.resumeQueryExecution();

                    qryCanceled.get(WAIT_OP_TIMEOUT_SEC, TimeUnit.SECONDS);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            },
            "success",
            "failed",
            "canceled");
    }
}
