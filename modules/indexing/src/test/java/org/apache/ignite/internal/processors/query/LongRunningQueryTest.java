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

package org.apache.ignite.internal.processors.query;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.LongRunningQueryManager;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static java.lang.Thread.currentThread;
import static org.apache.ignite.internal.processors.query.h2.LongRunningQueryManager.LONG_QUERY_EXEC_MSG;
import static org.gridgain.internal.h2.engine.Constants.DEFAULT_PAGE_SIZE;

/**
 * Tests for log print for long running query.
 */
public class LongRunningQueryTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEY_CNT = 1000;

    /** Query label. */
    private static final String LRQ_LABEL = "test-label";

    /** Query label substring pattern. */
    private static final Pattern LABEL_PATTERN = Pattern.compile(", label=" + LRQ_LABEL + ',');

    /** Local query mode. */
    private boolean local;

    /** Page size. */
    private int pageSize = DEFAULT_PAGE_SIZE;

    /** Number of keys to be queries in lazy queries. */
    private static final int LAZY_QRYS_KEY_CNT = 5;

    /** Lazy query mode. */
    private boolean lazy;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid();

        IgniteCache c = grid().createCache(new CacheConfiguration<Long, Long>()
            .setName("test")
            .setSqlSchema("TEST")
            .setQueryEntities(Collections.singleton(new QueryEntity(Long.class, Long.class)
                .setTableName("test")
                .addQueryField("id", Long.class.getName(), null)
                .addQueryField("val", Long.class.getName(), null)
                .setKeyFieldName("id")
                .setValueFieldName("val")
            ))
            .setAffinity(new RendezvousAffinityFunction(false, 10))
            .setSqlFunctionClasses(TestSQLFunctions.class));

        for (long i = 0; i < KEY_CNT; ++i)
            c.put(i, i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     *
     */
    @Test
    public void testLongDistributed() {
        local = false;
        lazy = false;

        checkLongRunning();
        checkFastQueries();
    }

    /**
     *
     */
    @Test
    public void testLongDistributedLazy() {
        local = false;
        lazy = true;

        checkLongRunning();
        checkFastQueries();
    }

    /**
     *
     */
    @Test
    public void testLongDmlDistributed() {
        local = false;
        lazy = false;

        checkLongRunningDml();
        checkFastQueries();
    }

    /**
     *
     */
    @Test
    public void testLongLocal() {
        local = true;
        lazy = false;

        checkLongRunning();
        checkFastQueries();
    }

    /**
     *
     */
    @Test
    public void testLongLocalLazy() {
        local = true;
        lazy = true;

        checkLongRunning();
        checkFastQueries();
    }

    /**
     *
     */
    @Test
    public void testLongDmlLocal() {
        local = true;
        lazy = false;

        checkLongRunningDml();
        checkFastQueries();
    }

    /**
     *
     */
    @Test
    public void testBigResultSetLocal() throws Exception {
        local = true;
        lazy = true;

        checkBigResultSet();
    }

    /**
     *
     */
    @Test
    public void testBigResultDistributed() throws Exception {
        local = false;
        lazy = true;

        checkBigResultSet();
    }

    /**
     * Test checks the correctness of thread name when displaying errors
     * about long queries.
     */
    @Test
    public void testCorrectThreadName() {
        GridWorker checkWorker = GridTestUtils.getFieldValue(longRunningQueryManager(), "checkWorker");

        LogListener logLsnr = LogListener
            .matches(LONG_QUERY_EXEC_MSG)
            .andMatches(logStr -> currentThread().getName().startsWith(checkWorker.name()))
            .andMatches(LABEL_PATTERN)
            .build();

        testLog().registerListener(logLsnr);

        sqlCheckLongRunning();

        assertTrue(logLsnr.check());
    }

    /**
     * Do several fast queries.
     * Log messages must not contain info about long query.
     */
    private void checkFastQueries() {
        ListeningTestLogger testLog = testLog();

        LogListener lsnr = LogListener
            .matches(Pattern.compile(LONG_QUERY_EXEC_MSG))
            .andMatches(LABEL_PATTERN)
            .build();

        testLog.registerListener(lsnr);

        // Several fast queries.
        for (int i = 0; i < 10; ++i)
            sql("SELECT * FROM test").getAll();

        assertFalse(lsnr.check());
    }

    /**
     * Do long running SELECT query canceled by timeout and check log output.
     * Log messages must contain info about long query.
     */
    private void checkLongRunning() {
        checkLongRunning(false);
    }

    /**
     * Do long running DML query canceled by timeout and check log output.
     * Log messages must contain info about long query.
     */
    private void checkLongRunningDml() {
        checkLongRunning(true);
    }

    private void checkLongRunning(boolean dml) {
        ListeningTestLogger testLog = testLog();

        LogListener lsnr = LogListener
            .matches(LONG_QUERY_EXEC_MSG)
            .andMatches(LABEL_PATTERN)
            .build();

        testLog.registerListener(lsnr);

        sqlCheckLongRunning(dml);

        assertTrue(lsnr.check());
    }

    /**
     */
    private void checkBigResultSet() throws Exception {
        ListeningTestLogger testLog = testLog();

        LogListener lsnr = LogListener
            .matches("Query produced big result set")
            .andMatches(LABEL_PATTERN)
            .build();

        testLog.registerListener(lsnr);

        try (FieldsQueryCursor cur = sql("SELECT T0.id FROM test AS T0, test AS T1")) {
            Iterator it = cur.iterator();

            while (it.hasNext())
                it.next();
        }

        assertTrue(lsnr.check(1_000));
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     */
    private void sqlCheckLongRunning(String sql, Object... args) {
        GridTestUtils.assertThrowsAnyCause(log, () -> sql(sql, args).getAll(), QueryCancelledException.class, "");
    }

    private void sqlCheckLongRunning() {
        sqlCheckLongRunning(false);
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     */
    private void sqlCheckLongRunningLazy(String sql, Object... args) {
        pageSize = 1;

        try {
            assertEquals(LAZY_QRYS_KEY_CNT, sql(sql, args).getAll().size());
        }
        finally {
            pageSize = DEFAULT_PAGE_SIZE;
        }
    }

    /**
     * Execute long running sql with a check for errors.
     */
    private void sqlCheckLongRunning(boolean dml) {
        if (dml)
            sqlCheckLongRunning("DELETE FROM test WHERE id not in (SELECT T0.id FROM test AS T0, test AS T1, test AS T2 where T0.id > ?)", 0);
        else if (lazy)
            sqlCheckLongRunningLazy("SELECT * FROM test WHERE _key < sleep_func(?, ?)", 2000, LAZY_QRYS_KEY_CNT);
        else
            sqlCheckLongRunning("SELECT T0.id FROM test AS T0, test AS T1, test AS T2 where T0.id > ?", 0);
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return grid().context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setTimeout(10, TimeUnit.SECONDS)
            .setLocal(local)
            .setLazy(lazy)
            .setSchema("TEST")
            .setPageSize(pageSize)
            .setLabel(LRQ_LABEL)
            .setArgs(args), false);
    }

    /**
     * Utility class with custom SQL functions.
     */
    public static class TestSQLFunctions {
        /**
         * @param sleep amount of milliseconds to sleep
         * @param val value to be returned by the function
         */
        @SuppressWarnings("unused")
        @QuerySqlFunction
        public static int sleep_func(int sleep, int val) {
            try {
                Thread.sleep(sleep);
            }
            catch (InterruptedException ignored) {
                // No-op
            }
            return val;
        }
    }

    /**
     * Setup and return test log.
     *
     * @return Test logger.
     */
    private ListeningTestLogger testLog() {
        ListeningTestLogger testLog = new ListeningTestLogger(false, log);

        GridTestUtils.setFieldValue(longRunningQueryManager(), "log", testLog);

        GridTestUtils.setFieldValue(((IgniteH2Indexing)grid().context().query().getIndexing()).mapQueryExecutor(),
            "log", testLog);

        GridTestUtils.setFieldValue(grid().context().query().getIndexing(), "log", testLog);

        return testLog;
    }

    /**
     * Getting {@link LongRunningQueryManager} from the node.
     *
     * @return LongRunningQueryManager.
     */
    private LongRunningQueryManager longRunningQueryManager() {
        return ((IgniteH2Indexing)grid().context().query().getIndexing()).longRunningQueries();
    }
}
