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

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.apache.ignite.internal.processors.query.h2.QueryMemoryManager;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for ignite SQL system views for queries.
 */
public class SqlQuerySystemViewsSelfTest extends AbstractIndexingCommonTest {
    /** */
    enum RunningQueriesViewField {
        /** */ SQL,
        /** */ QUERY_ID,
        /** */ SCHEMA_NAME,
        /** */ LOCAL,
        /** */ START_TIME,
        /** */ DURATION,
        /** */ MEMORY_CURRENT,
        /** */ MEMORY_MAX,
        /** */ DISK_ALLOCATION_CURRENT,
        /** */ DISK_ALLOCATION_MAX,
        /** */ DISK_ALLOCATION_TOTAL;

        /** */
        public int pos() {
            return ordinal();
        }
    }

    /** */
    enum QueriesHistoryViewField {
        /** */ SCHEMA_NAME,
        /** */ SQL,
        /** */ LOCAL,
        /** */ EXECUTIONS,
        /** */ FAILURES,
        /** */ DURATION_MIN,
        /** */ DURATION_MAX,
        /** */ LAST_START_TIME,
        /** */ MEMORY_MIN,
        /** */ MEMORY_MAX,
        /** */ DISK_ALLOCATION_MIN,
        /** */ DISK_ALLOCATION_MAX,
        /** */ DISK_ALLOCATION_TOTAL_MIN,
        /** */ DISK_ALLOCATION_TOTAL_MAX;

        /** */
        public int pos() {
            return ordinal();
        }
    }

    /** */
    private static final String SELECT_RUNNING_QUERIES = "SELECT " + Arrays.stream(RunningQueriesViewField.values())
        .map(Enum::name).collect(Collectors.joining(", ")) + " FROM " +
        QueryUtils.sysSchemaName() + ".LOCAL_SQL_RUNNING_QUERIES ORDER BY START_TIME";

    /**
     * @return System schema name.
     */
    private String systemSchemaName() {
        return QueryUtils.sysSchemaName();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /** */
    @Before
    public void prepare() {
        grid(0).destroyCache(DEFAULT_CACHE_NAME);
    }

    /**
     * @param ignite Ignite.
     * @param sql Sql.
     * @param args Args.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<List<?>> execSql(Ignite ignite, String sql, Object... args) {
        IgniteCache cache = ignite.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        if (args != null && args.length > 0)
            qry.setArgs(args);

        return cache.query(qry).getAll();
    }

    /**
     * @param sql Sql.
     * @param args Args.
     */
    private List<List<?>> execSql(String sql, Object... args) {
        return execSql(grid(), sql, args);
    }

    /**
     * @param sql Sql.
     */
    private void assertSqlError(final String sql) {
        Throwable t = GridTestUtils.assertThrowsWithCause(new Callable<Void>() {
            @Override public Void call() {
                execSql(sql);

                return null;
            }
        }, IgniteSQLException.class);

        IgniteSQLException sqlE = X.cause(t, IgniteSQLException.class);

        assert sqlE != null;

        assertEquals(IgniteQueryErrorCode.UNSUPPORTED_OPERATION, sqlE.statusCode());
    }

    /**
     * Test Query history system view.
     */
    @Test
    @SuppressWarnings({"ThrowableNotThrown"})
    public void testQueryHistoryMetricsModes() {
        IgniteEx ignite = grid(0);

        final String SCHEMA_NAME = "TEST_SCHEMA";
        final long MAX_SLEEP = 500;
        final long MIN_SLEEP = 50;

        long tsBeforeRun = System.currentTimeMillis();

        final long base = 30;

        AtomicLong mul = new AtomicLong(1);

        GridTestUtils.setFieldValue(
            ignite.context().query().getIndexing(),
            "memoryMgr",
            new QueryMemoryManager(ignite.context()) {
                @Override public GridQueryMemoryMetricProvider createQueryMemoryTracker(long maxQryMemory) {
                    return new TestDummyMemoryMetricProvider() {
                        @Override public long maxReserved() {
                            return base * mul.get();
                        }

                        @Override public long maxWrittenOnDisk() {
                            return base * mul.get();
                        }

                        @Override public long totalWrittenOnDisk() {
                            return base * mul.get();
                        }
                    };
                }
            }
        );

        IgniteCache<Integer, String> cache = ignite.createCache(
            new CacheConfiguration<Integer, String>(DEFAULT_CACHE_NAME)
                .setIndexedTypes(Integer.class, String.class)
                .setSqlSchema(SCHEMA_NAME)
                .setSqlFunctionClasses(GridTestUtils.SqlTestFunctions.class)
        );

        cache.put(100, "200");

        String sql = "SELECT \"STRING\"._KEY, \"STRING\"._VAL FROM \"STRING\" WHERE _key=100 AND sleep_and_can_fail()>0";

        GridTestUtils.SqlTestFunctions.sleepMs = MIN_SLEEP;
        GridTestUtils.SqlTestFunctions.fail = false;

        cache.query(new SqlFieldsQuery(sql).setSchema(SCHEMA_NAME)).getAll();

        mul.incrementAndGet();

        GridTestUtils.SqlTestFunctions.sleepMs = MAX_SLEEP;
        GridTestUtils.SqlTestFunctions.fail = false;

        cache.query(new SqlFieldsQuery(sql).setSchema(SCHEMA_NAME)).getAll();

        mul.incrementAndGet();

        GridTestUtils.SqlTestFunctions.sleepMs = MIN_SLEEP;
        GridTestUtils.SqlTestFunctions.fail = true;

        GridTestUtils.assertThrows(log,
            () ->
                cache.query(new SqlFieldsQuery(sql).setSchema(SCHEMA_NAME)).getAll(),
            CacheException.class,
            "Exception calling user-defined function");

        String sqlHist = "SELECT " + Arrays.stream(QueriesHistoryViewField.values())
            .map(Enum::name).collect(Collectors.joining(", ")) + " FROM " + systemSchemaName()
            + ".LOCAL_SQL_QUERY_HISTORY ORDER BY LAST_START_TIME";

        cache.query(new SqlFieldsQuery(sqlHist).setLocal(true)).getAll();
        cache.query(new SqlFieldsQuery(sqlHist).setLocal(true)).getAll();

        List<List<?>> res = cache.query(new SqlFieldsQuery(sqlHist).setLocal(true)).getAll();

        assertTrue(res.size() >= 2);

        long tsAfterRun = System.currentTimeMillis();

        // first and second row in context of current test
        List<?> firstRow = res.get(res.size() - 2);
        List<?> secondRow = res.get(res.size() - 1);

        assertEquals(SCHEMA_NAME, firstRow.get(QueriesHistoryViewField.SCHEMA_NAME.pos()));
        assertEquals(SCHEMA_NAME, secondRow.get(QueriesHistoryViewField.SCHEMA_NAME.pos()));

        assertEquals(sql, firstRow.get(QueriesHistoryViewField.SQL.pos()));
        assertEquals(sqlHist, secondRow.get(QueriesHistoryViewField.SQL.pos()));

        assertEquals(false, firstRow.get(QueriesHistoryViewField.LOCAL.pos()));
        assertEquals(true, secondRow.get(QueriesHistoryViewField.LOCAL.pos()));

        assertEquals(3L, firstRow.get(QueriesHistoryViewField.EXECUTIONS.pos()));
        assertEquals(2L, secondRow.get(QueriesHistoryViewField.EXECUTIONS.pos()));

        assertEquals(1L, firstRow.get(QueriesHistoryViewField.FAILURES.pos()));
        assertEquals(0L, secondRow.get(QueriesHistoryViewField.FAILURES.pos()));

        assertTrue((Long)firstRow.get(QueriesHistoryViewField.DURATION_MIN.pos()) >= MIN_SLEEP);
        assertTrue((Long)firstRow.get(QueriesHistoryViewField.DURATION_MIN.pos()) < (Long)firstRow.get(6));

        assertTrue((Long)firstRow.get(QueriesHistoryViewField.DURATION_MAX.pos()) >= MAX_SLEEP);

        assertFalse(((Timestamp)firstRow.get(QueriesHistoryViewField.LAST_START_TIME.pos())).before(new Timestamp(tsBeforeRun)));
        assertFalse(((Timestamp)firstRow.get(QueriesHistoryViewField.LAST_START_TIME.pos())).after(new Timestamp(tsAfterRun)));

        assertEquals(base, firstRow.get(QueriesHistoryViewField.MEMORY_MIN.pos()));
        assertEquals(3 * base, secondRow.get(QueriesHistoryViewField.MEMORY_MIN.pos()));

        assertEquals(3 * base, firstRow.get(QueriesHistoryViewField.MEMORY_MAX.pos()));
        assertEquals(3 * base, secondRow.get(QueriesHistoryViewField.MEMORY_MAX.pos()));

        assertEquals(base, firstRow.get(QueriesHistoryViewField.DISK_ALLOCATION_MIN.pos()));
        assertEquals(3 * base, secondRow.get(QueriesHistoryViewField.DISK_ALLOCATION_MIN.pos()));

        assertEquals(3 * base, firstRow.get(QueriesHistoryViewField.DISK_ALLOCATION_MAX.pos()));
        assertEquals(3 * base, secondRow.get(QueriesHistoryViewField.DISK_ALLOCATION_MAX.pos()));

        assertEquals(base, firstRow.get(QueriesHistoryViewField.DISK_ALLOCATION_TOTAL_MIN.pos()));
        assertEquals(3 * base, secondRow.get(QueriesHistoryViewField.DISK_ALLOCATION_TOTAL_MIN.pos()));

        assertEquals(3 * base, firstRow.get(QueriesHistoryViewField.DISK_ALLOCATION_TOTAL_MAX.pos()));
        assertEquals(3 * base, secondRow.get(QueriesHistoryViewField.DISK_ALLOCATION_TOTAL_MAX.pos()));
    }

    /**
     * Ensure memory fields reflects memory tracker changes
     */
    @Test
    public void testRunningQueriesViewMemoryFields() {
        IgniteEx ignite = grid(0);

        final long reserved = 42L;

        GridTestUtils.setFieldValue(
            ignite.context().query().getIndexing(),
            "memoryMgr",
            new QueryMemoryManager(ignite.context()) {
                @Override public GridQueryMemoryMetricProvider createQueryMemoryTracker(long maxQryMemory) {
                    return new TestDummyMemoryMetricProvider() {
                        @Override public long reserved() {
                            return reserved;
                        }

                        @Override public long maxReserved() {
                            return 2 * reserved;
                        }
                    };
                }
            }
        );

        IgniteCache<Integer, String> cache = ignite.createCache(
            new CacheConfiguration<Integer, String>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, String.class)
        );

        List<?> cur = cache.query(new SqlFieldsQuery(SELECT_RUNNING_QUERIES).setLocal(true)).getAll();

        assertEquals(1, cur.size());

        List<?> res0 = (List<?>)cur.get(0);

        assertEquals(reserved, res0.get(RunningQueriesViewField.MEMORY_CURRENT.pos()));
        assertEquals(2 * reserved, res0.get(RunningQueriesViewField.MEMORY_MAX.pos()));
    }

    /**
     * Ensure disc allocation fields reflects memory tracker changes
     */
    @Test
    public void testRunningQueriesViewDiscAllocationFields() {
        IgniteEx ignite = grid(0);

        final long written = 42L;

        GridTestUtils.setFieldValue(
            ignite.context().query().getIndexing(),
            "memoryMgr",
            new QueryMemoryManager(ignite.context()) {
                @Override public GridQueryMemoryMetricProvider createQueryMemoryTracker(long maxQryMemory) {
                    return new TestDummyMemoryMetricProvider() {
                        @Override public long writtenOnDisk() {
                            return written;
                        }

                        @Override public long maxWrittenOnDisk() {
                            return 2 * written;
                        }

                        @Override public long totalWrittenOnDisk() {
                            return 3 * written;
                        }
                    };
                }
            }
        );

        IgniteCache<Integer, String> cache = ignite.createCache(
            new CacheConfiguration<Integer, String>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, String.class)
        );

        List<?> cur = cache.query(new SqlFieldsQuery(SELECT_RUNNING_QUERIES).setLocal(true)).getAll();

        assertEquals(1, cur.size());

        List<?> res0 = (List<?>)cur.get(0);

        assertEquals(written, res0.get(RunningQueriesViewField.DISK_ALLOCATION_CURRENT.pos()));
        assertEquals(2 * written, res0.get(RunningQueriesViewField.DISK_ALLOCATION_MAX.pos()));
        assertEquals(3 * written, res0.get(RunningQueriesViewField.DISK_ALLOCATION_TOTAL.pos()));
    }

    /**
     * Ensure start time and duration fields reflects query execution process
     */
    @Test
    public void testRunningQueriesViewStartTimeDuration() {
        IgniteEx ignite = grid(0);

        IgniteCache<Integer, String> cache = ignite.createCache(
            new CacheConfiguration<Integer, String>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, String.class)
        );

        long before = System.currentTimeMillis();

        try (FieldsQueryCursor<?> notClosedFieldQryCursor = cache.query(new SqlFieldsQuery(SELECT_RUNNING_QUERIES).setLocal(true))) {
            List<List<?>> cur = cache.query(new SqlFieldsQuery(SELECT_RUNNING_QUERIES).setLocal(true)).getAll();

            long after = System.currentTimeMillis();

            assertEquals(2, cur.size());

            long startTime = ((Date)cur.get(0).get(RunningQueriesViewField.START_TIME.pos())).getTime();
            long prevDuration = (Long)cur.get(0).get(RunningQueriesViewField.DURATION.pos());

            assertTrue(after + " >= " + startTime, after >= startTime);
            assertTrue(prevDuration + " >= " + 0, prevDuration >= 0);

            for (int i = 0; i < 3; i++) {
                cur = cache.query(new SqlFieldsQuery(SELECT_RUNNING_QUERIES).setLocal(true)).getAll();

                long actStartTime = ((Date)cur.get(0).get(RunningQueriesViewField.START_TIME.pos())).getTime();
                long currDuration = (Long)cur.get(0).get(RunningQueriesViewField.DURATION.pos());

                assertEquals(startTime, actStartTime);

                long maxDuration = System.currentTimeMillis() - before;

                assertTrue(maxDuration + " >= " + currDuration, maxDuration >= currDuration);
            }
        }
    }

    /**
     * Ensure showed queries matches with real running queries.
     */
    @Test
    public void testRunningQueriesViewMetaInfo() {
        IgniteEx ignite = grid(0);

        IgniteCache<Integer, String> cache = ignite.createCache(
            new CacheConfiguration<Integer, String>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, String.class)
        );

        try (FieldsQueryCursor<?> notClosedFieldQryCursor = cache.query(new SqlFieldsQuery(SELECT_RUNNING_QUERIES).setLocal(true))) {
            List<List<?>> cur = cache.query(new SqlFieldsQuery(SELECT_RUNNING_QUERIES).setLocal(true)).getAll();

            assertEquals(2, cur.size());

            assertEquals(SELECT_RUNNING_QUERIES, cur.get(0).get(RunningQueriesViewField.SQL.pos()));
            assertEquals(SELECT_RUNNING_QUERIES, cur.get(1).get(RunningQueriesViewField.SQL.pos()));

            assertTrue((Boolean)cur.get(0).get(RunningQueriesViewField.LOCAL.pos()));
            assertTrue((Boolean)cur.get(1).get(RunningQueriesViewField.LOCAL.pos()));

            int lastQryNum = Integer.parseInt(
                cur.get(1).get(RunningQueriesViewField.QUERY_ID.pos()).toString().split("_")[1]
            );

            String nodeId = ignite.context().localNodeId().toString();

            for (int i = lastQryNum + 1; i < lastQryNum + 3; i++) {
                cur = cache.query(new SqlFieldsQuery(SELECT_RUNNING_QUERIES).setLocal(true)).getAll();

                String qryId = cur.get(1).get(RunningQueriesViewField.QUERY_ID.pos()).toString();

                assertEquals("Exp: " + nodeId + "_" + i + "; Act: " + qryId, nodeId + "_" + i, qryId);
            }

            assertEquals(2, cache.query(new SqlFieldsQuery(SELECT_RUNNING_QUERIES)).getAll().size());
        }

        assertEquals(1, cache.query(new SqlFieldsQuery(SELECT_RUNNING_QUERIES)).getAll().size());

        try (QueryCursor<?> notClosedQryCursor = cache.query(new SqlQuery<>(String.class, "_key=100"))) {
            String expSqlQry = "SELECT \"default\".\"STRING\"._KEY, \"default\".\"STRING\"._VAL FROM " +
                "\"default\".\"STRING\" WHERE _key=100";

            List<List<?>> cur = cache.query(new SqlFieldsQuery(SELECT_RUNNING_QUERIES)).getAll();

            assertEquals(2, cur.size());

            assertEquals(expSqlQry, cur.get(0).get(RunningQueriesViewField.SQL.pos()));

            assertFalse((Boolean)cur.get(0).get(RunningQueriesViewField.LOCAL.pos()));
            assertFalse((Boolean)cur.get(1).get(RunningQueriesViewField.LOCAL.pos()));
        }

        String sql = "SELECT * FROM " + systemSchemaName() + ".LOCAL_SQL_RUNNING_QUERIES WHERE DURATION > 100000";

        assertTrue(cache.query(new SqlFieldsQuery(sql)).getAll().isEmpty());

        sql = "SELECT * FROM " + systemSchemaName() + ".LOCAL_SQL_RUNNING_QUERIES WHERE QUERY_ID='UNKNOWN'";

        assertTrue(cache.query(new SqlFieldsQuery(sql)).getAll().isEmpty());
    }

    /**
     * Dummy tracker that tracks nothing and returns zeros.
     */
    private static class TestDummyMemoryMetricProvider implements GridQueryMemoryMetricProvider, H2MemoryTracker {
        /** {@inheritDoc} */
        @Override public void close() {
            // NO-OP
        }

        /** {@inheritDoc} */
        @Override public boolean reserve(long size) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public void release(long size) {
            // NO-OP
        }

        /** {@inheritDoc} */
        @Override public long reserved() {
            return 0L;
        }

        /** {@inheritDoc} */
        @Override public long maxReserved() {
            return 0L;
        }

        /** {@inheritDoc} */
        @Override public long writtenOnDisk() {
            return 0L;
        }

        /** {@inheritDoc} */
        @Override public long maxWrittenOnDisk() {
            return 0L;
        }

        /** {@inheritDoc} */
        @Override public long totalWrittenOnDisk() {
            return 0L;
        }

        /** {@inheritDoc} */
        @Override public void spill(long size) {
            // NO-OP
        }

        /** {@inheritDoc} */
        @Override public void unspill(long size) {
            // NO-OP
        }

        /** {@inheritDoc} */
        @Override public void incrementFilesCreated() {
            // NO-OP
        }

        /** {@inheritDoc} */
        @Override public H2MemoryTracker createChildTracker() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void onChildClosed(H2MemoryTracker child) {
            // NO-OP
        }

        /** {@inheritDoc} */
        @Override public boolean closed() {
            return false;
        }
    }
}
