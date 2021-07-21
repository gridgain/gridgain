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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.LongRunningQueryManager;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.h2.LongRunningQueryManager.LONG_QUERY_EXEC_MSG;

/**
 * Tests for log print for long running query.
 */
public class LongRunningQueryThrottlingTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int QRY_PARALLELISM = 10;

    /** Keys count. */
    private static final int KEY_CNT = 1000 * QRY_PARALLELISM;

    /** Local query mode. */
    private boolean local;

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
            .setQueryParallelism(QRY_PARALLELISM)
            .setSqlFunctionClasses(GridTestUtils.SqlTestFunctions.class));

        for (long i = 0; i < KEY_CNT; ++i)
            c.put(i, i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testLongDistributed() {
        local = false;
        lazy = false;

        checkLongRunning();
    }

    /** */
    @Test
    public void testLongLocal() {
        local = true;
        lazy = false;

        checkLongRunning();
    }

    /**
     * Do long running query canceled by timeout and check log output.
     * Log messages must contain info about long query.
     */
    private void checkLongRunning() {
        ListeningTestLogger testLog = testLog();

        AtomicInteger msgCnt = new AtomicInteger();

        LogListener lsnr = LogListener
            .matches((s) -> {
                if (s.contains(LONG_QUERY_EXEC_MSG)) {
                    msgCnt.incrementAndGet();

                    return true;
                }

                return false;
            })
            .build();

        testLog.registerListener(lsnr);

        sqlCheckLongRunning();

        assertTrue(lsnr.check());

        assertEquals(1, msgCnt.get());
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     */
    private void sqlCheckLongRunning(String sql, Object... args) {
        GridTestUtils.assertThrowsAnyCause(log, () -> sql(sql, args).getAll(), QueryCancelledException.class, "");
    }

    /**
     * Execute long running sql with a check for errors.
     */
    private void sqlCheckLongRunning() {
        sqlCheckLongRunning("SELECT T0.id, delay(100) FROM test AS T0, test AS T1, test AS T2 where T0.id > ?", 0);
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return grid().context().query().querySqlFields(new SqlFieldsQueryEx(sql, true)
            .setTimeout(10, TimeUnit.SECONDS)
            .setLocal(local)
            .setLazy(lazy)
            .setSchema("TEST")
            .setArgs(args), false);
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
