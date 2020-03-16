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
package org.apache.ignite.internal.processors.query.oom;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.H2LocalResultFactory;
import org.apache.ignite.internal.processors.query.h2.H2ManagedLocalResult;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.QueryMemoryManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.result.LocalResult;
import org.h2.result.LocalResultImpl;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_ENABLE_CONNECTION_MEMORY_QUOTA;
import static org.apache.ignite.internal.util.IgniteUtils.KB;
import static org.apache.ignite.internal.util.IgniteUtils.MB;

/**
 * Basic test class for quotas.
 */
@WithSystemProperty(key = IGNITE_SQL_ENABLE_CONNECTION_MEMORY_QUOTA, value = "true")
public abstract class AbstractQueryMemoryTrackerSelfTest extends GridCommonAbstractTest {
    /** Row count. */
    static final int SMALL_TABLE_SIZE = 1000;

    /** Row count. */
    static final int BIG_TABLE_SIZE = 10_000;

    /** Query local results. */
    static final List<H2ManagedLocalResult> localResults = Collections.synchronizedList(new ArrayList<>());

    /** Query memory limit. */
    protected long maxMem;

    /** Node client mode flag. */
    private boolean client;

    /** Flag whether to use jdbc2 node config with global quota. */
    protected boolean useJdbcV2GlobalQuotaCfg;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(IgniteSystemProperties.IGNITE_H2_LOCAL_RESULT_FACTORY, TestH2LocalResultFactory.class.getName());
        System.setProperty(IgniteSystemProperties.IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE, Long.toString(KB));

        startGrid(0);

        if (startClient()) {
            client = true;

            startGrid(1);
        }

        createSchema();

        populateData();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        System.clearProperty(IgniteSystemProperties.IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE);
        System.clearProperty(IgniteSystemProperties.IGNITE_H2_LOCAL_RESULT_FACTORY);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        maxMem = MB;
        useJdbcV2GlobalQuotaCfg = false;

        for (H2ManagedLocalResult res : localResults) {
            if (res.memoryTracker() != null)
                res.memoryTracker().close();
        }

        localResults.clear();

        resetMemoryManagerState(grid(0));

        if (startClient())
            resetMemoryManagerState(grid(1));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (H2ManagedLocalResult res : localResults) {
            if (res.memoryTracker() != null)
                res.memoryTracker().close();
        }

        checkMemoryManagerState(grid(0));

        if (startClient())
            checkMemoryManagerState(grid(1));

        super.afterTest();
    }

    /**
     * Check if all reserved memory was correctly released.
     * @param node Node.
     */
    private void checkMemoryManagerState(IgniteEx node) throws Exception {
        final QueryMemoryManager memMgr = memoryManager(node);

        GridTestUtils.waitForCondition(() -> memMgr.reserved() == 0, 5_000);

        long memReserved = memMgr.reserved();

        assertEquals("Potential memory leak in SQL engine: reserved=" + memReserved, 0, memReserved);
    }

    /**
     * Resets node query memory manager state.
     *
     * @param grid Node.
     */
    private void resetMemoryManagerState(IgniteEx grid) {
        QueryMemoryManager memoryManager = memoryManager(grid);

        // Reset memory manager.
        if (memoryManager.reserved() > 0)
            memoryManager.release(memoryManager.reserved());
    }

    /**
     * Return node query memory manager.
     *
     * @param node Node.
     * @return Query memory manager.
     */
    private QueryMemoryManager memoryManager(IgniteEx node) {
        IgniteH2Indexing h2 = (IgniteH2Indexing)node.context().query().getIndexing();

        return h2.memoryManager();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setClientMode(client)
            .setSqlOffloadingEnabled(false)
            .setSqlGlobalMemoryQuota(Long.toString(globalQuotaSize()));
    }

    /** */
    protected long globalQuotaSize() {
        return 10L * MB;
    }

    /**
     *
     */
    private void populateData() {
        for (int i = 0; i < SMALL_TABLE_SIZE; ++i)
            execSql("insert into T VALUES (?, ?, ?)", i, i, UUID.randomUUID().toString());

        for (int i = 0; i < BIG_TABLE_SIZE; ++i)
            execSql("insert into K VALUES (?, ?, ?, ?, ?)", i, i, i % 100, i % 100, UUID.randomUUID().toString());
    }

    /**
     *
     */
    protected void createSchema() {
        execSql("create table T (id int primary key, ref_key int, name varchar)");
        execSql("create table K (id int primary key, indexed int, grp int, grp_indexed int, name varchar)");
        execSql("create index K_IDX on K(indexed)");
        execSql("create index K_GRP_IDX on K(grp_indexed)");
    }


    /**
     * @param sql SQL query
     * @return Results set.
     */
    protected List<List<?>> execQuery(String sql, boolean lazy) throws Exception {
        try (FieldsQueryCursor<List<?>> cursor = query(sql, lazy)) {
            return cursor.getAll();
        }
    }

    /**
     * @param sql SQL query
     * @return Results set.
     */
    FieldsQueryCursor<List<?>> query(String sql, boolean lazy) throws Exception {
        boolean localQry = isLocal();

        return grid(startClient() ? 1 : 0).context().query().querySqlFields(
            new SqlFieldsQueryEx(sql, null)
                .setLocal(localQry)
                .setMaxMemory(maxMem)
                .setLazy(lazy)
                .setEnforceJoinOrder(true)
                .setPageSize(100), false);
    }

    /**
     * @return Local query flag.
     */
    protected abstract boolean isLocal();

    /**
     * @return {@code True} if client node should be started, {@code False} otherwise.
     */
    protected boolean startClient() {
        return !isLocal();
    }

    /**
     * @param sql SQL query
     * @param args Query parameters.
     */
    protected void execSql(String sql, Object... args) {
        grid(0).context().query().querySqlFields(
            new SqlFieldsQuery(sql).setArgs(args), false).getAll();
    }

    /**
     * @param sql SQL query.
     * @param lazy Lazy flag.
     */
    protected void checkQueryExpectOOM(String sql, boolean lazy) {
        IgniteSQLException sqlEx = (IgniteSQLException)GridTestUtils.assertThrowsAnyCause(log, () -> {
            execQuery(sql, lazy);

            return null;
        }, IgniteSQLException.class, "SQL query run out of memory: Query quota exceeded.");

        assertNotNull("SQL exception missed.", sqlEx);
        assertEquals(IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY, sqlEx.statusCode());
        assertEquals(IgniteQueryErrorCode.codeToSqlState(IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY), sqlEx.sqlState());
    }

    /**
     * Local result factory for test.
     */
    public static class TestH2LocalResultFactory extends H2LocalResultFactory {
        /** {@inheritDoc} */
        @Override public LocalResult create(Session ses, Expression[] expressions, int visibleColCnt, boolean system) {
            if (system)
                return new LocalResultImpl(ses, expressions, visibleColCnt);

            if (ses.memoryTracker() != null) {
                H2ManagedLocalResult res = new H2ManagedLocalResult(ses, expressions, visibleColCnt) {
                    @Override public void onClose() {
                        // Just prevent 'rows' from being nullified for test purposes.
                    }
                };

                localResults.add(res);

                return res;
            }

            return new H2ManagedLocalResult(ses, expressions, visibleColCnt);
        }

        /** {@inheritDoc} */
        @Override public LocalResult create() {
            throw new NotImplementedException();
        }
    }

    /**
     *
     */
    protected void clearResults() {
        for (LocalResult res : localResults)
            U.closeQuiet(res);

        localResults.clear();
    }
}