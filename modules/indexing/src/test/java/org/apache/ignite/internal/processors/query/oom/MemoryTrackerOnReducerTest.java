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

import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.QueryMemoryManager;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class MemoryTrackerOnReducerTest extends GridCommonAbstractTest {
    /** Big table size. */
    private static final int BIG_TBL_SIZE = 20_000;

    /** */
    private static final AtomicLong maxReserved = new AtomicLong();

    /** */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(3);

        createSchema();

        populateData();

        GridTestUtils.setFieldValue(
            grid(0).context().query().getIndexing(),
            "memoryMgr",
            new QueryMemoryManager(grid(0).context()) {
                @Override public boolean reserve(long size) {
                    boolean res = super.reserve(size);

                    maxReserved.set(Math.max(maxReserved.get(), reserved()));

                    return res;
                }
            }
        );
    }

    /** */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        maxReserved.set(0);
    }

    /** */
    @Test
    public void plainQueryWithoutMemoryTracker() {
        Iterator<List<?>> it = query("select * from TEST", true).iterator();

        int cnt = 0;
        while (it.hasNext()) {
            it.next();
            cnt++;
        }

        assertEquals(0, maxReserved.get());

        assertEquals(BIG_TBL_SIZE, cnt);
    }

    /** */
    @Test
    public void reduceQueryAndTrackReducedBuffers() {
        Iterator<List<?>> it = query(
            "select * from TEST T0, " +
                "(SELECT DISTINCT id, name from TEST) T1 " +
                "WHERE T0.id = T1.id",
             true).iterator();

        QueryMemoryManager memMgr = ((IgniteH2Indexing)grid(0).context().query().getIndexing()).memoryManager();

        int cnt = 0;
        while (it.hasNext()) {
            it.next();
            cnt++;

            if (cnt > 1 && cnt < BIG_TBL_SIZE - 1)
                assertTrue(memMgr.reserved() > 0);
        }

        assertTrue(maxReserved.get() > 0);

        assertEquals(0L, memMgr.reserved());

        assertEquals(BIG_TBL_SIZE, cnt);
    }

    /** */
    private void populateData() {
        for (int i = 0; i < BIG_TBL_SIZE; ++i)
            execSql("insert into TEST VALUES (?, ?, ?)", i, i % 100, UUID.randomUUID().toString());
    }

    /** */
    private void createSchema() {
        execSql("create table TEST (id int primary key, ref_key int, name varchar)");
    }

    /**
     * @param sql SQL query
     * @param args Query parameters.
     */
    private void execSql(String sql, Object... args) {
        grid(0).context().query().querySqlFields(
            new SqlFieldsQuery(sql).setArgs(args), false).getAll();
    }

    /**
     * @param sql SQL query
     * @return Results set.
     */
    FieldsQueryCursor<List<?>> query(String sql, boolean lazy) {
        return grid(0).context().query().querySqlFields(
            new SqlFieldsQueryEx(sql, null)
                .setLazy(lazy)
                .setEnforceJoinOrder(true)
                .setPageSize(100), false);
    }
}
