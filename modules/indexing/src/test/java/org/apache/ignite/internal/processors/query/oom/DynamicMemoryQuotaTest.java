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

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.QueryMemoryManager;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.util.IgniteUtils.KB;
import static org.apache.ignite.internal.util.IgniteUtils.MB;

/**
 * Test cases for dynamic query quota. TODO rework to
 */
public class DynamicMemoryQuotaTest extends AbstractQueryMemoryTrackerSelfTest {
    private static long GLOBAL_QUOTA = 50L * KB;
    private static long QUERY_QUOTA = 100L * MB;

    /** {@inheritDoc} */
    @Override protected boolean isLocal() {
        return false;
    }

    /** {@inheritDoc} */
    protected long globalQuotaSize() {
        return GLOBAL_QUOTA;
    }

    /** {@inheritDoc} */
    protected boolean startClient() {
        return true;
    }

    /** Check query failure with ORDER BY indexed col. */
    @Test
    public void testQueryWithSortByIndexedCol() throws Exception {
        maxMem = QUERY_QUOTA;
        checkQueryExpectOOM("select * from K ORDER BY K.indexed", false);

        assertEquals(1, localResults.size());
        assertTrue(localResults.get(0).memoryReserved() < GLOBAL_QUOTA);

        localResults.clear();

        setGlobalQuota(0);

        execQuery("select * from K ORDER BY K.indexed", false);

        assertEquals(2, localResults.size());
        assertTrue(localResults.get(0).memoryReserved() > GLOBAL_QUOTA);
        assertTrue(localResults.get(1).memoryReserved() > GLOBAL_QUOTA);
        localResults.clear();

        setGlobalQuota(GLOBAL_QUOTA);
        checkQueryExpectOOM("select * from K ORDER BY K.indexed", false);
        assertEquals(1, localResults.size());
        assertTrue(localResults.get(0).memoryReserved() < GLOBAL_QUOTA);
    }

    /** */
    public void setGlobalQuota(long newQuota) {
        for (Ignite node : G.allGrids()) {
            QueryMemoryManager memMgr = ((IgniteH2Indexing)((IgniteEx)node).context()
                .query()
                .getIndexing())
                .memoryManager();

            memMgr.setGlobalQuota(String.valueOf(newQuota));
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkQueryExpectOOM(String sql, boolean lazy) {
        IgniteSQLException sqlEx = (IgniteSQLException)GridTestUtils.assertThrowsAnyCause(log, () -> {
            execQuery(sql, lazy);

            return null;
        }, IgniteSQLException.class, "SQL query run out of memory: Global quota exceeded.");

        assertNotNull("SQL exception missed.", sqlEx);
        assertEquals(IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY, sqlEx.statusCode());
        assertEquals(IgniteQueryErrorCode.codeToSqlState(IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY), sqlEx.sqlState());
    }
}
