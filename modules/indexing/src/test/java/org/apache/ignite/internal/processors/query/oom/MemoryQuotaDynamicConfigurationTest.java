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

/**
 * Test cases for dynamic query quota.
 */
public class MemoryQuotaDynamicConfigurationTest extends AbstractQueryMemoryTrackerSelfTest {
    /** */
    private static final long GLOBAL_QUOTA = 50L * KB;

    /** {@inheritDoc} */
    @Override protected boolean isLocal() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected long globalQuotaSize() {
        return GLOBAL_QUOTA;
    }

    /** {@inheritDoc} */
    @Override protected boolean startClient() {
        return true;
    }

    /**  */
    @Test
    public void testGlobalQuota() throws Exception {
        maxMem = 0; // Disable implicit query quota.

        setGlobalQuota(GLOBAL_QUOTA);
        checkQueryExpectOOM("select * from K ORDER BY K.indexed", false);
        assertEquals(1, localResults.size());
        assertTrue(localResults.get(0).memoryReserved() < GLOBAL_QUOTA);

        clearResults();

        setGlobalQuota(0);
        execQuery("select * from K ORDER BY K.indexed", false);
        assertEquals(2, localResults.size());
        assertTrue(localResults.get(0).memoryReserved() > GLOBAL_QUOTA);

        clearResults();

        setGlobalQuota(GLOBAL_QUOTA);
        checkQueryExpectOOM("select * from K ORDER BY K.indexed", false);
        assertEquals(1, localResults.size());
        assertTrue(localResults.get(0).memoryReserved() < GLOBAL_QUOTA);
    }

    /**  */
    @Test
    public void testDefaultQueryQuota() throws Exception {
        maxMem = 0; // Disable implicit query quota.
        setGlobalQuota(0);

        // All quotas turned off, nothing should happen.
        execQuery("select * from K ORDER BY K.indexed", false);
        assertEquals(2, localResults.size());
        assertTrue(localResults.get(0).memoryReserved() > GLOBAL_QUOTA);

        clearResults();

        // Default query quota is set to 100, we expect exception.
        setDefaultQueryQuota(100);
        checkQueryExpectOOM("select * from K ORDER BY K.indexed", false);
        assertEquals(1, localResults.size());

        clearResults();

        // Turn on offloading, expect no error.
        setOffloadingEnabled(true);
        execQuery("select * from K ORDER BY K.indexed", false);
        assertEquals(2, localResults.size());
        assertTrue(localResults.get(0).memoryReserved() < 100);

        clearResults();

        // Turn off offloading, expect error.
        setOffloadingEnabled(false);
        checkQueryExpectOOM("select * from K ORDER BY K.indexed", false);
        assertEquals(1, localResults.size());

        clearResults();

        // Turn off quota, expect no error.
        setDefaultQueryQuota(0);
        execQuery("select * from K ORDER BY K.indexed", false);
        assertEquals(2, localResults.size());
        assertTrue(localResults.get(0).memoryReserved() > GLOBAL_QUOTA);
    }

    /** */
    private void setGlobalQuota(long newQuota) {
        for (Ignite node : G.allGrids()) {
            QueryMemoryManager memMgr = memoryManager((IgniteEx)node);

            memMgr.setGlobalQuota(String.valueOf(newQuota));
        }
    }

    /** */
    private void setDefaultQueryQuota(long newQuota) {
        for (Ignite node : G.allGrids()) {
            QueryMemoryManager memMgr = memoryManager((IgniteEx)node);

            memMgr.setQueryQuota(String.valueOf(newQuota));
        }
    }

    /** */
    private void setOffloadingEnabled(boolean enabled) {
        for (Ignite node : G.allGrids()) {
            QueryMemoryManager memMgr = memoryManager((IgniteEx)node);

            memMgr.setOffloadingEnabled(enabled);
        }
    }

    /** */
    private static QueryMemoryManager memoryManager(IgniteEx node) {
        return ((IgniteH2Indexing)node.context()
                    .query()
                    .getIndexing())
                    .memoryManager();
    }

    /** {@inheritDoc} */
    @Override protected void checkQueryExpectOOM(String sql, boolean lazy) {
        IgniteSQLException sqlEx = (IgniteSQLException)GridTestUtils.assertThrowsAnyCause(log, () -> {
            execQuery(sql, lazy);

            return null;
        }, IgniteSQLException.class, "SQL query run out of memory: ");

        assertNotNull("SQL exception missed.", sqlEx);
        assertEquals(IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY, sqlEx.statusCode());
        assertEquals(IgniteQueryErrorCode.codeToSqlState(IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY), sqlEx.sqlState());
    }
}
