/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.mxbean;

import javax.management.ObjectName;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.exceptions.SqlMemoryQuotaExceededException;
import org.apache.ignite.internal.metric.UserQueriesTestBase;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Test;

/**
 * Tests SQL Free Memory works correctly after "global memory quota exceeded" event
 *
 */
public class SqlQueryMXBeanImplSqlFreeMemQuotaExceededTest extends UserQueriesTestBase {
    /**
     * Teardown.
     */
    @After
    public void stopAll() {
        stopAllGrids();
    }


    /**
     * Test that sql engine free memory jmx property reverts to the correct settings after a "global quota exceeded" event.
     *
     * To achive this, we start one server + one client. Queries are started from client, so map step is only on server,
     * and reduce phase is only on client.
     *
     * @throws Exception on fail.
     */
    @Test
    public void testFreeMemoryAfterQuotaExceeded() throws Exception {
        int strongMemQuota = 1024 * 1024;
        int memQuotaUnlimited = 0;

        startGridWithMaxMem(MAPPER_IDX, strongMemQuota);
        startGridWithMaxMem(REDUCER_IDX, memQuotaUnlimited, true);

        IgniteCache cache = createCacheFrom(grid(REDUCER_IDX));

        final String mapFailMsg = "SQL query ran out of memory: Global quota was exceeded.";

        // map phase failure affects only general fail metric, not OOM metric.
        assertMetricsIncrementedOnlyOnReducer(() -> GridTestUtils.assertThrows(
            log,
            () -> cache.query(new SqlFieldsQuery("SELECT * FROM TAB").setLazy(false)).getAll(),
            SqlMemoryQuotaExceededException.class,
            mapFailMsg),
            "failed");

        long freeMem = getValue("mxbean.SqlQueryMXBeanImplSqlFreeMemQuotaExceededTest" + MAPPER_IDX, "SQL Query", "SqlQueryMXBeanImpl", "SqlFreeMemoryBytes");
        long maxMem = getValue("mxbean.SqlQueryMXBeanImplSqlFreeMemQuotaExceededTest" + MAPPER_IDX, "SQL Query", "SqlQueryMXBeanImpl", "SqlGlobalMemoryQuotaBytes");

        if (freeMem < maxMem)
            fail(String.format("Expected no memory reserved: [freeMem=%d, maxMem=%d]", freeMem, maxMem));
    }

    /** Checks that a bean with the specified group and name is available and has the expected attribute */
    private long getValue(String gridName, String grp, String name, String attributeName) throws Exception {
        ObjectName mBeanName = IgniteUtils.makeMBeanName(gridName, grp, name);
        Object attributeVal = grid(gridName).configuration().getMBeanServer().getAttribute(mBeanName, attributeName);

        return (long) attributeVal;
    }
}
