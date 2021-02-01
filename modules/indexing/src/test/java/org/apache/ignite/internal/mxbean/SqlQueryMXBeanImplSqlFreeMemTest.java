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

import java.util.concurrent.TimeUnit;
import javax.management.ObjectName;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.metric.SqlStatisticsMemoryQuotaTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.After;
import org.junit.Test;

/**
 * Tests SQL Free Memory Bytes JMX property for correctness in various usage scenarios.
 */
public class SqlQueryMXBeanImplSqlFreeMemTest extends SqlStatisticsMemoryQuotaTest {

    /**
     * Teardown.
     */
    @After
    public void stopAll() {
        stopAllGrids();
    }

    /**
     * {@inheritDoc}
     *
     * set the appropriate SQL Config attrs to test for.
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSqlConfiguration(new SqlConfiguration().setSqlGlobalMemoryQuota("20m").setSqlQueryMemoryQuota("10m").
        setLongQueryWarningTimeout(30 * 1000L).setSqlOffloadingEnabled(true));

        return cfg;
    }

    /* Checks that a bean with the specified group and name is available and has the expected attribute */
    private long getValue(String gridName, String grp, String name, String attributeName) throws Exception {
        ObjectName mBeanName = IgniteUtils.makeMBeanName(gridName, grp, name);
        Object attributeVal = grid(gridName).configuration().getMBeanServer().getAttribute(mBeanName, attributeName);

        return (long) attributeVal;
    }

    /**
     * test that the FreeMemory JMX property shows correct results when queries are being run.
     * @throws Exception
     */
    @Test
    public void testFreeMemoryJMXWhenLocalQueryIsRunningAndReleasedOnFinish() throws Exception {
        startGrids(2);

        int connNodeIdx = 1;
        int otherNodeIdx = 0;

        IgniteCache cache = createCacheFrom(grid(connNodeIdx));

        final String scanQry = "SELECT * FROM TAB WHERE ID <> suspendHook(5)";

        IgniteInternalFuture locQryIsDone =
            runAsyncX(() -> cache.query(new SqlFieldsQuery(scanQry).setLocal(true).setLazy(false)).getAll());

        SuspendQuerySqlFunctions.awaitQueryStopsInTheMiddle();

        validateMemoryUsageOn(connNodeIdx, MEMORY_IS_USED);
        validateMemoryUsageOn(otherNodeIdx, MEMORY_IS_FREE);

        SuspendQuerySqlFunctions.resumeQueryExecution();

        locQryIsDone.get(WAIT_OP_TIMEOUT_SEC, TimeUnit.SECONDS);

        validateMemoryUsageOn(connNodeIdx, MEMORY_IS_FREE);
        validateMemoryUsageOn(otherNodeIdx, MEMORY_IS_FREE);
    }

    /**
     * Validate memory metrics freeMem and maxMem on the specified node.
     *
     * @param nodeIdx index of the node which metrics to validate.
     * @param validator function(freeMem, maxMem) that validates these values.
     */
    public void validateMemoryUsageOn(int nodeIdx, MemValidator validator) throws Exception {
        long free = getValue("mxbean.SqlQueryMXBeanImplSqlFreeMemTest" + nodeIdx, "SQL Query", "SqlQueryMXBeanImpl", "SqlFreeMemoryBytes");
        long maxMem = getValue("mxbean.SqlQueryMXBeanImplSqlFreeMemTest" + nodeIdx, "SQL Query", "SqlQueryMXBeanImpl", "SqlGlobalMemoryQuotaBytes");

        if (free > maxMem)
            fail(String.format("Illegal state: there's more free memory (%s) than " +
                "maximum available for sql (%s) on the node %d", free, maxMem, nodeIdx));

        validator.validate(free, maxMem);
    }
}
