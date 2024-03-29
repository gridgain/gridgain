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

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Abstract test for sql metrics tests.
 */
public class SqlStatisticsAbstractTest extends GridCommonAbstractTest {
    /**
     * Timeout for each wait on sync operation in seconds.
     */
    public static final long WAIT_OP_TIMEOUT_SEC = 15;

    /**
     * Number of rows in the test table.
     */
    public static final int TABLE_SIZE = 10_000;

    /**
     * Starts server node with max memory quota.
     *
     * @param nodeIdx test framework index to start node with.
     * @param maxMem value of default global quota to set on node start; -1 forunlimited.
     * @throws Exception on fail.
     */
    protected void startGridWithMaxMem(int nodeIdx, long maxMem) throws Exception {
        startGridWithMaxMem(nodeIdx, maxMem, false);
    }

    /**
     * Starts grid with specified global (max memory quota) value.
     *
     * @param nodeIdx test framework index to start node with.
     * @param maxMem value of default global quota to set on node start; -1 forunlimited.
     * @param client if we need to start client node.
     */
    protected void startGridWithMaxMem(int nodeIdx, long maxMem, boolean client) throws Exception {
        String name = getTestIgniteInstanceName(nodeIdx);

        startGrid(name, getConfiguration(name)
            .setClientMode(client)
            .setSqlConfiguration(new SqlConfiguration()
                .setSqlGlobalMemoryQuota(Long.toString(maxMem)))
        );
    }

    /**
     * Start the cache with a test table and test data.
     */
    protected IgniteCache createCacheFrom(Ignite node) {
        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<Integer, String>(DEFAULT_CACHE_NAME)
            .setSqlFunctionClasses(SuspendQuerySqlFunctions.class)
            .setQueryEntities(Collections.singleton(
                new QueryEntity(Integer.class.getName(), String.class.getName())
                    .setTableName("TAB")
                    .addQueryField("id", Integer.class.getName(), null)
                    .addQueryField("name", String.class.getName(), null)
                    .setKeyFieldName("id")
                    .setValueFieldName("name")
            ));

        IgniteCache<Integer, String> cache = node.createCache(ccfg);

        try (IgniteDataStreamer<Object, Object> ds = node.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < TABLE_SIZE; i++)
                ds.addData(i, UUID.randomUUID().toString());
        }

        return cache;
    }

    /**
     * Run async action and log if exception occured.
     *
     * @param act action to perform on other thread.
     * @return future object to "action complited" event.
     */
    protected IgniteInternalFuture runAsyncX(Runnable act) {
        return GridTestUtils.runAsync(() -> {
            try {
                act.run();
            }
            catch (Throwable th) {
                log.error("Failed to perform async action. Probably test is broken.", th);
            }
        });
    }

    /**
     * This class exports function to the sql engine. Function implementation allows us to suspend query execution on test
     * logic condition.
     */
    public static class SuspendQuerySqlFunctions {
        /**
         * How many rows should be processed (by all nodes in total)
         */
        private static final int DFLT_PROCESS_ROWS_TO_SUSPEND = TABLE_SIZE / 4;

        /**
         * Latch to await till full scan query that uses this class function have done some job, so some memory is
         * reserved.
         */
        public static volatile CountDownLatch qryIsInTheMiddle;

        /**
         * This latch is released when query should continue it's execution after stop in the middle.
         */
        private static volatile CountDownLatch resumeQryExec;

        static {
            refresh();
        }

        /**
         * Refresh syncs.
         */
        public static void refresh() {
            if (qryIsInTheMiddle != null) {
                for (int i = 0; i < qryIsInTheMiddle.getCount(); i++)
                    qryIsInTheMiddle.countDown();
            }

            if (resumeQryExec != null)
                resumeQryExec.countDown();

            qryIsInTheMiddle = new CountDownLatch(DFLT_PROCESS_ROWS_TO_SUSPEND);

            resumeQryExec = new CountDownLatch(1);
        }

        /**
         * See {@link #qryIsInTheMiddle}.
         */
        public static void awaitQueryStopsInTheMiddle() throws InterruptedException {
            boolean reached = qryIsInTheMiddle.await(WAIT_OP_TIMEOUT_SEC, TimeUnit.SECONDS);

            if (!reached)
                throw new IllegalStateException("Unable to wait when query starts. Test is broken.");
        }

        /**
         * See {@link #resumeQryExec}.
         */
        public static void resumeQueryExecution() {
            resumeQryExec.countDown();
        }

        /**
         * Override process rows threshhold: after that number of rows are processed, query is suspended.
         */
        public static void setProcessRowsToSuspend(int rows) {
            qryIsInTheMiddle = new CountDownLatch(rows);
        }

        /**
         * Sql function used to suspend query when quarter of the table is processed. Should be used in full scan queries.
         *
         * @param ret number to return.
         */
        @QuerySqlFunction
        public static long suspendHook(long ret) throws InterruptedException {
            qryIsInTheMiddle.countDown();

            if (qryIsInTheMiddle.getCount() == 0) {
                boolean reached = resumeQryExec.await(WAIT_OP_TIMEOUT_SEC, TimeUnit.SECONDS);

                if (!reached) {
                    IllegalStateException exc =
                        new IllegalStateException("Unable to wait when to continue the query. Test is broken.");

                    // In some error cases exceptions from sql functions are ignored. Write it in the log.
                    log.error("Test exception.", exc);

                    throw exc;
                }
            }

            return ret;
        }

        /**
         * Function to fail the query.
         */
        @QuerySqlFunction
        public static long failFunction() {
            throw new RuntimeException("Fail the query.");
        }

        /**
         * Function to fail the query.
         *
         * @param dummy ignored parameter, required only for correct sql function signature.
         */
        @QuerySqlFunction
        public static long failFunction(long dummy) {
            throw new RuntimeException("Fail the query.");
        }
    }

    /**
     * Functional interface to validate memory metrics values.
     */
    protected static interface MemValidator {
        /**
         *
         * @param free freeMem metric value.
         * @param max maxMem metric value.
         */
        void validate(long free, long max);
    }

    /**
     * This callback validates that some memory is reserved.
     */
    protected static final MemValidator MEMORY_IS_USED = (freeMem, maxMem) -> {
        if (freeMem == maxMem)
            fail("Expected some memory reserved.");
    };

    /**
     * This callback validates that no "sql" memory is reserved.
     */
    protected static final MemValidator MEMORY_IS_FREE = (freeMem, maxMem) -> {
        if (freeMem < maxMem)
            fail(String.format("Expected no memory reserved: [freeMem=%d, maxMem=%d]", freeMem, maxMem));
    };

    /**
     * Checks that a bean with the specified group and name is available and has the expected attribute
     */
    protected long getValue(String gridName, String grp, String name, String attributeName) throws Exception {
        ObjectName mBeanName = IgniteUtils.makeMBeanName(gridName, grp, name);
        Object attributeVal = grid(gridName).configuration().getMBeanServer().getAttribute(mBeanName, attributeName);

        return (long) attributeVal;
    }
}
