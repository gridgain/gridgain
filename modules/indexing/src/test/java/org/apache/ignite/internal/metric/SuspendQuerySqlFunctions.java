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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;

/**
 * This class exports function to the sql engine. Function implementation allows us to suspend query execution on test
 * logic condition.
 */
public class SuspendQuerySqlFunctions {
    /**
     * Timeout for each wait on sync operation in seconds.
     */
    public static final long WAIT_OP_TIMEOUT_SEC = 15;

    /**
     * Number of rows in the test table.
     */
    public static final int TABLE_SIZE = 10_000;

    /**
     * How many rows should be processed (by all nodes in total)
     */
    private static final int PROCESS_ROWS_TO_SUSPEND = TABLE_SIZE / 2;

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

        qryIsInTheMiddle = new CountDownLatch(PROCESS_ROWS_TO_SUSPEND);

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
     * Sql function used to suspend query when half of the table is processed. Should be used in full scan queries.
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

                // In some error cases exceptions from sql functions are ignored. Duplicate it in the console.
                exc.printStackTrace();

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
}