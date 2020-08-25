/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.compatibility.sql.runner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * Class that runs queries in different Ignite version and compares their execution times.
 */
class QueryDuelRunner {
    /** */
    private final IgniteLogger log;

    /** JDBC connection pool of the old Ignite version. */
    private final Connection baseConn;

    /** JDBC connection pool of the new Ignite version. */
    private final Connection targetConn;

    /**
     * @param log Logger.
     * @param baseConn Connection to the base version.
     * @param targetConn Connection to the target version.
     */
    QueryDuelRunner(
        IgniteLogger log,
        Connection baseConn,
        Connection targetConn
    ) {
        this.log = log;
        this.baseConn = baseConn;
        this.targetConn = targetConn;
    }


    /**
     * @param qry Query.
     * @param successCnt Success count.
     * @param attemptsCnt Attempts count.
     */
    QueryDuelResult run(String qry, int successCnt, int attemptsCnt) {
        List<Long> baseExecTimes = new ArrayList<>(attemptsCnt);
        List<Long> targetExecTimes = new ArrayList<>(attemptsCnt);

        log.info("Started duel for qry=" + qry);

        while (attemptsCnt-- > 0) {
            QueryExecutionResult baseRes = executeQuery(baseConn, qry);
            QueryExecutionResult targetRes = executeQuery(targetConn, qry);

            if (!baseRes.success() || !targetRes.success())
                return QueryDuelResult.failedResult(qry, baseRes.error(), targetRes.error());

            baseExecTimes.add(baseRes.execTime());
            targetExecTimes.add(targetRes.execTime());

            if (isSuccessfulRun(baseRes.execTime(), targetRes.execTime()))
                successCnt--;

            if (successCnt == 0)
                break;
        }

        return successCnt > 0
            ? QueryDuelResult.verificationFailedResult(qry, baseExecTimes, targetExecTimes)
            : QueryDuelResult.successfulResult(qry, baseExecTimes, targetExecTimes);
    }

    /**
     * Executes given query on Ignite through given connection.
     *
     * @param conn Connection.
     * @param qry Query.
     */
    private QueryExecutionResult executeQuery(Connection conn, String qry) {
        int cnt = 0;
        long start = 0, end = 0;

        Exception ex = null;
        try (Statement stmt = conn.createStatement()) {
            start = System.nanoTime();

            try (ResultSet rs = stmt.executeQuery(qry)) {
                while (rs.next()) {
                    // Just read the full result set.
                    cnt++;
                }
            }
            end = System.nanoTime();
        }
        catch (SQLException e) {
            ex = e;
        }

        QueryExecutionResult res = ex == null
            ? new QueryExecutionResult(cnt, end - start)
            : new QueryExecutionResult(ex);

        log.info((conn == baseConn ? "Base" : "Target") + " execution result: qry=" + qry + ", " + res.resultString());

        return res;
    }

    /**
     * @param baseRes Query execution time in the base engine.
     * @param targetRes Query execution time in the target engine.
     * @return {@code true} if a query execution time in the target engine is not much longer than in the base one.
     */
    private boolean isSuccessfulRun(long baseRes, long targetRes) {
        final double epsilon = 2.0 * 1_000_000; // Let's say 2 ms is about statistical error.

        if (baseRes < targetRes && (baseRes > epsilon || targetRes > epsilon)) {
            double target = Math.max(targetRes, epsilon);
            double base = Math.max(baseRes, epsilon);

            return target / base <= 2;
        }

        return true;
    }

    /**
     * Result of the particular execution.
     */
    private static class QueryExecutionResult {
        /** */
        private final Exception err;

        /** */
        private final long rsSize;

        /** */
        private final long execTime;

        /**
         * Constructor for unsuccessful result.
         *
         * @param err Error.
         */
        public QueryExecutionResult(Exception err) {
            this.err = err;

            rsSize = -1;
            execTime = -1;
        }

        /**
         * Constructor for successful result.
         *
         * @param rsSize Result set size.
         * @param execTime Execution time.
         */
        public QueryExecutionResult(long rsSize, long execTime) {
            assert rsSize >= 0;
            assert execTime >= 0;

            this.rsSize = rsSize;
            this.execTime = execTime;

            err = null;
        }

        /**
         * Returns error of unsuccessful execution or {@code null}
         * if execution was successful.
         *
         * @return Error of execution.
         */
        public @Nullable Exception error() {
            return err;
        }

        /**
         * Returns the size of the result set if execution was successful, {@code -1} otherwise.
         *
         * @return Size of the result set.
         */
        public long rsSize() {
            return rsSize;
        }

        /**
         * Returns the time if execution was successful, {@code -1} otherwise.
         *
         * @return Time of the execution.
         */
        public long execTime() {
            return execTime;
        }

        /**
         * Returns {@code true} if execution was successful, {@code false} otherwise.
         *
         * @return {@code true} if execution was successful, {@code false} otherwise.
         */
        public boolean success() {
            return err == null;
        }

        /**
         * Returns string representation of current result.
         *
         * @return String representing current result.
         */
        String resultString() {
            long execTime0 = execTime / 1000;

            return "rsSize=" + rsSize +
                ", execTime=" + (1.0 * execTime0 / 1000) + "ms" +
                ", err=" + err;
        }
    }
}
