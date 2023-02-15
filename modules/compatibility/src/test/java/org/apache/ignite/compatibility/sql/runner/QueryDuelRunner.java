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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
     */
    QueryDuelResult run(QueryWithParams qry) {
        log.info("Started duel for query: " + qry);
        QueryExecutionResult baseRes, targetRes;
        if (qry.parametrized()) {
            baseRes = executeQueryPrepared(baseConn, qry);
            targetRes = executeQueryPrepared(targetConn, qry);
        }
        else {
            baseRes = executeQuerySimple(baseConn, qry);
            targetRes = executeQuerySimple(targetConn, qry);
        }

        assert baseRes.rsSize == targetRes.rsSize :
            "result mismatch [base=" + baseRes.rsSize + ", target=" + targetRes.rsSize + ", sql=" + qry + ']';

        return QueryDuelResult.builder(qry)
            .baseExecTime(baseRes.execTime())
            .baseErr(baseRes.error())
            .targetExecTime(targetRes.execTime())
            .targetErr(targetRes.error())
            .build();
    }

    /**
     * Executes given query on Ignite through given connection.
     *
     * @param conn Connection.
     * @param qry Query.
     */
    private QueryExecutionResult executeQuerySimple(Connection conn, QueryWithParams qry) {
        assert !qry.parametrized();

        int cnt = 0;
        long start = 0, end = 0;

        Exception ex = null;
        try (Statement stmt = conn.createStatement()) {
            start = System.nanoTime();

            try (ResultSet rs = stmt.executeQuery(qry.query())) {
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

        printResult(conn, res);

        return res;
    }

    /**
     * Executes given query on Ignite through given connection.
     *
     * @param conn Connection.
     * @param qry Query.
     */
    private QueryExecutionResult executeQueryPrepared(Connection conn, QueryWithParams qry) {
        assert qry.parametrized();

        int cnt = 0;
        long start = 0, end = 0;

        Exception ex = null;
        try (PreparedStatement stmt = conn.prepareStatement(qry.query())) {
            int i = 1;
            for (Object param : qry.params())
                stmt.setObject(i++, param);

            start = System.nanoTime();

            try (ResultSet rs = stmt.executeQuery()) {
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

        printResult(conn, res);

        return res;
    }

    /**
     * @param conn Connection.
     * @param res Response.
     */
    private void printResult(Connection conn, QueryExecutionResult res) {
        log.info("\t" + (conn == baseConn ? "Base" : "Target") + " execution result: " + res.resultString());
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
