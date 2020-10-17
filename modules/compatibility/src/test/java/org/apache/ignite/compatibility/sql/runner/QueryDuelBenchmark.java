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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Benchmark runner.
 */
public class QueryDuelBenchmark {
    /** Logger. */
    private final IgniteLogger log;

    /** Connection to the base Ignite. */
    private final Connection baseConn;

    /** Connection to the target Ignite. */
    private final Connection targetConn;

    /**
     * @param log Logger.
     * @param baseConn Connection to the base Ignite.
     * @param targetConn Connection to the target Ignite.
     */
    public QueryDuelBenchmark(IgniteLogger log, Connection baseConn, Connection targetConn) {
        A.notNull(log, "log", baseConn, "baseConn", targetConn, "targetConn");

        this.log = log;
        this.baseConn = baseConn;
        this.targetConn = targetConn;
    }

    /**
     * Starts task that compare query execution time in the new and old Ignite versions.
     *
     * @param timeout Test duration.
     * @param qrySupplier Sql queries generator.
     * @return Suspicious queries collection.
     */
    public List<QueryDuelResult> runBenchmark(
        final long timeout,
        Supplier<QueryWithParams> qrySupplier
    ) {
        final long end = System.currentTimeMillis() + timeout;

        QueryDuelRunner runner = new QueryDuelRunner(log, baseConn, targetConn);

        List<QueryDuelResult> res = new ArrayList<>();
        while (System.currentTimeMillis() < end) {
            QueryWithParams qry = qrySupplier.get();

            if (qry == null)
                break; // seems like supplier won't supply new queries anymore

            res.add(runner.run(qry));
        }

        return res;
    }
}
