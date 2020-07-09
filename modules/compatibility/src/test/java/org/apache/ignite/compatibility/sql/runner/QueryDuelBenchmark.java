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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.F;
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
    public Collection<QueryDuelResult> runBenchmark(
        final long timeout,
        Supplier<String> qrySupplier,
        int successCnt,
        int attemptsCnt
    ) {
        A.ensure(successCnt >= 0, "successCnt >= 0");
        A.ensure(attemptsCnt > 0, "attemptsCnt > 0");
        A.ensure(attemptsCnt >= successCnt, "attemptsCnt >= successCnt");

        final long end = System.currentTimeMillis() + timeout;

        QueryDuelRunner runner = new QueryDuelRunner(log, baseConn, targetConn);

        Set<QueryDuelResult> res = new HashSet<>();
        while (System.currentTimeMillis() < end) {
            String qry = qrySupplier.get();

            if (F.isEmpty(qry))
                break; // seems like supplier won't supply new queries anymore

            QueryDuelResult duelRes = runner.run(qry, successCnt, attemptsCnt);

            if (!duelRes.successful())
                res.add(duelRes);
        }

        return res;
    }
}
