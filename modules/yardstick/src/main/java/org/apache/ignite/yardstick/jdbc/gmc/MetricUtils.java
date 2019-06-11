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

package org.apache.ignite.yardstick.jdbc.gmc;

import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.yardstickframework.BenchmarkConfiguration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Useful methods for JDBC benchmarks.
 */
public class MetricUtils {
    /**
     * Common method to fill test stand with data.
     *
     * @param cfg Benchmark configuration.
     * @param ignite Ignite node.
     * @param range Data key range.
     */
    public static void fillData(BenchmarkConfiguration cfg, IgniteEx ignite, long range,
        CacheAtomicityMode atomicMode) {
        IgniteSemaphore sem = ignite.semaphore("sql-setup", 1, true, true);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        Map<String, Long> timestamps = new ConcurrentHashMap<>();


        try {
            if (sem.tryAcquire()) {
                println(cfg, "Create table...");

                StringBuilder qry = new StringBuilder(
                    "CREATE TABLE IF NOT EXISTS ts_metrics ("
                        + " id long PRIMARY KEY, "
                        + " ts long, "
                        + " cluster_id long, "
                        + " metric_id long, "
                        + " metric_value_long long, "
                        + " metric_value_double double "
                        + ") WITH \"wrap_value=true"
                );

                if (atomicMode != null)
                    qry.append(", atomicity=").append(atomicMode.name());

                qry.append("\";");

                String qryStr = qry.toString();

                println(cfg, "Creating table with schema: " + qryStr);

                GridQueryProcessor qProc = ignite.context().query();

                qProc.querySqlFields(new SqlFieldsQuery(qryStr), true);

                println(cfg, "Populate data...");

                for (long l = 1; l <= range; ++l) {
                    long clusterId = random.nextLong(1,4);
                    long metricId = random.nextLong(1, 51);
                    long longMetricValue = random.nextLong();
                    double doubleMetricValue = random.nextDouble();

                    String compoundId = clusterId + "_" + metricId;
                    long ts = timestamps.getOrDefault(compoundId, 0L) + 5000;

                    if (random.nextBoolean()) {
                        qProc.querySqlFields(
                                new SqlFieldsQuery(
                                    "INSERT INTO ts_metrics (id, ts, cluster_id, metric_id, metric_value_long) VALUES(?, ?, ?, ?, ?)"
                                ).setArgs(l, ts, clusterId, metricId, longMetricValue), true
                        );
                    } else {
                        qProc.querySqlFields(
                                new SqlFieldsQuery(
                                        "INSERT INTO ts_metrics (id, ts, cluster_id, metric_id, metric_value_double) VALUES(?, ?, ?, ?, ?)"
                                ).setArgs(l, ts, clusterId, metricId, doubleMetricValue), true
                        );
                    }

                    timestamps.put(compoundId, ts);

                    if (l % 10000 == 0)
                        println(cfg, "Populate " + l);
                }

                qProc.querySqlFields(new SqlFieldsQuery("CREATE INDEX metrics_idx ON ts_metrics (metric_id,cluster_id,ts)"), true);

                println(cfg, "Finished populating data");
            }
            else {
                // Acquire (wait setup by other client) and immediately release/
                println(cfg, "Waits for setup...");

                sem.acquire();
            }
        }
        finally {
            sem.release();
        }
    }
}
