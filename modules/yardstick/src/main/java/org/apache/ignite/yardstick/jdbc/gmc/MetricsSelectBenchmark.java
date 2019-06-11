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

import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Native sql benchmark that performs insert operations.
 */
public class MetricsSelectBenchmark extends AbstractMetricsBenchmark {
    /**
     * Benchmarked action that inserts and immediately deletes row.
     *
     * {@inheritDoc}
     */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long clusterId = random.nextLong(1,4);
        long metricId = random.nextLong(1, 51);

        // Select metrics by random cluster_id and for one day
        SqlFieldsQuery select = new SqlFieldsQuery("SELECT * FROM ts_metrics WHERE metric_id = ? AND cluster_id = ? AND ts >= ? AND ts <= ?");
        select.setArgs(metricId, clusterId, 0, 86_400_000);

        GridQueryProcessor qryProc = ((IgniteEx)ignite()).context().query();

        try (FieldsQueryCursor<List<?>> insCur = qryProc.querySqlFields(select, false)) {
            for (List<?> list : insCur) {
                // No-op, there is no result
            }
        }
        catch (Exception ign) {
            // collision occurred, ignoring
        }

        return true;
    }
}
