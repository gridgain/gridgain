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
public class MetricsInsertDeleteBenchmark extends AbstractMetricsBenchmark {
    /**
     * Benchmarked action that inserts and immediately deletes row.
     *
     * {@inheritDoc}
     */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long id = random.nextLong(args.range()) + 1 + args.range();
        long oldId = random.nextLong();

        long clusterId = random.nextLong(1,4);
        long metricId = random.nextLong(1, 51);
        long ts = System.currentTimeMillis();
        long longMetricValue = random.nextLong();

        SqlFieldsQuery insert = new SqlFieldsQuery("INSERT INTO ts_metrics (id, ts, cluster_id, metric_id, metric_value_long) VALUES(?, ?, ?, ?, ?)");
        insert.setArgs(id, ts, clusterId, metricId, longMetricValue);

        SqlFieldsQuery delete = new SqlFieldsQuery("DELETE FROM ts_metrics WHERE id = ?");
        delete.setArgs(oldId);

        GridQueryProcessor qryProc = ((IgniteEx)ignite()).context().query();

        try (FieldsQueryCursor<List<?>> insCur = qryProc.querySqlFields(insert, false);
             FieldsQueryCursor<List<?>> delCur = qryProc.querySqlFields(delete, false)) {
            // No-op, there is no result
        }
        catch (Exception ign) {
            // collision occurred, ignoring
        }

        return true;
    }
}
