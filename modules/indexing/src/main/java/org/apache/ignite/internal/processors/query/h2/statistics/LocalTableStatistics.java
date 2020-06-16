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

package org.apache.ignite.internal.processors.query.h2.statistics;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.h2.value.Value;

public class LocalTableStatistics {
    private final Map<Integer, TablePartitionStatistics> partStats;

    private volatile Map<String, ColumnStatistics> colNameToStat;

    public LocalTableStatistics(Collection<TablePartitionStatistics> stats) {
        assert stats != null: "stats";

        partStats = new HashMap<>(stats.size());

        for (TablePartitionStatistics part : stats)
            partStats.put(part.partId(), part);
    }

    public ColumnStatistics columnStatistics(String name) {
        return colNameToStat.get(name);
    }

    public long rowCount() {
        long rowCnt = 0;

        for (TablePartitionStatistics partStat : partStats.values())
            rowCnt += partStat.rowCount();

        return rowCnt;
    }

    private void aggregateStatistics(Collection<TablePartitionStatistics> stats) {
        for (TablePartitionStatistics part : stats)
            partStats.put(part.partId(), part);
    }

    private static class ColumnStatisticsAggregator {
        private Value min;

        private Value max;

        private int nulls;

        private int selectivity;
    }
}
