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

import java.util.Comparator;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.h2.table.Column;
import org.h2.value.Value;

public class ColumnStatisticsCollector {
    private final Column col;

    private final NavigableMap<Value, Long> valFreq;

    private long nullsCnt;

    public ColumnStatisticsCollector(Column col, Comparator<Value> comp) {
        this.col = col;

        valFreq = new TreeMap<>(comp);
    }

    public void add(Value val) {
        if (val == null) {
            nullsCnt++;

            return;
        }

        valFreq.compute(val, (key, cnt) -> {
            if (cnt == null)
                return 1L;

            return cnt + 1;
        });
    }

    public ColumnStatistics finish(long totalRows) {
        return new ColumnStatistics(valFreq.firstKey(), valFreq.lastKey(), totalRows, nullsCnt, valFreq.size());
    }

    public Column col() {
        return col;
    }
}
