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

package org.apache.ignite.internal.processors.query.h2.opt.statistics;

import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import net.agkn.hll.HLL;
import org.h2.table.Column;
import org.h2.value.Value;

/**
 * Collector to compute statistic by single column.
 */
public class ColumnStatisticsCollector {
    /** Seed to proper work of hash function. */
    private static final int seed = 123456;

    /** */
    private final Column col;

    /** Value hasher. */
    private final Hasher hasher;

    /** Hyper Log Log structure */
    private final HLL hll = new HLL(13/*log2m*/, 5/*registerWidth*/);

    /** Minimum value. */
    private Value min = null;

    /** Maximum value. */
    private Value max = null;

    //private final NavigableMap<Value, Long> valFreq;

    /** Column value comparator. */
    private final Comparator<Value> comp;

    /** */
    private long nullsCnt;


    public ColumnStatisticsCollector(Column col, Comparator<Value> comp) {
        this.col = col;
        this.comp = comp;
        //valFreq = new TreeMap<>(comp);

        HashFunction hash = Hashing.murmur3_128(seed);
        hasher = hash.newHasher();
    }

    public void add(Value val) {
        if (val == null) {
            nullsCnt++;

            return;
        }

        hll.addRaw(hasher.hash().asLong());

        if (null == min || comp.compare(val, min) < 0)
            min = val;

        if (null == max || comp.compare(val, max) > 0)
            max = val;
    }

    public ColumnStatistics finish(long totalRows) {
        int nulls = 0;
        Value min = null;
        Value max = null;

        nulls = nullsPercent(nullsCnt, totalRows);

        int selectivity = 0;

        selectivity = cardinalityPercent(nullsCnt, totalRows, hll.cardinality());

        return new ColumnStatistics(min, max, nulls, selectivity, hll.toBytes());
    }

    private static int nullsPercent(long nullsCnt, long totalRows) {
        if (totalRows > 0)
            return (int)(100 * nullsCnt / totalRows);
        return 0;
    }

    private static int cardinalityPercent(long nullsCnt, long totalRows, long cardinality) {
        if (totalRows - nullsCnt > 0)
            return (int)(100 * cardinality / (totalRows - nullsCnt));
        return 0;
    }

    public Column col() {
        return col;
    }

    /**
     * Aggregate specified (partition or local) column statistics into (local or global) single one.
     *
     * @param comp
     * @param totalRows
     * @param partStats
     * @return
     */
    public static ColumnStatistics aggregate(Comparator<Value> comp, long totalRows, List<ColumnStatistics> partStats) {

        HashFunction hash = Hashing.murmur3_128(seed);
        Hasher hasher = hash.newHasher();

        HLL hll = new HLL(13/*log2m*/, 5/*registerWidth*/);

        Value min = null;
        Value max = null;
        long nullsCnt = 0;

        for(ColumnStatistics partStat : partStats) {
            HLL partHll = HLL.fromBytes(partStat.raw());
            hll.union(partHll);

            nullsCnt += partStat.nulls();

            if (min == null || (partStat.min() != null && comp.compare(partStat.min(), min) < 0))
                min = partStat.min();

            if (max == null || (partStat.max() != null && comp.compare(partStat.max(), max) > 0))
                max = partStat.max();
        }

        return new ColumnStatistics(min, max, nullsPercent(nullsCnt, totalRows),
                cardinalityPercent(nullsCnt, totalRows, hll.cardinality()), hll.toBytes());
    }
}
