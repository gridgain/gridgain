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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.util.Comparator;
import java.util.List;
import net.agkn.hll.HLL;
import org.gridgain.internal.h2.table.Column;
import org.gridgain.internal.h2.value.Value;

/**
 * Collector to compute statistic by single column.
 */
public class ColumnStatisticsCollector {
    /** Seed to proper work of hash function. */
    private static final int seed = 123456;

    /** */
    private final Column col;

    /** Value hash function. */
    private final HashFunction hash;

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

        hash = Hashing.murmur3_128(seed);
    }

    public void add(Value val) {
        if (isNull((val))) {
            nullsCnt++;

            return;
        }

        long valHash = hash.hashBytes(val.getBytes()).asLong();
        hll.addRaw(valHash);

        if (null == min || comp.compare(val, min) < 0)
            min = val;

        if (null == max || comp.compare(val, max) > 0)
            max = val;
    }

    private boolean isNull(Value v) {
        return v == null || v.getType().getValueType() == Value.NULL;
    }

    /*private byte[] getBytes(Value v) {
        switch (v.getType().getValueType()) {
            case Value.NULL:
                throw new IllegalArgumentException();

            case Value.BOOLEAN:
                return v.getBytes();

            case Value.BYTE:
                return new GridH2Byte(v);

            case Value.SHORT:
                return new GridH2Short(v);

            case Value.INT:
                return new GridH2Integer(v);

            case Value.LONG:
                return new GridH2Long(v);

            case Value.DECIMAL:
                return new GridH2Decimal(v);

            case Value.DOUBLE:
                return new GridH2Double(v);

            case Value.FLOAT:
                return new GridH2Float(v);

            case Value.DATE:
                return new GridH2Date(v);

            case Value.TIME:
                return new GridH2Time(v);

            case Value.TIMESTAMP:
                return new GridH2Timestamp(v);

            case Value.BYTES:
                return new GridH2Bytes(v);

            case Value.STRING:
            case Value.STRING_FIXED:
            case Value.STRING_IGNORECASE:
                return new GridH2String(v);

            case Value.ROW: // Intentionally converts Value.ROW to GridH2Array to preserve compatibility
            case Value.ARRAY:
                return new GridH2Array(v);

            case Value.JAVA_OBJECT:
                if (v instanceof GridH2ValueCacheObject)
                    return new GridH2CacheObject((GridH2ValueCacheObject)v);

                return new GridH2JavaObject(v);

            case Value.UUID:
                return new GridH2Uuid(v);

            case Value.GEOMETRY:
                return new GridH2Geometry(v);

            default:
                throw new IllegalStateException("Unsupported H2 type: " + v.getType());
        }
    }*/

    public ColumnStatistics finish(long totalRows) {
        int nulls = nullsPercent(nullsCnt, totalRows);

        int cardinality = 0;

        cardinality = cardinalityPercent(nullsCnt, totalRows, hll.cardinality());

        return new ColumnStatistics(min, max, nulls, cardinality, hll.toBytes());
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
