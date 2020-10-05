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
package org.apache.ignite.internal.processors.query.stat;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;

import org.apache.ignite.internal.processors.query.stat.hll.HLL;
import org.gridgain.internal.h2.table.Column;
import org.gridgain.internal.h2.value.Value;

/**
 * Collector to compute statistic by single column.
 */
public class ColumnStatisticsCollector {

    /** */
    private final Column col;

    /** Hyper Log Log structure */
    private final HLL hll = new HLL(13/*log2m*/, 5/*registerWidth*/);

    /** Minimum value. */
    private Value min = null;

    /** Maximum value. */
    private Value max = null;

    /** Total vals in column. */
    private long total = 0;

    /** Total size of all non nulls values (in bytes).*/
    private long size = 0;

    /** Temporary byte buffer just to avoid unnecessary object creation. */
    private ByteBuffer bb;

    /** Column value comparator. */
    private final Comparator<Value> comp;

    /** Null values counter. */
    private long nullsCnt;

    /** Hasher. */
    private final Hasher hash = new Hasher();

    /**
     * Constructor.
     *
     * @param col column to collect statistics by.
     * @param comp column values comparator.
     */
    public ColumnStatisticsCollector(Column col, Comparator<Value> comp) {
        this.col = col;
        this.comp = comp;
    }

    /**
     * Try to fix unexpected behaviour of base Value class.
     *
     * @param value value to convert
     * @return byte array
     */
    private byte[] getBytes(Value value) {
        switch (value.getValueType()) {
            case Value.STRING:
                String strValue = value.getString();
                return strValue.getBytes(StandardCharsets.UTF_8);
            case Value.BOOLEAN:
                return value.getBoolean() ? new byte[]{1} : new byte[]{0} ;
            case Value.DECIMAL:
            case Value.DOUBLE:
            case Value.FLOAT:
                return value.getBigDecimal().unscaledValue().toByteArray();
            case Value.TIME:
                return BigInteger.valueOf(value.getTime().getTime()).toByteArray();
            case Value.DATE:
                return BigInteger.valueOf(value.getDate().getTime()).toByteArray();
            case Value.TIMESTAMP:
                return BigInteger.valueOf(value.getTimestamp().getTime()).toByteArray();
            default:
                return value.getBytes();
        }
    }

    public void add(Value val) {
        total++;

        if (isNull((val))) {
            nullsCnt++;

            return;
        }

        byte bytes[] = getBytes(val);
        size += bytes.length;

        hll.addRaw(hash.fastHash(bytes));

        if (null == min || comp.compare(val, min) < 0)
            min = val;

        if (null == max || comp.compare(val, max) > 0)
            max = val;
    }

    private boolean isNull(Value v) {
        return v == null || v.getType().getValueType() == Value.NULL;
    }

    /**
     * Get total column statistics.
     *
     * @return aggregated column statistics.
     */
    public ColumnStatistics finish() {
        int nulls = nullsPercent(nullsCnt, total);

        int cardinality = 0;

        cardinality = cardinalityPercent(nullsCnt, total, hll.cardinality());

        int averageSize = (total - nullsCnt > 0) ? (int) (size / (total - nullsCnt)) : 0;

        return new ColumnStatistics(min, max, nulls, cardinality, total, averageSize, hll.toBytes());
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

    /**
     * @return get column.
     */
    public Column col() {
        return col;
    }

    /**
     * Aggregate specified (partition or local) column statistics into (local or global) single one.
     *
     * @param comp value comparator.
     * @param partStats column statistics by partitions.
     * @return column statistics for all partitions.
     */
    public static ColumnStatistics aggregate(Comparator<Value> comp, List<ColumnStatistics> partStats) {
        HLL hll = new HLL(13/*log2m*/, 5/*registerWidth*/);

        Value min = null;
        Value max = null;

        // Total number of nulls
        long nullsCnt = 0;

        // Total values (null and not null) counter)
        long total = 0;

        // Total size in bytes
        long totalSize = 0;

        for(ColumnStatistics partStat : partStats) {
            HLL partHll = HLL.fromBytes(partStat.raw());
            hll.union(partHll);

            total += partStat.total();
            nullsCnt += (partStat.total() * partStat.nulls()) / 100;
            totalSize += (long)partStat.size() * (partStat.total() * (double)(100 - partStat.nulls()) / 100);

            if (min == null || (partStat.min() != null && comp.compare(partStat.min(), min) < 0))
                min = partStat.min();

            if (max == null || (partStat.max() != null && comp.compare(partStat.max(), max) > 0))
                max = partStat.max();
        }

        int averageSize = (total - nullsCnt > 0) ? (int)(totalSize / (total - nullsCnt)) : 0;

        return new ColumnStatistics(min, max, nullsPercent(nullsCnt, total),
                cardinalityPercent(nullsCnt, total, hll.cardinality()), total, averageSize, hll.toBytes());
    }
}
