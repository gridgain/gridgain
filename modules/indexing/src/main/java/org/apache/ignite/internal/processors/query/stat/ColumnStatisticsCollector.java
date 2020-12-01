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
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;

import org.apache.ignite.internal.processors.query.stat.hll.HLL;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.gridgain.internal.h2.table.Column;
import org.gridgain.internal.h2.value.TypeInfo;
import org.gridgain.internal.h2.value.Value;

import static org.apache.ignite.internal.processors.query.h2.H2Utils.isNullValue;

/**
 * Collector to compute statistic by single column.
 */
public class ColumnStatisticsCollector {
    /** Column. */
    private final Column col;

    /** Hyper Log Log structure */
    private final HLL hll = buildHll();

    /** Minimum value. */
    private Value min = null;

    /** Maximum value. */
    private Value max = null;

    /** Total values in column. */
    private long total = 0;

    /** Total size of all non nulls values (in bytes).*/
    private long size = 0;

    /** Column value comparator. */
    private final Comparator<Value> comp;

    /** Null values counter. */
    private long nullsCnt;

    /** Is column has complex type. */
    private final boolean complexType;

    /** Hasher. */
    private final Hasher hash = new Hasher();

    /**
     * Constructor.
     *
     * @param col Column to collect statistics by.
     * @param comp Column values comparator.
     */
    public ColumnStatisticsCollector(Column col, Comparator<Value> comp) {
        this.col = col;
        this.comp = comp;

        TypeInfo colTypeInfo = col.getType();
        complexType = colTypeInfo == TypeInfo.TYPE_ARRAY || colTypeInfo == TypeInfo.TYPE_ENUM_UNDEFINED
                || colTypeInfo == TypeInfo.TYPE_JAVA_OBJECT || colTypeInfo == TypeInfo.TYPE_RESULT_SET
                || colTypeInfo == TypeInfo.TYPE_UNKNOWN;

    }

    /**
     * Try to fix unexpected behaviour of base Value class.
     *
     * @param value Value to convert.
     * @return Byte array.
     */
    private byte[] getBytes(Value value) {
        switch (value.getValueType()) {
            case Value.STRING:
                String strValue = value.getString();
                return strValue.getBytes(StandardCharsets.UTF_8);
            case Value.BOOLEAN:
                return value.getBoolean() ? new byte[]{1} : new byte[]{0};
            case Value.DECIMAL:
            case Value.DOUBLE:
            case Value.FLOAT:
                return U.join(value.getBigDecimal().unscaledValue().toByteArray(),
                        BigInteger.valueOf(value.getBigDecimal().scale()).toByteArray());
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

    /**
     * Add value to statistics.
     *
     * @param val Value to add to statistics.
     */
    public void add(Value val) {
        total++;

        if (isNullValue(val)) {
            nullsCnt++;

            return;
        }

        byte bytes[] = getBytes(val);
        size += bytes.length;

        hll.addRaw(hash.fastHash(bytes));

        if (!complexType) {
            if (null == min || comp.compare(val, min) < 0)
                min = val;

            if (null == max || comp.compare(val, max) > 0)
                max = val;
        }
    }

    /**
     * Get total column statistics.
     *
     * @return Aggregated column statistics.
     */
    public ColumnStatistics finish() {
        int nulls = nullsPercent(nullsCnt, total);

        int cardinality = cardinalityPercent(nullsCnt, total, hll.cardinality());

        int averageSize = averageSize(size, total, nullsCnt);

        return new ColumnStatistics(min, max, nulls, cardinality, total, averageSize, hll.toBytes());
    }

    /**
     * Count percent of null values.
     *
     * @param nullsCnt Total number of nulls.
     * @param totalRows Total number of rows.
     * @return Percent of null values.
     */
    private static int nullsPercent(long nullsCnt, long totalRows) {
        if (totalRows > 0)
            return (int)(100 * nullsCnt / totalRows);
        return 0;
    }

    /**
     * Count cardinality percent.
     *
     * @param nullsCnt Total number of nulls.
     * @param totalRows Total number of rows.
     * @param cardinality Total cardinality (number of different values).
     * @return Percent of different non null values.
     */
    private static int cardinalityPercent(long nullsCnt, long totalRows, long cardinality) {
        if (totalRows - nullsCnt > 0)
            return (int)(100 * cardinality / (totalRows - nullsCnt));
        return 0;
    }

    /**
     * Calculate average record size in bytes.
     *
     * @param size Total size of all records.
     * @param total Total number of all records.
     * @param nullsCnt Number of nulls record.
     * @return Average size of not null record in byte.
     */
    private static int averageSize(long size, long total, long nullsCnt) {
        long averageSizeLong = (total - nullsCnt > 0) ? (size / (total - nullsCnt)) : 0;
        return (averageSizeLong > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) averageSizeLong;
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
     * @param comp Value comparator.
     * @param partStats Column statistics by partitions.
     * @return Column statistics for all partitions.
     */
    public static ColumnStatistics aggregate(Comparator<Value> comp, List<ColumnStatistics> partStats) {
        HLL hll = buildHll();

        Value min = null;
        Value max = null;

        // Total number of nulls
        long nullsCnt = 0;

        // Total values (null and not null) counter)
        long total = 0;

        // Total size in bytes
        long totalSize = 0;

        for (ColumnStatistics partStat : partStats) {
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

        int averageSize = averageSize(totalSize, total, nullsCnt);

        return new ColumnStatistics(min, max, nullsPercent(nullsCnt, total),
                cardinalityPercent(nullsCnt, total, hll.cardinality()), total, averageSize, hll.toBytes());
    }

    /**
     * Get HLL with default params.
     *
     * @return Empty hll structure.
     */
    private static HLL buildHll() {
        return new HLL(13, 5);
    }
}
