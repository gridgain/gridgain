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
package org.apache.ignite.internal.processors.query.stat.messages;

import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessage;
import java.io.Serializable;

/**
 * Statistics for single column.
 */
public class StatsColumnData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Min value in column. */
    private GridH2ValueMessage min;

    /** Max value in column. */
    private GridH2ValueMessage max;

    /** Percent of null values in column. */
    private int nulls;

    /** Percent of distinct values in column (except nulls). */
    private int cardinality;

    /** Total vals in column. */
    private long total;

    /** Average size, for variable size values (in bytes). */
    private int size;

    /** Raw data. */
    private byte[] rawData;

    /**
     * Constructor.
     *
     * @param min Min value in column.
     * @param max Max value in column.
     * @param nulls Percent of null values in column.
     * @param cardinality Percent of distinct values in column.
     * @param total Total values in column.
     * @param size Average size, for variable size types (in bytes).
     * @param rawData Raw data to make statistics aggregate.
     */
    public StatsColumnData(
            GridH2ValueMessage min,
            GridH2ValueMessage max,
            int nulls,
            int cardinality,
            long total,
            int size,
            byte[] rawData
    ) {
        this.min = min;
        this.max = max;
        this.nulls = nulls;
        this.cardinality = cardinality;
        this.total = total;
        this.size = size;
        this.rawData = rawData;
    }

    /**
     * @return Min value in column.
     */
    public GridH2ValueMessage min() {
        return min;
    }

    /**
     * @return Max value in column.
     */
    public GridH2ValueMessage max() {
        return max;
    }

    /**
     * @return Percent of null values in column.
     */
    public int nulls() {
        return nulls;
    }

    /**
     * @return Percent of distinct values in column.
     */
    public int cardinality() {
        return cardinality;
    }

    /**
     * @return Total values in column.
     */
    public long total() {
        return total;
    }

    /**
     * @return Average size, for variable size types (in bytes).
     */
    public int size() {
        return size;
    }

    /**
     * @return Raw data.
     */
    public byte[] rawData() {
        return rawData;
    }
}
