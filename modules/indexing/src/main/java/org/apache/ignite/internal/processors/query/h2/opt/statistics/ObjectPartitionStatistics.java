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

import com.google.common.base.Objects;

import java.util.Map;

/**
 * Statistic for some partition of data object.
 */
public class ObjectPartitionStatistics extends ObjectStatistics {
    /** Partition id. */
    private final int partId;

    /** Partition update counter at the moment when */
    private final long updCnt;

    /** Local flag. */
    private final boolean loc;

    /**
     * Constructor.
     *
     * @param partId partition id.
     * @param loc local flag.
     * @param rowsCnt total count of rows in partition.
     * @param updCnt update counter of partition.
     * @param colNameToStat column key to column statistics map.
     */
    public ObjectPartitionStatistics(int partId, boolean loc, long rowsCnt, long updCnt, Map<String, ColumnStatistics> colNameToStat) {
        super(rowsCnt, colNameToStat);

        this.partId = partId;
        this.loc = loc;
        this.updCnt = updCnt;
    }

    public int partId() {
        return partId;
    }

    public boolean local() {
        return loc;
    }

    public long updCnt() {
        return updCnt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ObjectPartitionStatistics that = (ObjectPartitionStatistics) o;
        return partId == that.partId &&
                updCnt == that.updCnt &&
                loc == that.loc;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(partId, updCnt, loc);
    }
}
