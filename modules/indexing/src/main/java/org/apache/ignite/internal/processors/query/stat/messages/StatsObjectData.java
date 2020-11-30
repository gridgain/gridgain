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

import org.apache.ignite.internal.processors.query.stat.StatsKey;
import org.apache.ignite.internal.processors.query.stat.StatsType;
import java.io.Serializable;
import java.util.Map;

/**
 * Statistics for some object (index or table) in database,
 */
public class StatsObjectData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Statistics key. */
    private StatsKey key;

    /** Total row count in current object. */
    private long rowsCnt;

    /** Type of statistics. */
    private StatsType type;

    /** Partition id if statistics was collected by partition. */
    private int partId;

    /** Update counter if statistics was collected by partition. */
    private long updCnt;

    /** Columns key to statistic map. */
    private Map<String, StatsColumnData> data;

    /**
     * Constructor.
     *
     * @param key Statistics key.
     * @param rowsCnt Total row count.
     * @param type Statistics type.
     * @param partId Partition id.
     * @param updCnt Partition update counter.
     * @param data Map of statistics column data.
     */
    public StatsObjectData(
            StatsKey key,
            long rowsCnt,
            StatsType type,
            int partId,
            long updCnt,
            Map<String, StatsColumnData> data
    ) {
        this.key = key;
        this.rowsCnt = rowsCnt;
        this.type = type;
        this.partId = partId;
        this.updCnt = updCnt;
        this.data = data;
    }

    /**
     * @return Statistics key.
     */
    public StatsKey key() {
        return key;
    }

    /**
     * @return Total rows count.
     */
    public long rowsCnt() {
        return rowsCnt;
    }

    /**
     * @return Statistics type.
     */
    public StatsType type() {
        return type;
    }

    /**
     * @return Partition id.
     */
    public int partId() {
        return partId;
    }

    /**
     * @return Partition update counter.
     */
    public long updCnt() {
        return updCnt;
    }

    /**
     * @return Statistics column data.
     */
    public Map<String, StatsColumnData> data() {
        return data;
    }
}
