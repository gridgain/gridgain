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
package org.apache.ignite.internal.processors.query.stat.schema;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.ignite.internal.processors.query.stat.StatisticsKey;

/**
 *
 */
public class ObjectStatisticsInfo implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final StatisticsKey key;

    /** */
    private final ColumnStatisticsInfo[] cols;

    /** */
    private final StatisticConfiguration cfg;

    /** */
    private final long version;

    /** */
    public ObjectStatisticsInfo(
        StatisticsKey key,
        ColumnStatisticsInfo[] cols,
        StatisticConfiguration cfg
    ) {
        this.key = key;
        this.cols = cols;
        this.cfg = cfg;
        version = 1;
    }

    /** */
    private ObjectStatisticsInfo(
        StatisticsKey key,
        ColumnStatisticsInfo[] cols,
        StatisticConfiguration cfg,
        long version
    ) {
        this.key = key;
        this.cols = cols;
        this.cfg = cfg;
        this.version = version;
    }

    /** */
    public static ObjectStatisticsInfo merge(ObjectStatisticsInfo oldInfo, ObjectStatisticsInfo newInfo) {
        assert oldInfo.key.equals(newInfo.key) : "Invalid schema to merge: [oldKey=" + oldInfo.key
            + ", newKey=" + newInfo.key + ']';

        Set<String> cols = Arrays.stream(oldInfo.cols)
            .map(ColumnStatisticsInfo::name).collect(Collectors.toSet());

        cols.addAll(Arrays.stream(newInfo.cols)
            .map(ColumnStatisticsInfo::name).collect(Collectors.toSet()));

        return new ObjectStatisticsInfo(
            newInfo.key,
            cols.stream()
                .map(ColumnStatisticsInfo::new)
                .collect(Collectors.toList())
                .toArray(new ColumnStatisticsInfo[cols.size()]),
            newInfo.cfg,
            oldInfo.version + 1
        );
    }

    /** */
    public StatisticsKey key() {
        return key;
    }

    /** */
    public ColumnStatisticsInfo[] columns() {
        return cols;
    }

    /** */
    public StatisticConfiguration config() {
        return cfg;
    }

    /** */
    public long version() {
        return version;
    }
}
