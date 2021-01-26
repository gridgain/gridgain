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
package org.apache.ignite.internal.processors.query.stat.config;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.ignite.internal.processors.query.stat.StatisticsKey;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class ObjectStatisticsConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int cacheGrpId;

    /** */
    @GridToStringInclude
    private final StatisticsKey key;

    /** */
    @GridToStringInclude
    private final ColumnStatisticsConfiguration[] cols;

    /** */
    @GridToStringInclude
    private final StatisticsCollectConfiguration cfg;

    /** */
    private final long ver;

    /** */
    public ObjectStatisticsConfiguration(
        int grpId,
        StatisticsKey key,
        ColumnStatisticsConfiguration[] cols,
        StatisticsCollectConfiguration cfg
    ) {
        this(grpId, key, cols, cfg, 0);
    }

    /** */
    private ObjectStatisticsConfiguration(
        int grpId,
        StatisticsKey key,
        ColumnStatisticsConfiguration[] cols,
        StatisticsCollectConfiguration cfg,
        long ver
    ) {
        this.cacheGrpId = grpId;
        this.key = key;
        this.cols = cols;
        this.cfg = cfg;
        this.ver = ver;
    }

    /** */
    public static ObjectStatisticsConfiguration merge(ObjectStatisticsConfiguration oldInfo, ObjectStatisticsConfiguration newInfo) {
        assert oldInfo.key.equals(newInfo.key) : "Invalid schema to merge: [oldKey=" + oldInfo.key
            + ", newKey=" + newInfo.key + ']';

        Set<String> cols = Arrays.stream(oldInfo.cols)
            .map(ColumnStatisticsConfiguration::name).collect(Collectors.toSet());

        cols.addAll(Arrays.stream(newInfo.cols)
            .map(ColumnStatisticsConfiguration::name).collect(Collectors.toSet()));

        return new ObjectStatisticsConfiguration(
            newInfo.cacheGrpId,
            newInfo.key,
            cols.stream()
                .map(ColumnStatisticsConfiguration::new)
                .collect(Collectors.toList())
                .toArray(new ColumnStatisticsConfiguration[cols.size()]),
            newInfo.cfg,
            oldInfo.ver + 1
        );
    }

    /** */
    public int cacheGroupId() {
        return cacheGrpId;
    }

    /** */
    public StatisticsKey key() {
        return key;
    }

    /** */
    public ColumnStatisticsConfiguration[] columns() {
        return cols;
    }

    /** */
    public StatisticsCollectConfiguration config() {
        return cfg;
    }

    /** */
    public long version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ObjectStatisticsConfiguration.class, this);
    }
}
