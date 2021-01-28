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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.ignite.internal.processors.query.stat.StatisticsKey;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class StatisticsObjectConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int cacheGrpId;

    /** */
    @GridToStringInclude
    private final StatisticsKey key;

    /** */
    @GridToStringInclude
    private final StatisticsColumnConfiguration[] cols;

    /** */
    @GridToStringInclude
    private final StatisticsCollectConfiguration cfg;

    /** */
    private final long ver;

    /** */
    public StatisticsObjectConfiguration(
        int grpId,
        StatisticsKey key,
        StatisticsColumnConfiguration[] cols,
        StatisticsCollectConfiguration cfg
    ) {
        this(grpId, key, cols, cfg, 0);
    }

    /** */
    private StatisticsObjectConfiguration(
        int grpId,
        StatisticsKey key,
        StatisticsColumnConfiguration[] cols,
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
    public static StatisticsObjectConfiguration merge(StatisticsObjectConfiguration oldInfo, StatisticsObjectConfiguration newInfo) {
        assert oldInfo.key.equals(newInfo.key) : "Invalid schema to merge: [oldKey=" + oldInfo.key
            + ", newKey=" + newInfo.key + ']';

        Set<String> cols = Arrays.stream(oldInfo.cols)
            .map(StatisticsColumnConfiguration::name).collect(Collectors.toSet());

        cols.addAll(Arrays.stream(newInfo.cols)
            .map(StatisticsColumnConfiguration::name).collect(Collectors.toSet()));

        return new StatisticsObjectConfiguration(
            newInfo.cacheGrpId,
            newInfo.key,
            cols.stream()
                .map(StatisticsColumnConfiguration::new)
                .collect(Collectors.toList())
                .toArray(new StatisticsColumnConfiguration[cols.size()]),
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
    public StatisticsColumnConfiguration[] columns() {
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
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        StatisticsObjectConfiguration that = (StatisticsObjectConfiguration)o;

        return cacheGrpId == that.cacheGrpId
            && ver == that.ver
            && Objects.equals(key, that.key)
            && Arrays.equals(cols, that.cols)
            && Objects.equals(cfg, that.cfg);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = Objects.hash(cacheGrpId, key, cfg, ver);

        result = 31 * result + Arrays.hashCode(cols);

        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StatisticsObjectConfiguration.class, this);
    }
}
