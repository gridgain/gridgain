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
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
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
    public static final StatisticsColumnConfiguration[] EMPTY_COLUMN_CFGS_ARR = new StatisticsColumnConfiguration[0];

    /** */
    @GridToStringInclude
    private final StatisticsKey key;

    /** */
    @GridToStringInclude
    private final StatisticsColumnConfiguration[] cols;

    /** */
    private final long ver;

    /** */
    public StatisticsObjectConfiguration(
        StatisticsKey key,
        StatisticsColumnConfiguration[] cols
    ) {
        this(key, cols, 0);
    }

    /** */
    public StatisticsObjectConfiguration(
        StatisticsKey key,
        StatisticsColumnConfiguration[] cols,
        long ver
    ) {
        this.key = key;
        this.cols = cols;
        this.ver = ver;
    }

    /** */
    public static StatisticsObjectConfiguration merge(StatisticsObjectConfiguration oldCfg, StatisticsObjectConfiguration newCfg) {
        assert oldCfg.key.equals(newCfg.key) : "Invalid schema to merge: [oldKey=" + oldCfg.key
            + ", newKey=" + newCfg.key + ']';

        Map<String, StatisticsColumnConfiguration> oldCols = Arrays.stream(oldCfg.cols)
            .collect(Collectors.toMap(StatisticsColumnConfiguration::name, Function.identity()));

        Map<String, StatisticsColumnConfiguration> newCols = Arrays.stream(newCfg.cols)
            .collect(Collectors.toMap(StatisticsColumnConfiguration::name, Function.identity()));

        newCols.putAll(oldCols);

        return new StatisticsObjectConfiguration(
            newCfg.key,
            newCols.values().toArray(EMPTY_COLUMN_CFGS_ARR),
            oldCfg.ver + 1
        );
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

        return ver == that.ver
            && Objects.equals(key, that.key)
            && Arrays.equals(cols, that.cols);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = Objects.hash(ver, key);

        result = 31 * result + Arrays.hashCode(cols);

        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StatisticsObjectConfiguration.class, this);
    }
}
