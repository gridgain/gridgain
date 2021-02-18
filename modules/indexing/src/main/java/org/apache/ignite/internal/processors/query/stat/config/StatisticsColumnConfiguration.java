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
import java.util.Objects;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class StatisticsColumnConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final String name;

    /** */
    private final long ver;

    /** */
    private final boolean tombstone;

    /** */
    public StatisticsColumnConfiguration(String name) {
        this(name, 0);
    }

    /** */
    public StatisticsColumnConfiguration(String name, long ver) {
        this(name, ver, false);
    }

    /** */
    private StatisticsColumnConfiguration(String name, long ver, boolean tombstone) {
        this.name = name;
        this.ver = ver;
        this.tombstone = tombstone;
    }

    /** */
    public String name() {
        return name;
    }

    /** */
    public long version() {
        return ver;
    }

    /** */
    public boolean tombstone() {
        return tombstone;
    }

    /** */
    public static StatisticsColumnConfiguration merge(
        StatisticsColumnConfiguration oldCfg,
        StatisticsColumnConfiguration newCfg)
    {
        if (oldCfg == null)
            return newCfg;

        return new StatisticsColumnConfiguration(newCfg.name, oldCfg.ver + 1);
    }

    /** */
    public StatisticsColumnConfiguration createTombstone()
    {
        return new StatisticsColumnConfiguration(name, ver + 1, true);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        StatisticsColumnConfiguration that = (StatisticsColumnConfiguration)o;

        return ver == that.ver
            && Objects.equals(name, that.name);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(name, ver);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StatisticsColumnConfiguration.class, this);
    }
}
