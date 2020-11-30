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

import java.io.Serializable;
import java.util.Objects;

/**
 * Statistics key.
 */
public class StatsKey implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Object schema. */
    private String schema;

    /** Object name. */
    private String obj;

    /**
     * Constructor.
     *
     * @param schema Object schema.
     * @param obj Object name.
     */
    public StatsKey(String schema, String obj) {
        this.schema = schema;
        this.obj = obj;
    }

    /**
     * @return Schema name.
     */
    public String schema() {
        return schema;
    }

    /**
     * @return Object name.
     */
    public String obj() {
        return obj;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsKey statsKey = (StatsKey) o;
        return Objects.equals(schema, statsKey.schema) &&
                Objects.equals(obj, statsKey.obj);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(schema, obj);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "StatsKey{" +
                "schema='" + schema + '\'' +
                ", obj='" + obj + '\'' +
                '}';
    }
}
