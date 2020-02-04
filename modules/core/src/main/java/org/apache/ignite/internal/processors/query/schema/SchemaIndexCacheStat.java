/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.query.schema;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Class for accumulation of record types and number of indexed records in index tree.
 */
public class SchemaIndexCacheStat {
    /**
     * Indexed types.
     */
    public Set<String> types = new HashSet<>();

    /**
     * Indexed keys.
     */
    public int scanned;

    @Override public String toString() {
        return S.toString(SchemaIndexCacheStat.class, this);
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SchemaIndexCacheStat stat = (SchemaIndexCacheStat)o;
        return scanned == stat.scanned &&
            types.equals(stat.types);
    }

    @Override public int hashCode() {
        return Objects.hash(types, scanned);
    }
}
