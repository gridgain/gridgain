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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * H2 cached statement key.
 */
class H2CachedStatementKey {
    /** Schema name. */
    private final String schemaName;

    /** SQL. */
    private final String sql;

    /** Flags. */
    private final byte flags;

    /**
     * Full-fledged constructor.
     *
     * @param schemaName Schema name.
     * @param sql SQL.
     * @param flags Query flags.
     */
    public H2CachedStatementKey(String schemaName, String sql, byte flags) {
        this.schemaName = schemaName;
        this.sql = sql;
        this.flags = flags;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * (31 * (schemaName != null ? schemaName.hashCode() : 0) + (sql != null ? sql.hashCode() : 0)) +
            flags;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        H2CachedStatementKey other = (H2CachedStatementKey)o;

        return F.eq(sql, other.sql) && F.eq(schemaName, other.schemaName) && flags == other.flags;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2CachedStatementKey.class, this);
    }
}
