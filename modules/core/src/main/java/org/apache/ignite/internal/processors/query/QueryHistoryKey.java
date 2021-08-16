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

package org.apache.ignite.internal.processors.query;

import java.util.Objects;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Immutable query metrics key used to group metrics.
 */
public class QueryHistoryKey {
    /** Textual query representation. */
    private final String qry;

    /** Schema. */
    private final String schema;

    /** Local flag. */
    private final boolean loc;

    /** Distributed joins. */
    private final boolean distributedJoins;

    /** Enforce join order. */
    private final boolean enforceJoinOrder;

    /** Lazy. */
    private final boolean lazy;

    /** Pre-calculated hash code. */
    private final int hash;

    /**
     * Constructor.
     *
     * @param qry Textual query representation.
     * @param schema Schema.
     * @param loc Local flag of execution query.
     * @param distributedJoins Distributed joins flag.
     * @param enforceJoinOrder Enforce join order flag.
     * @param lazy Lazy flag.
     */
    public QueryHistoryKey(String qry, String schema, boolean loc, boolean distributedJoins,
        boolean enforceJoinOrder, boolean lazy) {
        assert qry != null;
        assert schema != null;

        this.qry = qry;
        this.schema = schema;
        this.loc = loc;
        this.distributedJoins = distributedJoins;
        this.enforceJoinOrder = enforceJoinOrder;
        this.lazy = lazy;

        hash = Objects.hash(qry, schema, loc, distributedJoins, enforceJoinOrder, lazy);
    }

    /**
     * @return Textual representation of query.
     */
    public String query() {
        return qry;
    }

    /**
     * @return Textual representation of schema.
     */
    public String schema() {
        return schema;
    }

    /**
     * @return Local query flag.
     */
    public boolean local() {
        return loc;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return hash;
    }

    /**
     * @return Distributed joins.
     */
    public boolean distributedJoins() {
        return distributedJoins;
    }

    /**
     * @return Enforce join order.
     */
    public boolean enforceJoinOrder() {
        return enforceJoinOrder;
    }

    /**
     * @return Lazy.
     */
    public boolean lazy() {
        return lazy;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QueryHistoryKey key = (QueryHistoryKey)o;

        return F.eq(qry, key.qry) && F.eq(schema, key.schema) && F.eq(loc, key.loc) && F.eq(lazy, key.lazy)
            && F.eq(enforceJoinOrder, key.enforceJoinOrder) && F.eq(distributedJoins, key.distributedJoins);
    }
}
