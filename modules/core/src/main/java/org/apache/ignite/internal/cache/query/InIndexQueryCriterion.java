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

package org.apache.ignite.internal.cache.query;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.SB;

/**
 * Criterion for IN operator.
 */
public final class InIndexQueryCriterion implements SqlIndexQueryCriterion {
    /** */
    private static final long serialVersionUID = 0L;

    /** Index field name. */
    private final String field;

    /** Set of values that indexed {@link #field()} value should match. */
    private final Set<Object> vals;

    /** */
    public InIndexQueryCriterion(String field, Collection<?> vals) {
        this.field = field;
        this.vals = Collections.unmodifiableSet(new HashSet<>(vals));
    }

    /** */
    public Set<Object> values() {
        return vals;
    }

    /** {@inheritDoc} */
    @Override public String field() {
        return field;
    }

    /** {@inheritDoc} */
    @Override public String toSql(SqlBuilderContext ctx) {
        if (vals.isEmpty()) {
            throw new IllegalArgumentException("Unsupported criterion [criterion=" + this + ']');
        }

        SqlBuilderContext.ColumnDescriptor column = ctx.resolveColumn(field);
        String columnName = column.name();

        // SQL IN doesn't include NULLs, so must add IS NULL explicitly.
        boolean hasNull = vals.contains(null);

        if (!hasNull)
            return buildInStatement(columnName, ctx);

        if (vals.size() == 1)
            return column.nullable() ? columnName + " IS NULL" : "FALSE";

        return "(" + (column.nullable() ? columnName + " IS NULL OR " : "") + buildInStatement(columnName, ctx) + ')';
    }

    private String buildInStatement(String columnName, SqlBuilderContext ctx) {
        SB buf = new SB();

        vals.forEach(val -> {
            if (val == null)
                return;

            if (buf.length() > 0)
                buf.a(", ");

            buf.a('?');

            ctx.addArgument(val);
        });

        return columnName + " IN (" + buf + ')';
    }
}
