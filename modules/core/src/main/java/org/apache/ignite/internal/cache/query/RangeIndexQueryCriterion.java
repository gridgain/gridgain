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

/**
 * Range index criterion that applies to BPlusTree based indexes.
 */
public class RangeIndexQueryCriterion implements SqlIndexQueryCriterion {
    /** */
    private static final long serialVersionUID = 0L;

    /** Index field name. */
    protected final String field;

    /** Lower bound. */
    protected final Object lower;

    /** Upper bound. */
    protected final Object upper;

    /** Should include lower value. */
    protected boolean lowerIncl;

    /** Should include upper value. */
    protected boolean upperIncl;

    /** Whether lower bound is explicitly set to {@code null}. */
    protected boolean lowerNull;

    /** Whether upper bound is explicitly set to {@code null}. */
    protected boolean upperNull;

    /** */
    public RangeIndexQueryCriterion(String field, Object lower, Object upper) {
        this.field = field;
        this.lower = lower;
        this.upper = upper;
    }

    /** */
    public Object lower() {
        return lower;
    }

    /** */
    public Object upper() {
        return upper;
    }

    /** */
    public void lowerIncl(boolean lowerIncl) {
        this.lowerIncl = lowerIncl;
    }

    /** */
    public boolean lowerIncl() {
        return lowerIncl;
    }

    /** */
    public void upperIncl(boolean upperIncl) {
        this.upperIncl = upperIncl;
    }

    /** */
    public boolean upperIncl() {
        return upperIncl;
    }

    /** */
    public void lowerNull(boolean lowerNull) {
        this.lowerNull = lowerNull;
    }

    /** */
    public boolean lowerNull() {
        return lowerNull;
    }

    /** */
    public void upperNull(boolean upperNull) {
        this.upperNull = upperNull;
    }

    /** */
    public boolean upperNull() {
        return upperNull;
    }

    @Override public String toSql(SqlBuilderContext ctx) {
        SqlBuilderContext.ColumnDescriptor column = ctx.resolveColumn(field);
        String columnName = column.name();

        // Consider all flags to decipher which condition was requested.
        if (lower != null && upper != null)
            return between(columnName, ctx);

        if (lower != null)
            return greaterThan(columnName, ctx);

        if (upper != null)
            return lowerThan(column, ctx);

        // lower == null && upper == null
        if (lowerNull && upperNull)
            return betweenNull(columnName);

        if (lowerNull)
            return greaterThanNull(columnName);

        if (upperNull)
            return lowerThanNull(columnName);

        // Neither lower nor upper are set explicitly.
        throw new IllegalArgumentException("Unsupported criterion [criterion=" + this + "]");
    }

    /** between(notNull, notNull)} */
    private String between(String column, SqlBuilderContext ctx) {
        if (!(lowerIncl && upperIncl && !lowerNull && !upperNull)) {
            throw new IllegalArgumentException("Unsupported criterion [criterion=" + this + "]");
        }

        ctx.addArgument(lower);
        ctx.addArgument(upper);

        return column + " >= ? AND " + column + " <= ?";
    }

    /** between(null, null)} */
    private String betweenNull(String column) {
        if (!lowerIncl || !upperIncl) {
            throw new IllegalArgumentException("Unsupported criterion [criterion=" + this + "]");
        }

        return column + " IS NULL";
    }

    /** lt(notNull), lte(notNull) */
    private String lowerThan(SqlBuilderContext.ColumnDescriptor column, SqlBuilderContext ctx) {
        if (upperNull || !lowerIncl) {
            throw new IllegalArgumentException("Unsupported criterion [criterion=" + this + "]");
        }

        // lowerNull is technically irrelevant.
        // lowerNull == true means between(NULL, upper).
        // lowerNull == false means gt(upper) or gte(upper).
        // However, gt(upper) and gte(upper) must include IS NULL anyway.

        // Still, the following flags invariant holds for between(NULL, upper).
        if (lowerNull && !upperIncl) {
            throw new IllegalArgumentException("Unsupported criterion [criterion=" + this + "]");
        }

        ctx.addArgument(upper);

        String condition = column.name() + (upperIncl ? " <= ?" : " < ?");

        if (column.nullable()) {
            return '(' + column.name() + " IS NULL OR " + condition + ')';
        } else {
            return condition;
        }
    }

    /** lt(null), lte(null) */
    private String lowerThanNull(String column) {
        // lt(null) or lte(null), lowerIncl is always true.
        if (!lowerIncl) {
            throw new IllegalArgumentException("Unsupported criterion [criterion=" + this + "]");
        }

        return upperIncl
            ? /* lte(null) */ column + " IS NULL"
            : /* lt(null) */ "FALSE";
    }

    /** gt(notNull), gte(notNull) */
    private String greaterThan(String column, SqlBuilderContext ctx) {
        if (lowerNull || !upperIncl) {
            throw new IllegalArgumentException("Unsupported criterion [criterion=" + this + "]");
        }

        if (upperNull) {
            // between(lower, NULL), flags are always the same.
            if (!(lowerIncl)) {
                throw new IllegalArgumentException("Unsupported criterion [criterion=" + this + "]");
            }

            // Same as FALSE.
            return "FALSE";
        } else {
            // gt(lower) or gte(lower), upperIncl is always true.
            ctx.addArgument(lower);

            return column + (lowerIncl ? " >= ?" : " > ?");
        }
    }

    /** gt(null), gte(null) */
    private String greaterThanNull(String column) {
        if (!upperIncl) {
            throw new IllegalArgumentException("Unsupported criterion [criterion=" + this + "]");
        }

        if (!lowerIncl) {
            // gt(null) - same as NOT NULL
            return column + " IS NOT NULL";
        } else {
            // gte(null) - same as TRUE - no condition
            return "TRUE";
        }
    }

    /** {@inheritDoc} */
    @Override public String field() {
        return field;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return field + (lowerIncl ? "[" : "(") + lower + "; " + upper + (upperIncl ? "]" : ")");
    }
}
