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
    private final String field;

    /** Lower bound. */
    private final Object lower;

    /** Upper bound. */
    private final Object upper;

    /** Should include lower value. */
    private boolean lowerIncl;

    /** Should include upper value. */
    private boolean upperIncl;

    /** Whether lower bound is explicitly set to {@code null}. */
    private boolean lowerNull;

    /** Whether upper bound is explicitly set to {@code null}. */
    private boolean upperNull;

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
            return lowerThan(columnName, ctx);

        // gt(null), gte(null), lt(null), lte(null), between(null, null) considered invalid and prohibited at a high level
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

    /** lt(notNull), lte(notNull) */
    private String lowerThan(String columnName, SqlBuilderContext ctx) {
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

        // NULLs will not be included.
        return columnName + (upperIncl ? " <= ?" : " < ?");
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

    /** {@inheritDoc} */
    @Override public String field() {
        return field;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return field + (lowerIncl ? "[" : "(") + lower + "; " + upper + (upperIncl ? "]" : ")");
    }
}
