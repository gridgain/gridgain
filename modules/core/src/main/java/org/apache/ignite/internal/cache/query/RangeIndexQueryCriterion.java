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

import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.SB;

/**
 * Range index criterion that applies to BPlusTree based indexes.
 */
public final class RangeIndexQueryCriterion implements SqlIndexQueryCriterion {
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

    @Override public String toSQL(SqlBuilderContext ctx, List<Object> args) {
        SB buf = new SB();
        String column = ctx.columnName();
        boolean nullable = ctx.nullable();
        // Consider all flags to decipher which condition was requested.
        // TODO refactor - replace RangeIndexQueryCriterion with per-condition criterions.
        if (lower == null && upper == null) {
            if (lowerNull && upperNull) {
                // between(null, null) or eq(null) in which case all flags are true.
                if (!lowerIncl || !upperIncl) {
                    throw new IllegalArgumentException("Unsupported criterion [criterion=" + this + "]");
                }

                buf.a(column).a(" IS NULL");
            } else if (lowerNull) {
                // gt(null) or gte(null), upperIncl is always true.
                if (!upperIncl) {
                    throw new IllegalArgumentException("Unsupported criterion [criterion=" + this + "]");
                }

                if (!lowerIncl) {
                    // gt(null) - same as NOT NULL
                    buf.a(column).a(" IS NOT NULL");
                } else {
                    // gte(null) - same as TRUE - no condition
                    buf.a("TRUE");
                }
            } else if (upperNull) {
                // lt(null) or lte(null), lowerIncl is always true.
                if (!lowerIncl) {
                    throw new IllegalArgumentException("Unsupported criterion [criterion=" + this + "]");
                }

                if (!upperIncl) {
                    // lt(null) - same as FALSE
                    buf.a("FALSE");
                }
                else {
                    // lte(null) - same as IS NULL
                    buf.a(column).a(" IS NULL");
                }
            } else {
                // Neither lower nor upper are set explicitly.
                throw new IllegalArgumentException("Unsupported criterion [criterion=" + this + "]");
            }
        } else if (lower != null && upper == null) {
            if (lowerNull || !upperIncl) {
                throw new IllegalArgumentException("Unsupported criterion [criterion=" + this + "]");
            }

            if (upperNull) {
                // between(lower, NULL), flags are always the same.
                if (!(lowerIncl)) {
                    throw new IllegalArgumentException("Unsupported criterion [criterion=" + this + "]");
                }

                // Same as FALSE.
                buf.a(" FALSE");
            } else {
                // gt(lower) or gte(lower), upperIncl is always true.
                buf.a(column).a(lowerIncl ? " >= ?" : " > ?");
                args.add(lower);
            }
        } else if (lower == null /*&& upper != null*/) {
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

            String condition = column + (upperIncl ? " <= ?" : " < ?");

            if (nullable) {
                // TODO optimize for single index condition using UNION
                buf.a("(")
                    .a(column).a(" IS NULL")
                    .a(" OR ")
                    .a(condition)
                    .a(")");
            } else {
                buf.a(condition);
            }

            args.add(upper);
        } else /*if (lower != null && upper != null)*/ {
            // between(lower, upper), flags are always the same.
            if (!(lowerIncl && upperIncl && !lowerNull && !upperNull)) {
                throw new IllegalArgumentException("Unsupported criterion [criterion=" + this + "]");
            }

            buf.a(column).a(" >= ?")
                .a(" AND ")
                .a(column).a(" <= ?");
            args.add(lower);
            args.add(upper);
        }

        return buf.toString();
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
