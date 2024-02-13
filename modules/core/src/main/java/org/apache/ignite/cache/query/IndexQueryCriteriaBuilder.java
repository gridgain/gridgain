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

package org.apache.ignite.cache.query;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.internal.cache.query.InIndexQueryCriterion;
import org.apache.ignite.internal.cache.query.RangeIndexQueryCriterion;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Factory of {@link IndexQueryCriterion} for {@link IndexQuery}.
 */
public class IndexQueryCriteriaBuilder {
    /**
     * Equal To.
     *
     * @param field Index field to apply criterion.
     * @param val Strict equality value.
     * @return Criterion.
     */
    public static IndexQueryCriterion eq(String field, Object val) {
        return in(field, Collections.singletonList(val));
    }

    /**
     * Less Than.
     * <p>
     * Please note that {@code null} is not a valid value for setting search boundaries.
     * The result of executing this criterion will not contain {@code NULL} values for the specified {@code field}.
     * To obtain {@code NULL} values of the specified {@code field}, use explicit {@code eq(field, null)} or
     * {@code in(field, null, ...)} criteria.
     *
     * @param field Index field to apply criterion.
     * @param val Exclusive upper bound.
     * @return Criterion.
     */
    public static IndexQueryCriterion lt(String field, Object val) {
        A.notNullOrEmpty(field, "field");
        A.notNull(val, "val");

        RangeIndexQueryCriterion c = new RangeIndexQueryCriterion(field, null, val);
        c.lowerIncl(true);
        c.upperNull(val == null);

        return c;
    }

    /**
     * Less Than or Equal To.
     * <p>
     * Please note that {@code null} is not a valid value for setting search boundaries.
     * The result of executing this criterion will not contain {@code NULL} values for the specified {@code field}.
     * To obtain {@code NULL} values of the specified {@code field}, use explicit {@code eq(field, null)} or
     * {@code in(field, null, ...)} criteria.
     *
     * @param field Index field to apply criterion.
     * @param val Inclusive upper bound.
     * @return Criterion.
     */
    public static IndexQueryCriterion lte(String field, Object val) {
        A.notNullOrEmpty(field, "field");
        A.notNull(val, "val");

        RangeIndexQueryCriterion c = new RangeIndexQueryCriterion(field, null, val);
        c.lowerIncl(true);
        c.upperIncl(true);
        c.upperNull(val == null);

        return c;
    }

    /**
     * Greater Than.
     * <p>
     * Please note that {@code null} is not a valid value for setting search boundaries.
     * The result of executing this criterion will not contain {@code NULL} values for the specified {@code field}.
     * To obtain {@code NULL} values of the specified {@code field}, use explicit {@code eq(field, null)} or
     * {@code in(field, null, ...)} criteria.
     *
     * @param field Index field to apply criterion.
     * @param val Exclusive lower bound.
     * @return Criterion.
     */
    public static IndexQueryCriterion gt(String field, Object val) {
        A.notNullOrEmpty(field, "field");
        A.notNull(val, "val");

        RangeIndexQueryCriterion c = new RangeIndexQueryCriterion(field, val, null);
        c.upperIncl(true);
        c.lowerNull(val == null);

        return c;
    }

    /**
     * Greater Than or Equal To.
     * <p>
     * Please note that {@code null} is not a valid value for setting search boundaries.
     * The result of executing this criterion will not contain {@code NULL} values for the specified {@code field}.
     * To obtain {@code NULL} values of the specified {@code field}, use explicit {@code eq(field, null)} or
     * {@code in(field, null, ...)} criteria.
     *
     * @param field Index field to apply criterion.
     * @param val Inclusive lower bound.
     * @return Criterion.
     */
    public static IndexQueryCriterion gte(String field, Object val) {
        A.notNullOrEmpty(field, "field");
        A.notNull(val, "val");

        RangeIndexQueryCriterion c = new RangeIndexQueryCriterion(field, val, null);
        c.lowerIncl(true);
        c.upperIncl(true);
        c.lowerNull(val == null);

        return c;
    }

    /**
     * Between.
     * <p>
     * Please note that {@code null} is not a valid value for setting search boundaries.
     * The result of executing this criterion will not contain {@code NULL} values for the specified {@code field}.
     * To obtain {@code NULL} values of the specified {@code field}, use explicit {@code eq(field, null)} or
     * {@code in(field, null, ...)} criteria.
     *
     * @param field Index field to apply criterion.
     * @param lower Inclusive lower bound.
     * @param upper Inclusive upper bound.
     * @return Criterion.
     */
    public static IndexQueryCriterion between(String field, Object lower, Object upper) {
        A.notNullOrEmpty(field, "field");
        A.notNull(lower, "lower");
        A.notNull(upper, "upper");

        RangeIndexQueryCriterion c = new RangeIndexQueryCriterion(field, lower, upper);
        c.lowerIncl(true);
        c.upperIncl(true);
        c.lowerNull(lower == null);
        c.upperNull(upper == null);

        return c;
    }

    /**
     * In.
     *
     * @param field Index field to apply criterion.
     * @param vals Collection of values to find.
     * @return Criterion.
     */
    public static IndexQueryCriterion in(String field, Collection<?> vals) {
        A.notNullOrEmpty(field, "field");
        A.notEmpty(vals, "vals");

        return new InIndexQueryCriterion(field, vals);
    }
}
