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

import org.apache.ignite.cache.query.IndexQueryCriterion;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Criterion for IN operator.
 */
public final class InIndexQueryCriterion implements IndexQueryCriterion {
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
}
