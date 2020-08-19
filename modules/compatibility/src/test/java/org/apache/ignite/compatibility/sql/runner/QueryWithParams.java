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

package org.apache.ignite.compatibility.sql.runner;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.util.typedef.F;

public class QueryWithParams {

    private final String qry;

    private final List<Object> params;

    public QueryWithParams(String qry, List<Object> params) {
        assert !F.isEmpty(qry);

        this.qry = qry;
        this.params = F.isEmpty(params) ? Collections.emptyList() : Collections.unmodifiableList(params);
    }

    public QueryWithParams(String qry) {
        this(qry, null);
    }

    public boolean parametrized() {
        return !F.isEmpty(params);
    }

    public String query() {
        return qry;
    }

    public List<Object> params() {
        return params;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QueryWithParams params1 = (QueryWithParams)o;

        return qry.equals(params1.qry) &&
            params.equals(params1.params);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(qry, params);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "QueryWithParams[" +
            "qry='" + qry + '\'' +
            ", params=" + params +
            ']';
    }
}
