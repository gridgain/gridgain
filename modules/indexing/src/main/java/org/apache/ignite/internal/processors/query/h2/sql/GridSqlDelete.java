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

package org.apache.ignite.internal.processors.query.h2.sql;

import org.gridgain.internal.h2.util.StringUtils;

/** */
public class GridSqlDelete extends GridSqlStatement {
    /** */
    private GridSqlElement from;

    /** */
    private GridSqlElement where;

    /** */
    public GridSqlDelete from(GridSqlElement from) {
        this.from = from;
        return this;
    }

    /** */
    public GridSqlElement from() {
        return from;
    }

    /** */
    public GridSqlDelete where(GridSqlElement where) {
        this.where = where;
        return this;
    }

    /** */
    public GridSqlElement where() {
        return where;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StringBuilder buff = new StringBuilder(explain() ? "EXPLAIN " : "");
        buff.append("DELETE")
            .append("\nFROM ")
            .append(from.getSQL());

        if (where != null)
            buff.append("\nWHERE ").append(StringUtils.unEnclose(where.getSQL()));

        if (limit != null)
            buff.append("\nLIMIT (").append(StringUtils.unEnclose(limit.getSQL())).append(')');

        return buff.toString();
    }
}
