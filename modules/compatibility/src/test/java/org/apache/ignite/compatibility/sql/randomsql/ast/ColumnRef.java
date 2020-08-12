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

package org.apache.ignite.compatibility.sql.randomsql.ast;

import org.apache.ignite.compatibility.sql.randomsql.Column;

/**
 * Column reference.
 */
public class ColumnRef implements Ast {
    /** */
    private final TableRef tbl;

    /** */
    private final Column col;

    /**
     * @param tbl Table reference.
     * @param col Referenced column.
     */
    ColumnRef(TableRef tbl, Column col) {
        this.col = col;
        this.tbl = tbl;
    }

    /** {@inheritDoc} */
    @Override public void writeTo(StringBuilder out) {
        out.append(' ')
            .append(tbl.alias())
            .append('.')
            .append(col.name())
            .append(' ');
    }

    /**
     * Returns type of the column.
     *
     * @return Type of the column.
     */
    public Class<?> typeClass() {
        return col.typeClass();
    }
}
