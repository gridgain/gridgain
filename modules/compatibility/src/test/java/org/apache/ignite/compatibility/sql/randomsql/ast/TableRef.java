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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.ignite.compatibility.sql.randomsql.Column;
import org.apache.ignite.compatibility.sql.randomsql.Table;
import org.jetbrains.annotations.Nullable;

/**
 * Table reference.
 */
public class TableRef implements Ast {
    /** */
    private final Table tbl;

    /** */
    private final String alias;

    /** */
    private final Map<String, ColumnRef> cols;

    /**
     * @param tbl Referenced table.
     * @param alias Alias.
     */
    public TableRef(Table tbl, String alias) {
        this.tbl = tbl;
        this.alias = alias;

        cols = Collections.unmodifiableMap(
            tbl.columnsList().stream().collect(Collectors.toMap(Column::name, c -> new ColumnRef(this, c)))
        );
    }

    /**
     * Returns alias for table.
     *
     * @return Alias for table.
     */
    public String alias() {
        return alias;
    }

    /**
     * Returns colum list of the current table.
     *
     * @return Column list of the current table.
     */
    public List<ColumnRef> cols() {
        return new ArrayList<>(cols.values());
    }

    /**
     * Returns column by its name.
     *
     * @param name Column name.
     * @return Column by given name or {@code null}.
     */
    public @Nullable ColumnRef col(String name) {
        return cols.get(Objects.requireNonNull(name, "name"));
    }

    /** {@inheritDoc} */
    @Override public void writeTo(StringBuilder out) {
        out.append(' ')
            .append(tbl.name()).append(' ')
            .append(alias).append(' ');
    }
}
