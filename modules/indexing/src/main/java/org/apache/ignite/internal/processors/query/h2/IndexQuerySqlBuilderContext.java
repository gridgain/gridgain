/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.query.h2;

import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.SqlBuilderContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;

/** The context used when converting an index query to an SQL query. */
class IndexQuerySqlBuilderContext implements SqlBuilderContext {
    /** Query arguments. */
    private final List<Object> arguments;

    /** Table descriptor. */
    private final GridH2Table table;

    IndexQuerySqlBuilderContext(GridH2Table table, List<Object> arguments) {
        this.arguments = arguments;
        this.table = table;
    }

    /** {@inheritDoc} */
    @Override public void addArgument(Object arg) {
        arguments.add(arg);
    }

    /** {@inheritDoc} */
    @Override public ColumnDescriptor resolveColumn(String name) {
        org.gridgain.internal.h2.table.Column column = resolveH2Column(name, table);

        return new ColumnDescriptorImpl("\"" + column.getName() + "\"", column.isNullable());
    }

    private org.gridgain.internal.h2.table.Column resolveH2Column(String name, GridH2Table table) {
        String upperName = name.toUpperCase();

        if (table.doesColumnExist(name))
            return table.getColumn(name);

        if (table.doesColumnExist(upperName))
            return table.getColumn(upperName);

        throw new IgniteException("Column \"" + upperName + "\" not found.");
    }

    private static class ColumnDescriptorImpl implements ColumnDescriptor {
        /** Quoted column name. */
        private final String name;

        /** Nullable column flag. */
        private final boolean nullable;

        private ColumnDescriptorImpl(String name, boolean nullable) {
            this.name = name;
            this.nullable = nullable;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public boolean nullable() {
            return nullable;
        }
    }
}
