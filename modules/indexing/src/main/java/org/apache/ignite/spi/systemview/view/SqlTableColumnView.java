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

package org.apache.ignite.spi.systemview.view;

import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.gridgain.internal.h2.table.Column;
import org.gridgain.internal.h2.table.IndexColumn;

/**
 * Sql table column representation for a {@link SystemView}.
 */
public class SqlTableColumnView {
    /** Table. */
    private final GridH2Table tbl;

    /** Column. */
    private final Column col;

    /** Query property. */
    private final GridQueryProperty prop;

    /** Affinity column. */
    private final IndexColumn affCol;

    /**
     * @param tbl Table.
     * @param col Column.
     */
    public SqlTableColumnView(GridH2Table tbl, Column col) {
        this.tbl = tbl;
        this.col = col;
        this.prop = tbl.rowDescriptor().type().property(col.getName());
        this.affCol = tbl.getAffinityKeyColumn();
    }

    /** @return Column name. */
    @Order
    public String columnName() {
        return col.getName();
    }

    /** @return Schema name. */
    @Order(2)
    public String schemaName() {
        return tbl.getSchema().getName();
    }

    /** @return Table name. */
    @Order(1)
    public String tableName() {
        return tbl.identifier().table();
    }

    /** @return Field data type. */
    public Class<?> type() {
        if (prop == null)
            return null;

        return prop.type();
    }

    /** @return Field default. */
    public String defaultValue() {
        if (prop == null)
            return null;

        return String.valueOf(prop.defaultValue());
    }

    /** @return Precision. */
    public int precision() {
        return H2Utils.resolveDefaultPrecisionIfUndefined(prop);
    }

    /** @return Scale. */
    public int scale() {
        return H2Utils.resolveDefaultScaleIfUndefined(prop);
    }

    /** @return {@code True} if nullable field. */
    public boolean nullable() {
        return col.isNullable();
    }

    /** @return {@code True} if primary key. */
    public boolean pk() {
        return tbl.rowDescriptor().isKeyColumn(col.getColumnId());
    }

    /** @return {@code True} if autoincremented field. */
    public boolean autoIncrement() {
        return col.isAutoIncrement();
    }

    /** @return {@code True} if autoincremented field. */
    public boolean affinityColumn() {
        return affCol != null && col.getColumnId() == affCol.column.getColumnId();
    }
}
