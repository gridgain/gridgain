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
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemView;
import org.h2.table.Column;
import org.h2.value.DataType;

/**
 * Sql view column representation for a {@link SystemView}.
 */
public class SqlViewColumnView {
    /** System view. */
    private final SqlSystemView view;

    /** Column. */
    private final Column col;

    /**
     * @param view View.
     * @param col Column.
     */
    public SqlViewColumnView(SqlSystemView view, Column col) {
        this.view = view;
        this.col = col;
    }

    /** @return Column name. */
    @Order
    public String columnName() {
        return col.getName();
    }

    /** @return Schema name. */
    @Order(2)
    public String schemaName() {
        return QueryUtils.sysSchemaName();
    }

    /** @return View name. */
    @Order(1)
    public String viewName() {
        return view.getTableName();
    }

    /** @return Field data type. */
    public String type() {
        return DataType.getTypeClassName(col.getType().getValueType(), false);
    }

    /** @return Field default. */
    public String defaultValue() {
        return String.valueOf(col.getDefaultExpression());
    }

    /** @return Precision. */
    public long precision() {
        return col.getType().getPrecision();
    }

    /** @return Scale. */
    public int scale() {
        return col.getType().getScale();
    }

    /** @return {@code True} if nullable field. */
    public boolean nullable() {
        return col.isNullable();
    }
}
