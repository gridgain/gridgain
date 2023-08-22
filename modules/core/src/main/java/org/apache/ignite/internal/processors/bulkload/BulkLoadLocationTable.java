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

package org.apache.ignite.internal.processors.bulkload;

import org.apache.ignite.internal.sql.command.SqlQualifiedName;

import java.util.List;

/**
 * Table with syntax {@code [<schema>.]<table_name> [ ( <col_name> [ , <col_name> ... ] ) ]} .
 */
public class BulkLoadLocationTable implements BulkLoadLocation {

    /** Schema name + table name. */
    private SqlQualifiedName tblQName;

    /** User-specified list of columns. */
    private List<String> columns;

    public BulkLoadLocationTable tableQualifiedName(SqlQualifiedName tblQName) {
        this.tblQName = tblQName;
        return this;
    }

    /**
     * Returns the schemaName.
     *
     * @return schemaName.
     */
    public String schemaName() {
        return tblQName.schemaName();
    }

    /**
     * @param schemaName Schema name.
     */
    public BulkLoadLocationTable schemaName(String schemaName) {
        tblQName.schemaName(schemaName);
        return this;
    }

    /**
     * Returns the table name.
     *
     * @return The table name
     */
    public String tableName() {
        return tblQName.name();
    }

    /**
     * Sets the table name
     *
     * @param tblName The table name.
     */
    public BulkLoadLocationTable tableName(String tblName) {
        tblQName.name(tblName);
        return this;
    }

    public SqlQualifiedName qualifiedName() {
        return tblQName;
    }

    /**
     * @return true if schema name is quoted, otherwise false.
     */
    public boolean isSchemaNameQuoted() {
        return tblQName.isSchemaNameQuoted();
    }

    /**
     * @return true if table name is quoted, otherwise false.
     */
    public boolean isTableNameQuoted() {
        return tblQName.isNameQuoted();
    }

    /**
     * Returns the list of columns.
     *
     * @return The list of columns.
     */
    public List<String> columns() {
        return columns;
    }

    public BulkLoadLocationTable columns(List<String> columns) {
        this.columns = columns;
        return this;
    }
}
