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

package org.apache.ignite.internal.sql.command;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * SQL qualified name.
 */
public class SqlQualifiedName {
    /** Schema name. */
    private String schemaName;

    /**
     * Schema name is quoted.
     */
    private boolean schemaNameQuoted;

    /** Object name. */
    private String name;

    /**
     * Object name is quoted.
     */
    private boolean nameQuoted;

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @param schemaName Schema name.
     * @return This instance.
     */
    public SqlQualifiedName schemaName(String schemaName) {
        this.schemaName = schemaName;

        return this;
    }

    /**
     * @param schemaName Schema name.
     * @param quoted is schema name quoted.
     * @return This instance.
     */
    public SqlQualifiedName schemaName(String schemaName, boolean quoted) {
        this.schemaName = schemaName;
        this.schemaNameQuoted = quoted;

        return this;
    }

    /**
     * @return Object name.
     */
    public String name() {
        return name;
    }

    /**
     * @param name Object name.
     * @return This instance.
     */
    public SqlQualifiedName name(String name) {
        this.name = name;

        return this;
    }

    /**
     * @param name Object name.
     * @param quoted Object name quoted.
     * @return This instance.
     */
    public SqlQualifiedName name(String name, boolean quoted) {
        this.name = name;
        this.nameQuoted = quoted;

        return this;
    }

    public boolean isSchemaNameQuoted() {
        return schemaNameQuoted;
    }

    public boolean isNameQuoted() {
        return nameQuoted;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlQualifiedName.class, this);
    }
}
