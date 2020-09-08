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

package org.apache.ignite.compatibility.sql.randomsql;

import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Column description.
 */
public class Column {
    /** */
    private final String name;

    /** */
    private final Table tbl;

    /** */
    private final Class<?> typeCls;

    /**
     * @param name Name of the column.
     * @param typeClsName Type of the column.
     * @param tbl Table this column belongs to.
     */
    public Column(String name, String typeClsName, Table tbl) {
        this.name = name;
        this.tbl = tbl;

        typeCls = U.classForName(typeClsName, null);

        assert typeCls != null;
    }

    /**
     * Returns name of the column.
     *
     * @return Name of the column.
     */
    public String name() {
        return name;
    }

    /**
     * Returns type of the column.
     *
     * @return Type of the column.
     */
    public Class<?> typeClass() {
        return typeCls;
    }

    /**
     * Returns table this column belongs to.
     *
     * @return Table this column belongs to.
     */
    public Table table() {
        return tbl;
    }
}
