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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Schema description.
 */
public class Schema {
    /** */
    private final List<Table> tbls = new ArrayList<>();

    /**
     * Adds table to the schema.
     *
     * @param tbl Table.
     */
    public void addTable(Table tbl) {
        A.ensure(tbl != null, "tbl != null");

        tbls.add(tbl);
    }

    /**
     * Returns all tables in current schema.
     *
     * @return Tables of current schema.
     */
    public List<Table> tables() {
        return tbls;
    }
}
