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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.QueryEntity;

/**
 * Table description.
 */
public class Table {
    /** */
    private final String name;

    /** */
    private final LinkedHashMap<String, Column> cols = new LinkedHashMap<>();

    /**
     * @param entity Query entity description.
     */
    public Table(QueryEntity entity) {
        name = entity.getTableName();

        for (Map.Entry<String, String> f : entity.getFields().entrySet()) {
            Column col = new Column(f.getKey(), f.getValue(), this);

            cols.put(col.name(), col);
        }
    }

    /**
     * Returns all columns of the table.
     *
     * @return Column list.
     */
    public List<Column> columnsList() {
        return new ArrayList<>(cols.values());
    }

    /**
     * Returns name of the table.
     *
     * @return name of the table.
     */
    public String name() {
        return name;
    }
}
