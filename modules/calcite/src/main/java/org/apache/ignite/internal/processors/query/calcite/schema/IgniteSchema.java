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

package org.apache.ignite.internal.processors.query.calcite.schema;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;

/**
 *
 */
public class IgniteSchema extends AbstractSchema {
    /** */
    private final String schemaName;

    /** */
    private final Map<String, Table> tableMap = new ConcurrentHashMap<>();

    public IgniteSchema(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getName() {
        return schemaName;
    }

    @Override protected Map<String, Table> getTableMap() {
        return Collections.unmodifiableMap(tableMap);
    }

    public void onSqlTypeCreate(GridQueryTypeDescriptor typeDescriptor, GridCacheContextInfo cacheInfo) {
        tableMap.putIfAbsent(typeDescriptor.tableName(), new IgniteTable(typeDescriptor, cacheInfo));
    }

    public void onSqlTypeDrop(GridQueryTypeDescriptor typeDescriptor, GridCacheContextInfo cacheInfo) {
        tableMap.remove(typeDescriptor.tableName());
    }
}
