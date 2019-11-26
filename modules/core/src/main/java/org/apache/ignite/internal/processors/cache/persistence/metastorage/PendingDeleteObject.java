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
package org.apache.ignite.internal.processors.cache.persistence.metastorage;

import java.io.Serializable;
import org.apache.ignite.internal.util.GridStringBuilder;

import static org.apache.ignite.internal.processors.cache.persistence.metastorage.PendingDeleteObjectType.SQL_INDEX;
import static org.apache.ignite.internal.processors.cache.persistence.metastorage.PendingDeleteObjectType.SQL_TABLE;

public class PendingDeleteObject implements Serializable {
    /** */
    private PendingDeleteObjectType type;

    /** */
    private String name;

    /** */
    private String cacheName;

    /** */
    private String schemaName;

    /** */
    private String tableName;

    /** */
    public PendingDeleteObject() {}

    /** */
    public PendingDeleteObject(PendingDeleteObjectType type, String name, String cacheName, String schemaName, String tableName) {
        this.type = type;
        this.name = name;
        this.cacheName = cacheName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public PendingDeleteObjectType type() {
        return type;
    }

    public String name() {
        return name;
    }

    public String cacheName() {
        return cacheName;
    }

    public String schemaName() {
        return schemaName;
    }

    private boolean isSql() {
        return type == SQL_TABLE || type == SQL_INDEX;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return new GridStringBuilder(type.toString()).a("-")
            .a(isSql() ? schemaName + "." + name : cacheName)
            .toString();
    }
}
