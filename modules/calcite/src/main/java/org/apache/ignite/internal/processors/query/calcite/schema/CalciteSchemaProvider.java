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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.schema.SchemaChangeListener;

/**
 *
 */
public class CalciteSchemaProvider implements SchemaChangeListener, SchemaProvider {
    private final Map<String, IgniteSchema> schemas = new HashMap<>();

    private SchemaPlus schema;
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    @Override public SchemaPlus schema() {
        lock.readLock().lock();
        try {
            return Objects.requireNonNull(schema);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override public void onSchemaCreate(String schemaName) {
        lock.writeLock().lock();
        try {
            schemas.putIfAbsent(schemaName, new IgniteSchema(schemaName));

            SchemaPlus schema = Frameworks.createRootSchema(false);

            schemas.forEach(schema::add);

            this.schema = schema;
        } finally {
            lock.writeLock().unlock();
        }

    }

    @Override public void onSchemaDrop(String schemaName) {
        lock.writeLock().lock();
        try {
            schemas.remove(schemaName);

            SchemaPlus schema = Frameworks.createRootSchema(false);

            schemas.forEach(schema::add);

            this.schema = schema;
        } finally {
            lock.writeLock().unlock();
        }

    }

    @Override public void onSqlTypeCreate(String schemaName, GridQueryTypeDescriptor typeDescriptor, GridCacheContextInfo cacheInfo) {
        lock.writeLock().lock();
        try {
            schemas.computeIfAbsent(schemaName, IgniteSchema::new).onSqlTypeCreate(typeDescriptor, cacheInfo);

            SchemaPlus schema = Frameworks.createRootSchema(false);

            schemas.forEach(schema::add);

            this.schema = schema;
        } finally {
            lock.writeLock().unlock();
        }

    }

    @Override public void onSqlTypeDrop(String schemaName, GridQueryTypeDescriptor typeDescriptor, GridCacheContextInfo cacheInfo) {
        lock.writeLock().lock();
        try {
            schemas.computeIfAbsent(schemaName, IgniteSchema::new).onSqlTypeCreate(typeDescriptor, cacheInfo);

            SchemaPlus schema = Frameworks.createRootSchema(false);

            schemas.forEach(schema::add);

            this.schema = schema;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
