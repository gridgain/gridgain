/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.cache.store.jdbc;

import org.apache.ignite.cache.store.jdbc.dialect.JdbcDialect;
import org.apache.ignite.cache.store.jdbc.dialect.MariaDBDialect;

import javax.cache.CacheException;

/**
 * A {@link CacheJdbcPojoStore} extension that resolves to {@link MariaDBDialect} by default.
 * Suitable for caches backed by MariaDB; functionally equivalent to wiring
 * {@link CacheJdbcPojoStore} with {@code setDialect(new MariaDBDialect())}.
 */
public class MariaDBCacheStore<K, V> extends CacheJdbcPojoStore<K, V> {
    /** {@inheritDoc} */
    @Override protected JdbcDialect resolveDialect() throws CacheException {
        return new MariaDBDialect();
    }
}
