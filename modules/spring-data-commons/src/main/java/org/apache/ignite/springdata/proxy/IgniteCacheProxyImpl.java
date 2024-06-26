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

package org.apache.ignite.springdata.proxy;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientException;
import org.jetbrains.annotations.NotNull;

/** Implementation of {@link IgniteCacheProxy} that provides access to Ignite cache through {@link IgniteCache} instance. */
public class IgniteCacheProxyImpl<K, V> implements IgniteCacheProxy<K, V> {
    /** {@link IgniteCache} instance to which operations are delegated. */
    private final IgniteCache<K, V> cache;

    /** */
    public IgniteCacheProxyImpl(IgniteCache<K, V> cache) {
        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override public V get(K key) throws ClientException {
        return cache.get(key);
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) throws ClientException {
        cache.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode... peekModes) throws ClientException {
        return cache.size(peekModes);
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Set<? extends K> keys) throws ClientException {
        return cache.getAll(keys);
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> map) throws ClientException {
        cache.putAll(map);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) throws ClientException {
        return cache.remove(key);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Set<? extends K> keys) throws ClientException {
        cache.removeAll(keys);
    }

    /** {@inheritDoc} */
    @Override public void clear() throws ClientException {
        cache.clear();
    }

    /** {@inheritDoc} */
    @Override public IgniteCacheProxy<K, V> withExpiryPolicy(ExpiryPolicy expirePlc) {
        return new IgniteCacheProxyImpl<>(cache.withExpiryPolicy(expirePlc));
    }

    /** {@inheritDoc} */
    @Override public <R> QueryCursor<R> query(Query<R> qry) {
        return cache.query(qry);
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        return cache.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public @NotNull Iterator<Cache.Entry<K, V>> iterator() {
        return cache.<Cache.Entry<K, V>>query(new ScanQuery<>()).getAll().iterator();
    }

    /** @return {@link IgniteCache} instance to which operations are delegated. */
    public IgniteCache<K, V> delegate() {
        return cache;
    }
}
