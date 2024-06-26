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

import java.util.Map;
import java.util.Set;
import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.client.ClientCache;

/** Reperesents Ignite cache operations required by Spring Data. */
public interface IgniteCacheProxy<K, V> extends Iterable<Cache.Entry<K, V>> {
    /**
     * Gets an entry from the cache.
     *
     * @param key the key whose associated value is to be returned
     * @return the element, or null, if it does not exist.
     */
    public V get(K key);

    /**
     * Associates the specified value with the specified key in the cache.
     *
     * @param key key with which the specified value is to be associated
     * @param val value to be associated with the specified key.
     */
    public void put(K key, V val);

    /**
     * Gets the number of all entries cached across all nodes.
     *
     * @param peekModes Optional peek modes. If not provided, then total cache size is returned.
     */
    public int size(CachePeekMode... peekModes);

    /**
     * Gets a collection of entries from the Ignite cache, returning them as
     * {@link Map} of the values associated with the set of keys requested.
     *
     * @param keys The keys whose associated values are to be returned.
     * @return A map of entries that were found for the given keys.
     */
    public Map<K, V> getAll(Set<? extends K> keys);

    /**
     * Copies all of the entries from the specified map to the Ignite cache.
     *
     * @param map Mappings to be stored in this cache.
     */
    public void putAll(Map<? extends K, ? extends V> map);

    /**
     * Removes the mapping for a key from this cache if it is present.
     *
     * @param key Key whose mapping is to be removed from the cache.
     * @return <tt>false</tt> if there was no matching key.
     */
    public boolean remove(K key);

    /**
     * Removes entries for the specified keys.
     *
     * @param keys The keys to remove.
     */
    public void removeAll(Set<? extends K> keys);

    /** Clears the contents of the cache. */
    public void clear();

    /**
     * Returns cache with the specified expired policy set. This policy will be used for each operation invoked on
     * the returned cache.
     *
     * @return Cache instance with the specified expiry policy set.
     */
    public IgniteCacheProxy<K, V> withExpiryPolicy(ExpiryPolicy expirePlc);

    /**
     * Execute SQL query and get cursor to iterate over results.
     *
     * @param qry SQL query.
     * @return Query result cursor.
     */
    public <R> QueryCursor<R> query(Query<R> qry);

    /**
     * Determines if the {@link ClientCache} contains an entry for the specified key.
     *
     * @param key key whose presence in this cache is to be tested.
     * @return <tt>true</tt> if this map contains a mapping for the specified key.
     */
    public boolean containsKey(K key);
}
