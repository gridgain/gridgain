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
package org.apache.ignite.springdata22.repository.support;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.springdata.proxy.IgniteCacheProxy;
import org.apache.ignite.springdata.proxy.IgniteCacheProxyImpl;
import org.apache.ignite.springdata.proxy.IgniteProxy;
import org.apache.ignite.springdata.proxy.IgniteProxyImpl;
import org.apache.ignite.springdata22.repository.IgniteRepository;
import org.apache.ignite.springdata22.repository.config.RepositoryConfig;
import org.jetbrains.annotations.Nullable;
import org.springframework.context.annotation.Conditional;

/**
 * General Apache Ignite repository implementation. This bean should've never been loaded by context directly, only via
 * {@link IgniteRepositoryFactory}
 *
 * @param <V> the cache value type
 * @param <K> the cache key type
 * @author Apache Ignite Team
 * @author Manuel Núñez (manuel.nunez@hawkore.com)
 */
@Conditional(ConditionFalse.class)
public class IgniteRepositoryImpl<V, K extends Serializable> implements IgniteRepository<V, K> {
    /** Error message indicating that operation is supported only if {@link Ignite} instance is used to access the cluster. */
    private static final String UNSUPPORTED_ERR_MSG = "Current operation is supported only if Ignite node instance is" +
        " used to access the Ignite cluster. See " + RepositoryConfig.class.getName() + "#igniteInstance.";

    /**
     * Ignite Cache bound to the repository
     */
    private final IgniteCacheProxy<K, V> cache;

    /**
     * Ignite instance bound to the repository
     */
    private final IgniteProxy ignite;

    /**
     * Repository constructor.
     *
     * @param ignite the ignite
     * @param cache  Initialized cache instance.
     */
    public IgniteRepositoryImpl(IgniteProxy ignite, IgniteCacheProxy<K, V> cache) {
        this.cache = cache;
        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> cache() {
        if (cache instanceof IgniteCacheProxyImpl)
            return ((IgniteCacheProxyImpl<K, V>)cache).delegate();

        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MSG);
    }

    /** {@inheritDoc} */
    @Override public Ignite ignite() {
        if (ignite instanceof IgniteProxyImpl)
            return ((IgniteProxyImpl)ignite).delegate();

        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MSG);
    }

    /** {@inheritDoc} */
    @Override public <S extends V> S save(K key, S entity) {
        cache.put(key, entity);

        return entity;
    }

    /** {@inheritDoc} */
    @Override public <S extends V> Iterable<S> save(Map<K, S> entities) {
        cache.putAll(entities);

        return entities.values();
    }

    /** {@inheritDoc} */
    @Override public <S extends V> S save(K key, S entity, @Nullable ExpiryPolicy expiryPlc) {
        if (expiryPlc != null)
            cache.withExpiryPolicy(expiryPlc).put(key, entity);
        else
            cache.put(key, entity);
        return entity;
    }

    /** {@inheritDoc} */
    @Override public <S extends V> Iterable<S> save(Map<K, S> entities, @Nullable ExpiryPolicy expiryPlc) {
        if (expiryPlc != null)
            cache.withExpiryPolicy(expiryPlc).putAll(entities);
        else
            cache.putAll(entities);
        return entities.values();
    }

    /**
     * Not implemented.
     */
    @Override public <S extends V> S save(S entity) {
        throw new UnsupportedOperationException("Use IgniteRepository.save(key,value) method instead.");
    }

    /**
     * Not implemented.
     */
    @Override public <S extends V> Iterable<S> saveAll(Iterable<S> entities) {
        throw new UnsupportedOperationException("Use IgniteRepository.save(Map<keys,value>) method instead.");
    }

    /** {@inheritDoc} */
    @Override public Optional<V> findById(K id) {
        return Optional.ofNullable(cache.get(id));
    }

    /** {@inheritDoc} */
    @Override public boolean existsById(K id) {
        return cache.containsKey(id);
    }

    /** {@inheritDoc} */
    @Override public Iterable<V> findAll() {
        final Iterator<Cache.Entry<K, V>> iter = cache.iterator();

        return new Iterable<V>() {
            /** */
            @Override public Iterator<V> iterator() {
                return new Iterator<V>() {
                    /** {@inheritDoc} */
                    @Override public boolean hasNext() {
                        return iter.hasNext();
                    }

                    /** {@inheritDoc} */
                    @Override public V next() {
                        return iter.next().getValue();
                    }

                    /** {@inheritDoc} */
                    @Override public void remove() {
                        iter.remove();
                    }
                };
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Iterable<V> findAllById(Iterable<K> ids) {
        if (ids instanceof Set)
            return cache.getAll((Set<K>)ids).values();

        if (ids instanceof Collection)
            return cache.getAll(new HashSet<>((Collection<K>)ids)).values();

        TreeSet<K> keys = new TreeSet<>();

        for (K id : ids)
            keys.add(id);

        return cache.getAll(keys).values();
    }

    /** {@inheritDoc} */
    @Override public long count() {
        return cache.size(CachePeekMode.PRIMARY);
    }

    /** {@inheritDoc} */
    @Override public void deleteById(K id) {
        cache.remove(id);
    }

    /** {@inheritDoc} */
    @Override public void delete(V entity) {
        throw new UnsupportedOperationException("Use IgniteRepository.deleteById(key) method instead.");
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(Iterable<? extends V> entities) {
        throw new UnsupportedOperationException("Use IgniteRepository.deleteAllById(keys) method instead.");
    }

    /** {@inheritDoc} */
    @Override public void deleteAllById(Iterable<? extends K> ids) {
        if (ids instanceof Set) {
            cache.removeAll((Set<K>)ids);
            return;
        }

        if (ids instanceof Collection) {
            cache.removeAll(new HashSet<>((Collection<K>)ids));
            return;
        }

        TreeSet<K> keys = new TreeSet<>();

        for (K id : ids)
            keys.add(id);

        cache.removeAll(keys);
    }

    /** {@inheritDoc} */
    @Override public void deleteAll() {
        cache.clear();
    }
}
