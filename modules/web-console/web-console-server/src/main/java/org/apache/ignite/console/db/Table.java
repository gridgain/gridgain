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

package org.apache.ignite.console.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.console.dto.AbstractDto;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.toSet;

/**
 * Table for data objects.
 *
 * @param <T> Type of DTO.
 */
public class Table<T extends AbstractDto> extends CacheHolder<UUID, T> {
    /** Unique indexes. */
    private final List<UniqueIndex<T>> uniqueIndexes = new ArrayList<>();

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    public Table(Ignite ignite, String cacheName) {
        super(ignite, cacheName);
    }

    /**
     * @param keyGenerator Key generator.
     * @param msgGenerator Message generator.
     */
    public Table<T> addUniqueIndex(Function<T, Object> keyGenerator, Function<T, String> msgGenerator) {
        uniqueIndexes.add(new UniqueIndex<>(keyGenerator, msgGenerator));

        return this;
    }

    /** */
    private IgniteCache<Object, UUID> indexCache() {
        IgniteCache c = cache;

        return (IgniteCache<Object, UUID>)c;
    }

    /**
     * @param id ID.
     * @return {@code true} If table contains specified key.
     */
    public boolean contains(UUID id) {
        return cache.containsKey(id);
    }

    /**
     * @param id ID.
     * @return DTO.
     */
    @Nullable public T load(UUID id) {
        return cache.get(id);
    }

    /**
     * @param key Indexed key.
     * @return DTO.
     */
    @Nullable public T getByIndex(Object key) {
        UUID id = indexCache().get(key);

        if (id == null)
            return null;

        return cache.get(id);
    }

    /**
     * @return Collection of DTOs.
     */
    public List<T> loadAll() throws IgniteCheckedException {
        Iterator<Cache.Entry<UUID, T>> it = internalCache().scanIterator(false, DTOPredicate.INSTANCE);
        
        ArrayList<T> res = new ArrayList<>();

        it.forEachRemaining(entry -> res.add(entry.getValue()));

        return res;
    }

    /**
     * @param keys Indexed keys.
     * @return Collection of DTOs.
     */
    public Collection<T> loadAllByIndex(Set<?> keys) {
        Map<?, UUID> ids = indexCache().getAll(keys);

        if (ids.isEmpty())
            return Collections.emptyList();

        return loadAll(new HashSet<>(ids.values()));
    }

    /**
     * @param ids IDs.
     * @return Collection of DTOs.
     */
    public Collection<T> loadAll(Set<UUID> ids) {
        Map<UUID, T> res = cache.getAll(ids);

        return F.isEmpty(res) ? Collections.emptyList() : res.values();
    }

    /**
     * @param val DTO.
     * @return Saved DTO.
     */
    public T save(T val) throws IgniteException {
        UUID id = val.getId();

        T oldVal = cache.getAndPut(id, val);

        for (UniqueIndex<T> idx : uniqueIndexes) {
            Object newIdxKey = idx.key(val);

            UUID oldId = indexCache().getAndPutIfAbsent(newIdxKey, id);

            if (oldId != null && !id.equals(oldId))
                throw new IgniteException(idx.message(val));

            if (oldVal != null && !idx.key(oldVal).equals(newIdxKey))
                indexCache().remove(idx.key(oldVal));
        }

        return val;
    }

    /**
     * @param map Map of DTOs.
     */
    public void saveAll(Map<UUID, T> map) throws IgniteException {
        if (F.isEmpty(uniqueIndexes)) {
            cache.putAll(map);
            
            return;
        }

        for (T item : map.values())
            save(item);
    }

    /**
     * @param id ID.
     * @return Previous value.
     */
    @Nullable public T delete(UUID id) {
        T val = cache.getAndRemove(id);

        indexCache().removeAll(uniqueIndexes.stream().map(idx -> idx.key(val)).collect(toSet()));

        return val;
    }

    /**
     * @param ids IDs.
     */
    public void deleteAll(Set<UUID> ids) {
        Set<Object> idxIds = ids.stream()
            .map(cache::getAndRemove)
            .flatMap((payload) -> uniqueIndexes.stream().map(idx -> idx.key(payload)))
            .collect(toSet());

        indexCache().removeAll(idxIds);
    }

    /**
     * Queries cache.
     *
     * @param qry Query to execute.
     * @return Cursor.
     */
     public <R> QueryCursor<R> query(Query<R> qry) {
        return cache.query(qry);
    }

    /**
     * Index for unique constraint.
     */
    private static class UniqueIndex<T> {
        /** */
        private final Function<T, Object> keyGenerator;

        /** */
        private final Function<T, String> msgGenerator;

        /**
         * Constructor.
         */
        UniqueIndex(Function<T, Object> keyGenerator, Function<T, String> msgGenerator) {
            this.keyGenerator = keyGenerator;
            this.msgGenerator = msgGenerator;
        }

        /**
         * @param val Value.
         * @return Unique key.
         */
        public Object key(T val) {
            return keyGenerator.apply(val);
        }

        /**
         * @param val Value.
         */
        public String message(T val) {
            return msgGenerator.apply(val);
        }
    }

    /**
     * Filter that keeps DTO objects and drops non DTO objects.
     */
    private static class DTOPredicate implements IgniteBiPredicate<Object, Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private static final DTOPredicate INSTANCE = new DTOPredicate();

        /** {@inheritDoc} */
        @Override public boolean apply(Object key, Object val) {
            return val instanceof AbstractDto;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DTOPredicate.class, this);
        }
    }
}
