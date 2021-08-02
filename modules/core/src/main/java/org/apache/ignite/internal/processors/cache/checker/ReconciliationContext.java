/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.checker;

import java.util.Iterator;
import java.util.function.BiFunction;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.util.KeyComparator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Context for a cache size reconciliation.
 * All fields use {@link Map} because a partition contains data from several caches in case of cache group.
 * The keys of such map is a cache ID.
 * If a partition contains data from an only one cache, then {@link GridCacheUtils#UNDEFINED_CACHE_ID} is used as a key.
 */
public class ReconciliationContext {
    /** */
    public static final KeyComparator KEY_COMPARATOR = new KeyComparator();

    /** */
    public enum SizeReconciliationState {
        /** */
        NOT_STARTED,

        /** */
        IN_PROGRESS,

        /** */
        FINISHED
    }

    /**
     * Reconciliation state.
     */
    private final Map<Integer, SizeReconciliationState> sizeReconciliationState = new ConcurrentHashMap<>();

    /**
     * The last key in the last page that was readed by reconciliation cursor.
     */
    private final Map<Integer, KeyCacheObject> lastKeys = new ConcurrentHashMap<>();

    /**
     * Size that evensually will be equals the real size of partition.
     */
    private final Map<Integer, AtomicLong> sizes = new ConcurrentHashMap<>();

    /**
     * A temp storage for keys that reconciliation util need to count.
     * The temp storage is cleared after each a partition page is readed.
     */
    private final Map<Integer, Map<KeyCacheObject, Boolean>> cachesKeysMap = new ConcurrentHashMap<>();

    /**
     * @param cacheId Cache ID.
     * @return State of reconciliation.
     */
    public SizeReconciliationState sizeReconciliationState(int cacheId) {
        return sizeReconciliationState.get(cacheId);
    }

    /**
     * @param cacheId Cache ID.
     * @param sizeReconciliationState State of reconciliation.
     */
    public void sizeReconciliationState(int cacheId, SizeReconciliationState sizeReconciliationState) {
        this.sizeReconciliationState.put(cacheId, sizeReconciliationState);
    }

    /**
     * @param cacheId Cache ID.
     * @return Last key.
     */
    public KeyCacheObject lastKey(int cacheId) {
        return lastKeys.get(cacheId);
    }

    /**
     * @param cacheId Cache ID.
     * @param key Key.
     */
    public void lastKey(int cacheId, KeyCacheObject key) {
        lastKeys.put(cacheId, key);
    }

    /**
     * @param cacheId Cache ID.
     * @return Cache size.
     */
    public AtomicLong size(int cacheId) {
        return sizes.get(cacheId);
    }

    /**
     * @param cacheId Cache ID.
     * @return Cache size.
     */
    public AtomicLong finishSize(int cacheId) {
        return sizes.remove(cacheId);
    }

    /**
     * @param cacheId Cache ID.
     * @param size Cache size.
     */
    public void size(int cacheId, long size) {
        sizes.get(cacheId).set(size);
    }

    /**
     * @param cacheId Cache ID.
     */
    public void resetSize(int cacheId) {
        sizes.put(cacheId, new AtomicLong());
    }

    /**
     * @return {@code true} if size reconciliation finished for all caches in cache group.
     */
    public boolean partSizeIsFinished() {
        return sizes.isEmpty();
    }

    /**
     * @param cacheId Cache ID.
     */
    public void resetCacheKeysMap(Integer cacheId) {
        cachesKeysMap.put(cacheId, new ConcurrentHashMap<>());
    }

    /**
     * @param cacheId Cache ID.
     */
    public void removeCacheKeysMap(Integer cacheId) {
        cachesKeysMap.remove(cacheId);
    }

    /**
     * @param cacheId Cache ID.
     * @param key Key.
     */
    public void putCacheKeyMap(Integer cacheId, KeyCacheObject key) {
        cachesKeysMap.get(cacheId).put(key, true);
    }

    /**
     * @param cacheId Cache ID.
     * @return Iterator for temp storage of keys.
     */
    public Iterator<Map.Entry<KeyCacheObject, Boolean>> getCacheKeysIterator(Integer cacheId) {
        return cachesKeysMap.get(cacheId).entrySet().iterator();
    }

    /**
     * @param cacheId Cache ID.
     * @param key Key.
     * @param c Closure.
     */
    public void computeCacheKeysMap(Integer cacheId, KeyCacheObject key, BiFunction<KeyCacheObject, Boolean, Boolean> c) {
        cachesKeysMap.get(cacheId).compute(key, c);
    }

    /**
     * @param cacheId Cache ID.
     * @param key Key.
     * @param c Closure.
     */
    public void computeIfPresentCacheKeysMap(Integer cacheId, KeyCacheObject key, BiFunction<KeyCacheObject, Boolean, Boolean> c) {
        cachesKeysMap.get(cacheId).computeIfPresent(key, c);
    }

    /**
     * @param cacheId Cache ID.
     * @param newMap New map.
     */
    public void putAllCacheKeysMap(Integer cacheId, Map<KeyCacheObject, Boolean> newMap) {
        cachesKeysMap.get(cacheId).putAll(newMap);
    }
}
