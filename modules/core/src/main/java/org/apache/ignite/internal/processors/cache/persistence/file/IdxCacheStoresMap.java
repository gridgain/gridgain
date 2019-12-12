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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Proxy class for {@link FilePageStoreManager#idxCacheStores} map that wraps data adding and replacing
 * operations to disallow concurrent execution simultaneously with cleanup of file page storage. Wrapping
 * of data removing operations is not needed.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class IdxCacheStoresMap<K, V> extends ConcurrentHashMap<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Executor that wraps data adding and replacing operations.
     */
    private final LongOperationAsyncExecutor longOperationAsyncExecutor;

    /**
     * Default constructor.
     *
     * @param longOperationAsyncExecutor executor that wraps data adding and replacing operations.
     */
    IdxCacheStoresMap(LongOperationAsyncExecutor longOperationAsyncExecutor) {
        this.longOperationAsyncExecutor = longOperationAsyncExecutor;
    }

    /** {@inheritDoc} */
    @Override public V put(K key, V val) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.put(key, val));
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> m) {
        longOperationAsyncExecutor.afterAsyncCompletion(() -> {
            super.putAll(m);

            return null;
        });
    }

    /** {@inheritDoc} */
    @Override public V putIfAbsent(K key, V val) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.putIfAbsent(key, val));
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.replace(key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override public V replace(K key, V val) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.replace(key, val));
    }

    /** {@inheritDoc} */
    @Override public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.computeIfAbsent(key, mappingFunction));
    }

    /** {@inheritDoc} */
    @Override public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.computeIfPresent(key, remappingFunction));
    }

    /** {@inheritDoc} */
    @Override public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.compute(key, remappingFunction));
    }

    /** {@inheritDoc} */
    @Override public V merge(K key, V val, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.merge(key, val, remappingFunction));
    }
}