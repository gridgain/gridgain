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

package org.apache.ignite.internal.processors.cache.warmup;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;

/**
 * Warm-up strategy that only records which regions have visited it and how many times.
 */
class SimpleObservableWarmUpStrategy implements WarmUpStrategy<SimpleObservableWarmUpConfiguration> {
    /** Visited regions with a counter. */
    final Map<String, AtomicInteger> visitRegions = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public Class<SimpleObservableWarmUpConfiguration> configClass() {
        return SimpleObservableWarmUpConfiguration.class;
    }

    /** {@inheritDoc} */
    @Override public void warmUp(
        SimpleObservableWarmUpConfiguration cfg,
        DataRegion region
    ) throws IgniteCheckedException {
        visitRegions.computeIfAbsent(region.config().getName(), s -> new AtomicInteger()).incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteCheckedException {
        // No-op.
    }
}
