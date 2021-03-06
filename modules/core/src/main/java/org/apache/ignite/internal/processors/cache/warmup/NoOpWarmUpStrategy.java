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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.NoOpWarmUpConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Noop warming up strategy.
 */
public class NoOpWarmUpStrategy implements WarmUpStrategy<NoOpWarmUpConfiguration> {
    /** {@inheritDoc} */
    @Override public Class<NoOpWarmUpConfiguration> configClass() {
        return NoOpWarmUpConfiguration.class;
    }

    /** {@inheritDoc} */
    @Override public void warmUp(
        NoOpWarmUpConfiguration cfg,
        DataRegion region
    ) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NoOpWarmUpStrategy.class, this);
    }
}
