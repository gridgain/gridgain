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

package org.apache.ignite.configuration;

import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * "Load All" warm-up configuration that loads data into persistent data region
 * until it reaches {@link DataRegionConfiguration#getMaxSize} with index priority.
 */
public class LoadAllWarmUpConfiguration implements WarmUpConfiguration {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Default thread count. */
    public static final int DFLT_THREADS = Math.max(8, Runtime.getRuntime().availableProcessors());

    /** Number of threads used to perform warmup in parallel. */
    private int threads = DFLT_THREADS;

    /**
     * @return Number of threads used to load pages in parallel. Falls back to {@link #DFLT_THREADS}.
     */
    public int getThreads() {
        return threads > 0 ? threads : DFLT_THREADS;
    }

    /**
     * Sets the number of threads used to load pages in parallel. Must be positive.
     *
     * @param threads Number of threads.
     * @return {@code this} for chaining.
     */
    public LoadAllWarmUpConfiguration setThreads(int threads) {
        A.ensure(threads > 0, "threads must be positive");

        this.threads = threads;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(LoadAllWarmUpConfiguration.class, this);
    }
}
