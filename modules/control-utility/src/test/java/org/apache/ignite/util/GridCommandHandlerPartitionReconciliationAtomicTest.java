/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.util;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Tests for checking partition reconciliation control.sh command within context of atomic cache.
 */
public class GridCommandHandlerPartitionReconciliationAtomicTest extends
    GridCommandHandlerPartitionReconciliationAbstractTest {
    /** {@inheritDoc} */
    @Override protected void prepareCache() {
        ignite.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 16))
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(3));

        try (IgniteDataStreamer streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < INVALID_KEY; i++)
                streamer.addData(i, VALUE_PREFIX + i);
        }
    }
}
