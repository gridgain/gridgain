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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Test for put wrong value types to cache with indexed types.
 */
public class WrongIndexedTypesTest extends GridCommonAbstractTest {
    /** Failed. */
    private boolean failed;

    /** Transactional cache name. */
    private static final String TX_CACHE = "tx_cache";

    /** Atomic cache name. */
    private static final String ATOMIC_CACHE = "atomic_cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler((ignite, ctx) -> failed = true)
            .setCacheConfiguration(
                new CacheConfiguration<>(TX_CACHE).setAtomicityMode(TRANSACTIONAL)
                    .setIndexedTypes(String.class, String.class),
                new CacheConfiguration<>(ATOMIC_CACHE).setAtomicityMode(ATOMIC)
                    .setIndexedTypes(String.class, String.class));
    }

    /**
     *
     */
    @Test
    public void testPutIndexedType() throws Exception {
        Ignite ignite = startGrids(2);

        for (String cacheName : F.asList(TX_CACHE, ATOMIC_CACHE)) {
            for (int i = 0; i < 10; i++) {
                try {
                    ignite.cache(cacheName).put(i, "val" + i);

                    fail("Exception expected");
                }
                catch (Exception expected) {
                    // No-op.
                }
            }
        }

        assertFalse(failed);
    }
}
