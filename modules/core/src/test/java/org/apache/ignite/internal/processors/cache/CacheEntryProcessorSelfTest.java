/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

/** */
public class CacheEntryProcessorSelfTest extends GridCommonAbstractTest {
    /**
     * Tests a get operation after an invoke operation which removed the entry in the same transaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAfterRemoveInInvoke() throws Exception {
        try (Ignite ignite = startGrid(0)) {

            CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<Integer, Integer>("CACHE_1")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheCfg);

            int key = 1;
            int val = 10;

            cache.put(key, val);

            try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC,
                TransactionIsolation.SERIALIZABLE, 5000, 0)) {

                removeAndGetEntryInInvoke(cache, key);

                tx.rollback();
            }

            assertNotNull(cache.get(key));
            assertEquals(val, (int) cache.get(key));

            try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC,
                TransactionIsolation.SERIALIZABLE, 5000, 0)) {

                removeAndGetEntryInInvoke(cache, key);

                tx.commit();
            }

            assertNull(cache.get(key));
        }
    }

    private void removeAndGetEntryInInvoke(IgniteCache<Integer, Integer> cache, int key) {
        cache.invoke(key, (entry, object) -> {
            Integer oldValue = entry.getValue();

            entry.remove();

            return oldValue;
        });

        assertNull(cache.get(key));
    }
}
