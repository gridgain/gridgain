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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Assert;
import org.junit.Test;

import javax.cache.CacheException;

/**
 * Tests check that second operation fail if it doesn't pass validation.
 */
public class TransactionValidationTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void validationOnRemoteNode() throws Exception {
        validationTest(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void validationOnLocalNode() throws Exception {
        validationTest(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void validationTest(boolean distributed) throws Exception {
        IgniteEx txCrd;

        if (distributed) {
            startGrid(0);
            startGrid(1);
            txCrd = startClientGrid(2);
        }
        else
            txCrd = startGrid(0);

        IgniteCache<Object, Object> cache0 = txCrd.createCache(
                new CacheConfiguration<>("cache0")
                        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
        );

        IgniteCache<Object, Object> cache1 = txCrd.createCache(
                new CacheConfiguration<>("cache1")
                        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                        .setTopologyValidator(nodes -> false)
        );

        try (Transaction tx = txCrd.transactions().txStart()) {
            cache0.put(1, 1);

            CacheException e = null;

            try {
                cache1.put(1, 1);
            } catch (CacheException e0) {
                e = e0;
            }

            Assert.assertTrue(e.getCause() instanceof CacheInvalidStateException);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }
}
