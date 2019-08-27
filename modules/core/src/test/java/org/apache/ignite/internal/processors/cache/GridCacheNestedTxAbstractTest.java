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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Nested transaction emulation.
 */
public class GridCacheNestedTxAbstractTest extends GridCommonAbstractTest {
    /** Counter key. */
    private static final String CNTR_KEY = "CNTR_KEY";

    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (int i = 0; i < GRID_CNT; i++) {
            grid(i).cache(DEFAULT_CACHE_NAME).removeAll();

            assert grid(i).cache(DEFAULT_CACHE_NAME).localSize() == 0;
        }
    }

    /**
     * Default constructor.
     *
     */
    protected GridCacheNestedTxAbstractTest() {
        super(false /** Start grid. */);
    }

    /**
     * JUnit.
     */
    @Test
    public void testTwoTx() {
        final IgniteCache<String, Integer> c = grid(0).cache(DEFAULT_CACHE_NAME);

        GridKernalContext ctx = grid(0).context();

        c.put(CNTR_KEY, 0);

        for (int i = 0; i < 10; i++) {
            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                c.get(CNTR_KEY);

                ctx.closure().callLocalSafe((new Callable<Boolean>() {
                    @Override public Boolean call() {
                        assertFalse(((IgniteInternalCache)c).context().tm().inUserTx());

                        assertNull(((IgniteInternalCache)c).context().tm().userTx());

                        return true;
                    }
                }), true);

                tx.commit();
            }
        }
    }
}
