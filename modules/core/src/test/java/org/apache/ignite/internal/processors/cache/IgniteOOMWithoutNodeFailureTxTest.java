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
package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Arrays.asList;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests that put/putAll/replace to in-memory transactional cache doesn't cause IgniteOutOfMemoryException.
 */
@RunWith(Parameterized.class)
public class IgniteOOMWithoutNodeFailureTxTest extends IgniteOOMWithoutNodeFailureAbstractTest {
    /** */
    @Parameterized.Parameter(value = 0)
    public TransactionConcurrency concurrency;

    /** */
    @Parameterized.Parameter(value = 1)
    public TransactionIsolation isolation;

    /** */
    @Parameterized.Parameters(name = "concurrency={0}, isolation={1}")
    public static Collection<Object[]> params() {
        return asList(
            new Object[] { OPTIMISTIC, READ_COMMITTED },
            new Object[] { OPTIMISTIC, REPEATABLE_READ },
            new Object[] { OPTIMISTIC, SERIALIZABLE },
            new Object[] { PESSIMISTIC, READ_COMMITTED },
            new Object[] { PESSIMISTIC, REPEATABLE_READ },
            new Object[] { PESSIMISTIC, SERIALIZABLE }
        );
    }

    /** Tests that put/putAll/replace to in-memory transactional cache doesn't cause IgniteOutOfMemoryException. */
    @Test
    public void testTxCache() throws Exception {
        testIgniteOOMWithoutNodeFailure(ignite.cache(TX_CACHE_NAME), op -> {
            try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                op.run();

                tx.commit();
            }
        });
    }
}
