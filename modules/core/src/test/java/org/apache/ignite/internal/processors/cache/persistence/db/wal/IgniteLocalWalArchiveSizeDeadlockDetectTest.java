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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.Repeat;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionHeuristicException;
import org.junit.Test;

/**
 * Class for checking deadlock detection when working with WAL archive.
 */
public class IgniteLocalWalArchiveSizeDeadlockDetectTest extends IgniteLocalWalArchiveSizeTest {
    /**
     * Checking detection of a deadlock when committing a large transaction, and fall of node ин FH.
     * Also, the maximum archive size must not be exceeded.
     *
     * @throws Exception If failed.
     */
    @Test
    @Repeat(10)
    @Override public void test() throws Exception {
        IgniteEx n = startGrid(0);

        GridTestUtils.assertThrows(log, () -> {
            try (Transaction tx = n.transactions().txStart()) {
                for (int i = 0; i < 1_0000; i++)
                    n.cache(DEFAULT_CACHE_NAME).put(i, new byte[(int)(100 * U.KB)]);

                tx.commit();
            }
        }, TransactionHeuristicException.class, "Committing a transaction has produced runtime exception");

        stopWatcher(watcher -> assertFalse(watcher.exceed));

        FailureContext failureCtx = n.context().failure().failureContext();
        assertNotNull(failureCtx);
        assertEquals(FailureType.CRITICAL_ERROR, failureCtx.type());

        Throwable error = failureCtx.error();
        assertTrue(error instanceof IgniteCheckedException);
        GridTestUtils.assertContains(log, error.getMessage(), "WAL archive is full and cannot be cleared");
    }
}
