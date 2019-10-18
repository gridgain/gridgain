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

import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.cluster.ClusterState.READ_ONLY;
import static org.apache.ignite.internal.processors.cache.ClusterStateTestUtils.FAILED_ACTIVATE_MSG;
import static org.apache.ignite.internal.processors.cache.ClusterStateTestUtils.FAILED_DEACTIVATE_MSG;
import static org.apache.ignite.internal.processors.cache.ClusterStateTestUtils.FAILED_READ_ONLY_MSG;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/**
 *
 */
public abstract class ClusterStateServerAbstractTest extends ClusterStateAbstractTest {
    /**
     * Tests that deactivation is prohibited if explicit lock is held in current thread.
     *
     */
    @Test
    public void testDeactivationWithPendingLock() {
        changeClusterStateWithPendingLock(INACTIVE, FAILED_DEACTIVATE_MSG);
    }

    /**
     * Tests that enabling read-only mode is prohibited if explicit lock is held in current thread.
     *
     */
    @Test
    public void testReadOnlyWithPendingLock() {
        changeClusterStateWithPendingLock(READ_ONLY, FAILED_READ_ONLY_MSG);
    }

    /**
     * Tests that change cluster mode from {@link ClusterState#ACTIVE} to {@link ClusterState#INACTIVE} is prohibited
     * if transaction is active in current thread.
     *
     */
    @Test
    public void testDeactivationWithPendingTransaction() {
        grid(0).cluster().state(ACTIVE);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values())
                changeStateWithPendingTransaction(INACTIVE, concurrency, isolation, FAILED_DEACTIVATE_MSG);
        }
    }

    /**
     * Tests that change cluster mode from {@link ClusterState#READ_ONLY} to {@link ClusterState#INACTIVE} is prohibited
     * if transaction is active in current thread.
     *
     */
    @Test
    public void testDeactivateFromReadonlyWithPendingTransaction() {
        grid(0).cluster().state(READ_ONLY);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values())
                changeStateWithPendingTransaction(INACTIVE, concurrency, isolation, FAILED_DEACTIVATE_MSG);
        }
    }

    /**
     * Tests that change cluster mode from {@link ClusterState#ACTIVE} to {@link ClusterState#READ_ONLY} is prohibited
     * if transaction is active in current thread.
     *
     */
    @Test
    public void testReadOnlyWithPendingTransaction() {
        grid(0).cluster().state(ACTIVE);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values())
                changeStateWithPendingTransaction(READ_ONLY, concurrency, isolation, FAILED_READ_ONLY_MSG);
        }
    }

    /**
     * Tests that change cluster mode from {@link ClusterState#READ_ONLY} to {@link ClusterState#ACTIVE} is prohibited
     * if transaction is active in current thread.
     *
     */
    @Test
    public void testDisableReadonlyWithPendingTransaction() {
        grid(0).cluster().state(READ_ONLY);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values())
                changeStateWithPendingTransaction(ACTIVE, concurrency, isolation, FAILED_ACTIVATE_MSG);
        }
    }

    /**
     */
    @Test
    public void testDynamicCacheStart() {
        grid(0).cluster().state(ACTIVE);

        try {
            IgniteCache<Object, Object> cache2 = grid(0).createCache(new CacheConfiguration<>("cache2"));

            for (int k = 0; k < ENTRY_CNT; k++)
                cache2.put(k, k);
        }
        finally {
            grid(0).destroyCache("cache2");
        }
    }

    /** */
    private void changeStateWithPendingTransaction(
        ClusterState state,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        String exceptionMsg
    ) {
        final IgniteCache<Object, Object> cache0 = grid(0).cache(CACHE_NAME);

        assertNotSame(state, grid(0).cluster().state());

        try (Transaction ignore = grid(0).transactions().txStart(concurrency, isolation)) {
            if (grid(0).cluster().state() != READ_ONLY)
                cache0.put(1, "1");

            //noinspection ThrowableNotThrown
            assertThrowsAnyCause(log, ClusterStateTestUtils.changeState(grid(0), state), IgniteException.class, exceptionMsg);
        }

        assertNotSame(state, grid(0).cluster().state());

        if (grid(0).cluster().state() != READ_ONLY)
            assertNull(cache0.get(1));

        assertNull(grid(0).transactions().tx());
    }

    /**
     * Tests that cluster state change to {@code newState} is prohibited if explicit lock is held in current thread.
     *
     * @param newState New cluster state.
     * @param exceptionMsg Exception message.
     */
    private void changeClusterStateWithPendingLock(ClusterState newState, String exceptionMsg) {
        grid(0).cluster().state(ACTIVE);

        Lock lock = grid(0).cache(CACHE_NAME).lock(1);

        lock.lock();

        try {
            //noinspection ThrowableNotThrown
            assertThrowsAnyCause(log, ClusterStateTestUtils.changeState(grid(0), newState), IgniteException.class, exceptionMsg);
        }
        finally {
            lock.unlock();
        }
    }
}
