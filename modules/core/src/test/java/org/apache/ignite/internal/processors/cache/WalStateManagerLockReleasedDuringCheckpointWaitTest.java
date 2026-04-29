/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Verifies that {@link WalStateManager#onProposeExchange} does not
 * hold {@code mux} while waiting on the checkpoint future, so that {@code disco-notifier-worker}
 * can continue to process incoming WAL state discovery messages during a checkpoint wait.
 */
public class WalStateManagerLockReleasedDuringCheckpointWaitTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "c1";

    /** Cache group name. */
    private static final String GROUP_NAME = "grp1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(256L * 1024 * 1024)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60_000;
    }

    /**
     * Holds the checkpoint read lock so the WAL-state-change checkpoint cannot complete its mark
     * phase, then asserts that {@link WalStateManager#mux} can still be acquired by an unrelated
     * thread.
     */
    @Test
    public void testMuxNotHeldDuringCheckpointMarkWait() throws Exception {
        IgniteEx node = startGrid(0);
        node.cluster().state(ClusterState.ACTIVE);

        node.createCache(new CacheConfiguration<Integer, Integer>(CACHE_NAME)
            .setGroupName(GROUP_NAME)
            .setQueryEntities(java.util.Collections.singletonList(
                new QueryEntity(Integer.class, Integer.class))));

        node.cache(CACHE_NAME).put(1, 1);

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)
            node.context().cache().context().database();

        // Drain any in-flight checkpoint so the next one is the wal-state-change one.
        db.waitForCheckpoint("test-pre");

        // Hold the checkpoint READ lock so the wal-state-change checkpoint's WRITE lock
        // acquisition blocks indefinitely.
        db.checkpointReadLock();

        IgniteInternalFuture<Boolean> walDisableFut = null;

        try {
            // Trigger a real WAL-disable operation; the exchange-worker enters
            // onProposeExchange, calls triggerCheckpoint, then parks on
            // cpFut.futureFor(LOCK_RELEASED).get().
            walDisableFut = GridTestUtils.runAsync(() -> node.cluster().disableWal(CACHE_NAME));

            // Give the exchange-worker time to reach the wait. If this is too short, the next
            // assertion may falsely pass on broken code; we therefore also assert that
            // walDisableFut is still in flight, proving the exchange-worker is parked.
            Thread.sleep(2_000);

            assertFalse(
                "WAL disable completed before the test could observe the wait — " +
                    "checkpoint read lock did not block the mark phase as expected",
                walDisableFut.isDone()
            );

            // Reflectively access mux.
            WalStateManager wsm = node.context().cache().context().walState();

            Object mux = U.field(wsm, "mux");

            // Try to acquire mux from an unrelated thread. With the fix, this completes
            // immediately. Without the fix, this hangs because exchange-worker holds mux.
            IgniteInternalFuture<Object> muxAcquireFut = GridTestUtils.runAsync(() -> {
                synchronized (mux) {
                    return null;
                }
            });

            // 5 s is well above the time a healthy synchronized acquisition needs, and well
            // below the 60 s test method timeout, so a regression manifests as a timely
            // IgniteFutureTimeoutCheckedException rather than a hung suite.
            muxAcquireFut.get(5, TimeUnit.SECONDS);
        }
        finally {
            db.checkpointReadUnlock();

            if (walDisableFut != null) {
                // Now that the read lock is released, the exchange-worker can finish the WAL
                // state change and the cluster API call returns.
                walDisableFut.get(30_000);
            }
        }
    }
}
