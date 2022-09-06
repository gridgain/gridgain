/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.db.checkpoint;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE;

/**
 * Test of CheckpointTimeoutLock.
 */
public class CheckpointTimeoutLockTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
    public void testLockIsHeldByThreadAfterTimeout() throws Exception {
        IgniteEx ignite = startGrid();

        ignite.cluster().state(ClusterState.ACTIVE);

        GridCacheDatabaseSharedManager db = dbMgr(ignite);

        CountDownLatch latch = new CountDownLatch(1);

        db.addCheckpointListener(new CheckpointListener() {
            @Override public void onMarkCheckpointBegin(Context ctx) {
                latch.countDown();

                doSleep(200L);
            }

            @Override public void onCheckpointBegin(Context ctx) {
                // No-op.
            }

            @Override public void beforeCheckpointBegin(Context ctx) {
                // No-op.
            }
        });

        db.forceCheckpoint("lock");

        latch.await(getTestTimeout(), TimeUnit.MILLISECONDS);

        db.checkpointReadLockTimeout(10L);
        db.checkpointReadLock();
        db.checkpointReadUnlock();
        assertFalse(db.checkpointLockIsHeldByThread());
    }
}
