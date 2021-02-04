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
package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Consumer;

/**
 * Statistics cleaning tests.
 */
public class StatisticsClearTest extends StatisticsRestartAbstractTest {
    /** {@inheritDoc} */
    @Override public int nodes() {
        return 2;
    }

    /**
     * 1) Collect statistics on two nodes cluster.
     * 2) Test that it available on each node.
     * 3) Clear statistics.
     * 4) Test that it cleaned on each node.
     *
     * @throws Exception In case of errors.
     */
    @Test
    public void testStatisticsClear() throws Exception {
        IgniteStatisticsManager statMgr0 = grid(0).context().query().getIndexing().statsManager();
        IgniteStatisticsManager statMgr1 = grid(1).context().query().getIndexing().statsManager();

        statMgr0.gatherObjectStatistics(SMALL_TARGET);

        Assert.assertNotNull(statMgr0.getLocalStatistics(SCHEMA, "SMALL"));
        Assert.assertNotNull(statMgr0.getGlobalStatistics(SCHEMA, "SMALL"));

        Assert.assertNotNull(statMgr1.getLocalStatistics(SCHEMA, "SMALL"));
        Assert.assertNotNull(statMgr1.getGlobalStatistics(SCHEMA, "SMALL"));

        statMgr1.clearObjectStatistics(SMALL_TARGET);

        GridTestUtils.waitForCondition(() -> {
            try {
                return null == statMgr0.getLocalStatistics(SCHEMA, "SMALL")
                    && null == statMgr1.getLocalStatistics(SCHEMA, "SMALL")
                    && null == statMgr0.getGlobalStatistics(SCHEMA, "SMALL")
                    && null == statMgr1.getGlobalStatistics(SCHEMA, "SMALL");
            }
            catch (IgniteCheckedException e) {
                fail(e.getMessage());
            }
            return false;
        }, TIMEOUT);
    }

    /**
     * 1) Clear statistics by non existing table.
     * 2) Acquire statistics by non existing table.
     *
     * @throws Exception In case of errors.
     */
    @Test
    public void testStatisticsClearOnNotExistingTable() throws Exception {
        IgniteStatisticsManager statMgr0 = grid(0).context().query().getIndexing().statsManager();
        IgniteStatisticsManager statMgr1 = grid(1).context().query().getIndexing().statsManager();

        statMgr1.clearObjectStatistics(SMALL_TARGET);

        Assert.assertNull(statMgr0.getLocalStatistics(SCHEMA, "NO_NAME"));
        Assert.assertNull(statMgr1.getLocalStatistics(SCHEMA, "NO_NAME"));

        Assert.assertNull(statMgr0.getGlobalStatistics(SCHEMA, "NO_NAME"));
        Assert.assertNull(statMgr1.getGlobalStatistics(SCHEMA, "NO_NAME"));
    }


    /**
     * 1) Restart without statistics version
     * 2) Check that statistics was cleared
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testRestartWrongVersion() throws Exception {

        testRestartVersion(metaStorage -> {
            try {
                metaStorage.write("stats.version", 2);
            }
            catch (IgniteCheckedException e) {
                Assert.fail();
            }
        });
    }

    /**
     * Test without any statistics version.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testRestartNoVersion() throws Exception {
        testRestartVersion(metaStorage -> {
            try {
                metaStorage.remove("stats.version");
            }
            catch (IgniteCheckedException e) {
                Assert.fail();
            }
        });
    }

    /**
     * Test with corrupted (from the current version point of view) statistics version.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testRestartCorruptedVersion() throws Exception {
        testRestartVersion(metaStorage -> {
            try {
                metaStorage.write("stats.version", "corrupted");
            }
            catch (IgniteCheckedException e) {
                Assert.fail();
            }
        });
    }

    /**
     * Apply function to metastorage and restart node to verify that it will lead to metadata removal.
     *
     * @param verCorruptor statistics version corruptor.
     * @throws Exception In case of errors.
     */
    private void testRestartVersion(Consumer<MetaStorage> verCorruptor) throws Exception {
        IgniteCacheDatabaseSharedManager db = grid(0).context().cache().context().database();

        checkStatisticsExist(db);

        db.checkpointReadLock();
        try {
            verCorruptor.accept(db.metaStorage());
        }
        finally {
            db.checkpointReadUnlock();
        }

        stopGrid(0);

        startGrid(0);

        grid(0).cluster().state(ClusterState.ACTIVE);

        db = grid(0).context().cache().context().database();
        try {
            checkStatisticsExist(db);
            throw new Exception("Statistics exists after restart.");
        }
        catch (AssertionError e) {
            // NoOp.
        }
    }

    /**
     * Test that statistics for table SMALL exists in local metastorage.
     *
     * @param db IgniteCacheDatabaseSharedManager to test in.
     * @throws IgniteCheckedException In case of errors.
     */
    private void checkStatisticsExist(IgniteCacheDatabaseSharedManager db) throws IgniteCheckedException {
        db.checkpointReadLock();
        try {
            boolean found[] = new boolean[1];

            db.metaStorage().iterate("stats.data.PUBLIC.SMALL.", (k, v) -> found[0] = true, true);

            Assert.assertTrue(found[0]);
        }
        finally {
            db.checkpointReadUnlock();
        }
    }
}
