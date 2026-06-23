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

package org.apache.ignite.internal.processors.metastorage;

import java.util.regex.Pattern;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests INFO-level logging of distributed metastorage version updates.
 */
public class DistributedMetaStorageUpdateLoggingTest extends GridCommonAbstractTest {
    /** Shared listening logger for all nodes. */
    private static ListeningTestLogger testLog;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        testLog = new ListeningTestLogger(log);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(128L * 1024 * 1024)
                .setPersistenceEnabled(true)));

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        testLog.clearListeners();
    }

    /**
     * Checks that a distributed metastorage write is logged at INFO on every node, not just the originator: with
     * two server nodes the update message is applied (and logged) once per node.
     */
    @Test
    public void testVersionUpdateLoggedOnAllNodes() throws Exception {
        IgniteEx ignite = startGrids(2);

        ignite.cluster().state(ClusterState.ACTIVE);

        // Both nodes apply the discovery update locally, so the line is expected exactly twice (one per node).
        LogListener lsnr = LogListener.matches(Pattern.compile(
            "Wrote metastorage history item \\[ver=.*, itemToWrite=\\[test-key=test-value\\]\\]")).times(2).build();

        testLog.registerListener(lsnr);

        ignite.context().distributedMetastorage().write("test-key", "test-value");

        assertTrue("Update was not logged on both nodes", waitForCondition(lsnr::check, 10_000));
    }

    /**
     * Checks that history catch-up replay on node rejoin is logged at INFO as a single batch line containing
     * the initial and final versions plus all replayed keys.
     */
    @Test
    public void testCatchUpBatchLoggedOnRejoin() throws Exception {
        IgniteEx ignite = startGrids(2);

        ignite.cluster().state(ClusterState.ACTIVE);

        stopGrid(1);

        for (int i = 0; i < 3; i++) {
            ignite.context().distributedMetastorage().write("catch-up-key-" + i, i);
        }

        LogListener lsnr = LogListener.matches(Pattern.compile(
            "Applying received distributed metastorage updates: \\[updates=\\[.*catch-up-key-0=0.*" +
                "catch-up-key-1=1.*catch-up-key-2=2.*\\], initialVer=.*, ver=.*\\]")).build();

        testLog.registerListener(lsnr);

        startGrid(1);

        assertTrue("Catch-up batch was not logged on restarted node", waitForCondition(lsnr::check, 10_000));
    }

    /**
     * Checks the mirror scenario of {@link #testCatchUpBatchLoggedOnRejoin()}: when a node that is <em>ahead</em>
     * of the cluster joins, the existing node applies the joiner's newer history and logs it at INFO as a single
     * batch line (the "Applying new metastore data on join" path).
     */
    @Test
    public void testNewDataFromJoiningNodeLoggedOnExistingNode() throws Exception {
        IgniteEx ignite = startGrids(2);

        ignite.cluster().state(ClusterState.ACTIVE);

        ignite.context().distributedMetastorage().write("base-key", "base");

        // Stop node 1 and advance the version on node 0 only, so node 0 ends up ahead of node 1.
        stopGrid(1);

        for (int i = 0; i < 3; i++)
            ignite.context().distributedMetastorage().write("join-key-" + i, i);

        stopGrid(0);

        // Start the behind node first (it becomes coordinator), then let the ahead node join.
        startGrid(1);

        LogListener lsnr = LogListener.matches(Pattern.compile(
            "Applying new metastore data on join: \\[updates=\\[.*join-key-0=0.*" +
                "join-key-1=1.*join-key-2=2.*\\], initialVer=.*, ver=.*\\]")).build();

        testLog.registerListener(lsnr);

        startGrid(0);

        assertTrue("New-data-on-join batch was not logged on the existing node", waitForCondition(lsnr::check, 10_000));
    }
}
