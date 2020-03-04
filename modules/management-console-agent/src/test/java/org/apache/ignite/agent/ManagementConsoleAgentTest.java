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

package org.apache.ignite.agent;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.agent.processor.AbstractServiceTest;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.management.ManagementConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Control Center processor test.
 */
public class ManagementConsoleAgentTest extends AgentCommonAbstractTest {
    /**
     * Launch three nodes.
     * Await agent launch.
     * Stop coordinator node.
     * Stop non-coordinator node.
     * Stop last node.
     * Check that all threads of agents is stopped.
     */
    @Test
    public void shouldStopTheAgentCorrectly() throws Exception {
        IgniteEx ignite = startGrids(3);

        changeManagementConsoleConfig(ignite);

        stopGrid(0, true);

        stopGrid(2, true);

        stopGrid(1, true);

        checkThreads();
    }

    /**
     * Launch one node.
     * Validate default URIs of Control Center.
     */
    @Test
    public void shouldLoadConfigurationProperly() throws Exception {
        try (IgniteEx ignite = startGrids(1)) {
            ManagementConfiguration cfg = ignite.context().managementConsole().configuration();

            assertEquals("http://localhost:3000", F.first(cfg.getConsoleUris()));
        }
    }

    /**
     * Control Center processor with mock context test.
     */
    public static class ManagementConsoleAgentWithMockContextTest extends AbstractServiceTest {
        /**
         * GG-26201 - Testcase 5:
         *
         * 1. Mock `isTracingEnabled()` method to return false
         * 2. Verify the warning message is present in log after start.
         */
        @Test
        public void shouldNotCreateSpanExporterIfNodeNotSupportTracingFeature() {
            GridKernalContext ctx = getMockContext();

            GridEventStorageManager evt = mock(GridEventStorageManager.class);

            when(ctx.event()).thenReturn(evt);

            ClusterNode node = mock(ClusterNode.class);

            when(node.isClient()).thenReturn(true);

            when(ctx.discovery().localNode()).thenReturn(node);

            GridIoManager ioMgr = mock(GridIoManager.class);

            when(ctx.io()).thenReturn(ioMgr);

            ManagementConsoleAgent proc = spy(new ManagementConsoleAgent(ctx));

            doReturn(false).when(proc).isTracingEnabled();

            proc.onKernalStart(true);

            IgniteLogger log = ctx.log(ManagementConsoleAgent.class);

            verify(log).warning("Current Ignite configuration does not support tracing functionality" +
                " and control center agent will not collect traces" +
                " (consider adding ignite-opencensus module to classpath).", null);
        }
    }
}
