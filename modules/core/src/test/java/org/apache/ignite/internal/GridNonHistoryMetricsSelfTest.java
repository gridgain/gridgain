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

package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.GridTestJob;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class GridNonHistoryMetricsSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMetricsHistorySize(5);

        cfg.setCacheConfiguration();

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSingleTaskMetrics() throws Exception {
        final Ignite ignite = grid();

        ignite.compute().execute(new TestTask(), "testArg");

        // Let metrics update twice.
        awaitMetricsUpdate(2);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                ClusterMetrics metrics = ignite.cluster().localNode().metrics();

                return metrics.getTotalExecutedJobs() == 5;
            }
        }, 5000);

        ClusterMetrics metrics = ignite.cluster().localNode().metrics();

        info("Node metrics: " + metrics);

        assertEquals(5, metrics.getTotalExecutedJobs());
        assertEquals(0, metrics.getTotalCancelledJobs());
        assertEquals(0, metrics.getTotalRejectedJobs());
    }

    /**
     * Test task.
     */
    private static class TestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** Logger. */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            Collection<ComputeJob> refs = new ArrayList<>(gridSize * 5);

            for (int i = 0; i < gridSize * 5; i++)
                refs.add(new GridTestJob(arg.toString() + i + 1));

            return refs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            return results;
        }
    }
}
