/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.util;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationDataRowMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationRepairMeta;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;

public class GridCommandHandlerTracingConfigurationTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    protected IgniteEx ignite;

    /** */
    private static CommandHandler hnd;

//    /**
//     * <ul>
//     *  <li>Init diagnostic and persistence dirs.</li>
//     *  <li>Start 2 nodes and activate cluster.</li>
//     *  <li>Prepare cache.</li>
//     * </ul>
//     *
//     * @throws Exception If failed.
//     */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrids(2);

//        ignite.cluster().active(true);
//
//        prepareCache();

        hnd = new CommandHandler();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // Do nothing;
    }

    @Test
    public void testTracingConfigurationListsAllDefaultConfigurations() throws Exception {
//        new CommandHandler().execute(Arrays.asList("--baseline", "auto_adjust", "disable", "--yes"));
        assertEquals(EXIT_CODE_OK, execute(hnd,"--tracing-configuration"));
    }

    @Test
    public void testTracingConfigurationListsAllDefaultConfigurationsWithRetrieveAllParam() throws Exception {
        assertEquals(EXIT_CODE_OK, execute(hnd,"--tracing-configuration", "retrieve-all"));
    }
}
