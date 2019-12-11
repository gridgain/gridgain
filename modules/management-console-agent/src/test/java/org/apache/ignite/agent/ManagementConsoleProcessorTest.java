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

import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/**
 * Management console processor test.
 */
public class ManagementConsoleProcessorTest extends AgentCommonAbstractTest {
    /**
     * The onKernalStop method should correct close all agent threads.
     */
    @Test
    public void shouldCorrectStartAgentOnAnotherNode_When_Coordinator_And_AnotherNode_IsStopping() throws Exception {
        IgniteEx ignite = startGrids(3);

        changeManagementConsoleConfig(ignite);

        stopGrid(0, true);

        stopGrid(2, true);

        stopGrid(1, true);

        checkThreads();
    }
}
