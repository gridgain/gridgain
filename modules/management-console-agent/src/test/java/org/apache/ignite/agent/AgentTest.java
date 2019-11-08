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

import java.util.Random;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

public class AgentTest extends AgentCommonAbstractSelfTest {
    /**
     * Should split list of elements into batches and send.
     */
    @Test
    public void shouldCorrectStopConnectPool() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        Agent agent = (Agent)ignite.context().managementConsole();

        while(true) {
            agent.onKernalStop(true);

            U.sleep(new Random().nextInt(5000));

            agent.onKernalStart(ignite.cluster().active());
        }
    }
}
