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

package org.apache.ignite.agent.action.controller;

import java.util.UUID;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.node.VisorNodePingTaskArg;
import org.junit.Test;

import static org.apache.ignite.agent.dto.action.ActionStatus.COMPLETED;
import static org.apache.ignite.agent.dto.action.ActionStatus.FAILED;

/**
 * Cluster actions controller test.
 */
public class NodeActionsControllerTest extends AbstractActionControllerTest {
    /**
     * Should success execute ping node action.
     */
    @Test
    public void successPingNode() {
        Request req = new Request()
                .setAction("NodeActions.pingNode")
                .setId(UUID.randomUUID())
                .setArgument(new VisorNodePingTaskArg(cluster.localNode().id()));

        executeAction(req, (r) -> r.getStatus() == COMPLETED);
    }

    /**
     * Should success execute ping node action.
     */
    @Test
    public void errorPingNode() {
        Request req = new Request()
                .setAction("NodeActions.pingNode")
                .setId(UUID.randomUUID())
                .setArgument(new VisorNodePingTaskArg(UUID.randomUUID()));

        executeAction(req, (r) -> r.getStatus() == FAILED && !F.isEmpty(r.getError().getMessage()));
    }
}
