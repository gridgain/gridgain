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

package org.gridgain.action.controller;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.node.VisorNodePingTaskArg;
import org.gridgain.dto.action.Request;
import org.junit.Test;

import java.util.UUID;

import static org.gridgain.dto.action.ActionStatus.COMPLETED;
import static org.gridgain.dto.action.ActionStatus.ERROR;

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
            .setActionName("NodeActionsController.pingNode")
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
                .setActionName("NodeActionsController.pingNode")
                .setId(UUID.randomUUID())
                .setArgument(new VisorNodePingTaskArg(UUID.randomUUID()));

        executeAction(req, (r) -> r.getStatus() == ERROR && !F.isEmpty(r.getError().getMessage()));
    }
}
