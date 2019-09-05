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

import org.apache.ignite.IgniteCompute;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.node.VisorNodePingTask;
import org.apache.ignite.internal.visor.node.VisorNodePingTaskArg;
import org.apache.ignite.internal.visor.node.VisorNodePingTaskResult;
import org.gridgain.action.ActionController;
import org.gridgain.agent.AgentUtils;

import java.util.concurrent.CompletableFuture;

/**
 * Cluster actions controller.
 */
@ActionController
public class NodeActionsController {
    /** Compute. */
    private final IgniteCompute compute;

    /**
     * @param ctx Context.
     */
    public NodeActionsController(GridKernalContext ctx) {
        compute = ctx.grid().compute();
    }

    /**
     * @param arg Argument.
     */
    public CompletableFuture<VisorNodePingTaskResult> pingNode(VisorNodePingTaskArg arg) {
        CompletableFuture<VisorNodePingTaskResult> fut = new CompletableFuture<>();
        VisorTaskArgument<VisorNodePingTaskArg> taskArg = new VisorTaskArgument<>(arg.getNodeId(), arg, false);

        compute.executeAsync(VisorNodePingTask.class, taskArg).chain(f -> AgentUtils.completeFuture(f, fut));

        return fut;
    }
}
