/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.internal.compute.flow;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Flow processor.
 */
public class FlowProcessor extends GridProcessorAdapter {
    private final Map<String, TaskFlow> flows = new ConcurrentHashMap<>();
    /**
     * @param ctx Kernal context.
     */
    public FlowProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Add new flow to metastorage.
     *
     * @param name Name of the flow.
     * @param flow Flow.
     * @param replaceExisting Whether to replace existing.
     */
    public void addFlow(String name, TaskFlow flow, boolean replaceExisting) {
        flows.put(name, flow);
    }

    /**
     * Remove flow from metastorage.
     *
     * @param name Name of the flow.
     */
    public void removeFlow(String name) {
        flows.remove(name);
    }

    /**
     * Get flow by name.
     *
     * @param name Name of the flow.
     */
    public TaskFlow flow(String name) {
        return flows.get(name);
    }

    /**
     * Execute flow.
     *
     * @param name Name of the flow.
     * @param initialParams Initial parameters. These parameters will be passed as a parameters for root element task.
     * @return Flow execution result.
     */
    public IgniteFuture<FlowTaskTransferObject> executeFlow(String name, FlowTaskTransferObject initialParams) {
        TaskFlow flow = flow(name);

        TaskFlowContext flowContext = new TaskFlowContext(ctx, flow, initialParams);

        IgniteInternalFuture<FlowTaskTransferObject> fut = flowContext.start();

        return new IgniteFutureImpl<>(fut);
    }
}