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

package org.apache.ignite.agent.processor.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.agent.dto.IgniteConfigurationWrapper;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;

import static org.apache.ignite.agent.utils.AgentObjectMapperFactory.jsonMapper;

/**
 * Node configuration exporter.
 */
public class NodesConfigurationExporter extends GridProcessorAdapter {
    /** Mapper. */
    private final ObjectMapper mapper = jsonMapper();

    /** Topic for node config. */
    public static final String TOPIC_NODE_CFG = "mgmt-console-node-configuration-topic";

    /**
     * @param ctx Context.
     */
    public NodesConfigurationExporter(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Send node configuration to coordinator.
     */
    public void export() {
        try {
            ctx.grid()
                .message(ctx.grid().cluster().forOldest())
                .send(TOPIC_NODE_CFG, mapper.writeValueAsString(new IgniteConfigurationWrapper(ctx.config())));
        }
        catch (JsonProcessingException e) {
            log.error("Failed to serialize the IgniteConfiguration to JSON", e);
        }
    }
}
