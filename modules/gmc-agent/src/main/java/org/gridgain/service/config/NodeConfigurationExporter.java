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

package org.gridgain.service.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.gridgain.dto.IgniteConfigurationWrapper;
import org.gridgain.service.sender.CoordinatorSender;

import static org.gridgain.utils.AgentObjectMapperFactory.jsonMapper;

/**
 * Node configuration exporter.
 */
public class NodeConfigurationExporter implements AutoCloseable {
    /** Mapper. */
    private final ObjectMapper mapper = jsonMapper();

    /** Queue capacity. */
    private static final int QUEUE_CAP = 10;

    /** Status description. */
    public static final String NODE_CONFIGURATION_TOPIC = "gmc-node-configuration-topic";

    /** Sender. */
    private final CoordinatorSender<String> snd;

    /** Logger. */
    private final IgniteLogger log;

    /** Context. */
    private GridKernalContext ctx;

    /**
     * @param ctx Context.
     */
    public NodeConfigurationExporter(GridKernalContext ctx) {
        this.ctx = ctx;
        this.snd = new CoordinatorSender<>(ctx, QUEUE_CAP, NODE_CONFIGURATION_TOPIC);
        this.log = ctx.log(NodeConfigurationExporter.class);
    }

    /**
     * Send node configuration to coordinator.
     */
    public void export() {
        try {
            snd.send(mapper.writeValueAsString(new IgniteConfigurationWrapper(ctx.config())));
        }
        catch (JsonProcessingException e) {
            log.error("Failed to serialize the IgniteConfiguration to JSON", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        U.closeQuiet(snd);
    }
}
