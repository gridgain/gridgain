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

import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.gridgain.agent.WebSocketManager;
import org.gridgain.dto.IgniteConfigurationWrapper;
import org.gridgain.dto.NodeConfiguration;
import org.gridgain.service.sender.GmcSender;

import static org.gridgain.agent.StompDestinationsUtils.buildClusterNodeConfigurationDest;
import static org.gridgain.service.config.NodeConfigurationExporter.NODE_CONFIGURATION_TOPIC;

/**
 * Node configuration service.
 */
public class NodeConfigurationService implements AutoCloseable {
    /** Queue capacity. */
    private static final int QUEUE_CAP = 10;

    /** Mapper. */
    private final ObjectMapper mapper = new ObjectMapper();

    /** Context. */
    private final GridKernalContext ctx;

    /** Manager. */
    private final WebSocketManager mgr;

    /** Sender. */
    private final GmcSender<NodeConfiguration> snd;

    /** Logger. */
    private final IgniteLogger log;

    /** On node traces listener. */
    private final IgniteBiPredicate<UUID, Object> lsnr = this::onNodeConfiguration;

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public NodeConfigurationService(GridKernalContext ctx, WebSocketManager mgr) {
        this.ctx = ctx;
        this.mgr = mgr;
        this.snd = createSender();
        this.log = ctx.log(NodeConfigurationService.class);

        ctx.grid().message().localListen(NODE_CONFIGURATION_TOPIC, lsnr);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        ctx.grid().message().stopLocalListen(NODE_CONFIGURATION_TOPIC, lsnr);
        U.closeQuiet(snd);
    }

    /**
     * @param uuid Uuid.
     * @param cfgList Config list.
     */
    boolean onNodeConfiguration(UUID uuid, Object cfgList) {
        try {
            IgniteConfigurationWrapper cfg = F.first((List<IgniteConfigurationWrapper>) cfgList);
            String consistentId = ctx.cluster().get().localNode().consistentId().toString();
            NodeConfiguration nodeCfg = new NodeConfiguration(consistentId, mapper.writeValueAsString(cfg));

            snd.send(nodeCfg);
        }
        catch (JsonProcessingException e) {
            log.error("Failed to serialiaze the IgniteConfigurationWrapper to string", e);
        }

        return true;
    }

    /**
     * @return Sender which send messages from queue to gmc.
     */
    private GmcSender<NodeConfiguration> createSender() {
        UUID clusterId = ctx.cluster().get().id();
        return new GmcSender<>(ctx, mgr, QUEUE_CAP, buildClusterNodeConfigurationDest(clusterId));
    }
}
