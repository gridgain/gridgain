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

package org.apache.ignite.agent.service.config;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.agent.dto.NodeConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.agent.WebSocketManager;
import org.apache.ignite.agent.service.sender.GmcSender;

import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterNodeConfigurationDest;

/**
 * Node configuration service.
 */
public class NodeConfigurationService implements AutoCloseable {
    /** Queue capacity. */
    private static final int QUEUE_CAP = 10;

    /** Context. */
    private final GridKernalContext ctx;

    /** Manager. */
    private final WebSocketManager mgr;

    /** Sender. */
    private final GmcSender<NodeConfiguration> snd;

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

        ctx.grid().message().localListen(NodeConfigurationExporter.NODE_CONFIGURATION_TOPIC, lsnr);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        ctx.grid().message().stopLocalListen(NodeConfigurationExporter.NODE_CONFIGURATION_TOPIC, lsnr);
        U.closeQuiet(snd);
    }

    /**
     * @param nid Uuid.
     * @param cfgList Config list.
     */
    boolean onNodeConfiguration(UUID nid, Object cfgList) {
        String cfg = F.first((List<String>) cfgList);

        if (!F.isEmpty(cfg)) {
            String consistentId = ctx.cluster().get().node(nid).consistentId().toString();
            NodeConfiguration nodeCfg = new NodeConfiguration(consistentId, cfg);

            snd.send(nodeCfg);
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
