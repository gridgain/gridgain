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
import org.apache.ignite.agent.WebSocketManager;
import org.apache.ignite.agent.dto.NodeConfiguration;
import org.apache.ignite.agent.service.sender.ManagementConsoleSender;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;

import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterNodeConfigurationDest;
import static org.apache.ignite.agent.service.config.NodeConfigurationExporter.TOPIC_NODE_CFG;

/**
 * Node configuration service.
 */
public class NodeConfigurationService extends GridProcessorAdapter {
    /** Queue capacity. */
    private static final int QUEUE_CAP = 10;

    /** Manager. */
    private final WebSocketManager mgr;

    /** Sender. */
    private final ManagementConsoleSender<NodeConfiguration> snd;

    /** On node traces listener. */
    private final IgniteBiPredicate<UUID, Object> lsnr = this::processNodeConfigurations;

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public NodeConfigurationService(GridKernalContext ctx, WebSocketManager mgr) {
        super(ctx);
        this.mgr = mgr;
        this.snd = createSender();

        ctx.grid().message().localListen(TOPIC_NODE_CFG, lsnr);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        ctx.grid().message().stopLocalListen(TOPIC_NODE_CFG, lsnr);
        U.closeQuiet(snd);
    }

    /**
     * @param nid Uuid.
     * @param cfgList Config list.
     */
    boolean processNodeConfigurations(UUID nid, Object cfgList) {
        String cfg = F.first((List<String>) cfgList);

        if (!F.isEmpty(cfg)) {
            String consistentId = ctx.cluster().get().node(nid).consistentId().toString();
            NodeConfiguration nodeCfg = new NodeConfiguration(consistentId, cfg);

            snd.send(nodeCfg);
        }

        return true;
    }

    /**
     * @return Sender which send messages from queue to Management Console.
     */
    private ManagementConsoleSender<NodeConfiguration> createSender() {
        return new ManagementConsoleSender<>(
            ctx,
            mgr,
            buildClusterNodeConfigurationDest(ctx.cluster().get().id()),
            "mgmt-console-node-cfg-sender-",
            QUEUE_CAP
        );
    }
}
