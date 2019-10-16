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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.gridgain.action.annotation.ActionController;

import static org.gridgain.utils.AgentUtils.authorizeIfNeeded;

/**
 * Baseline actions controller.
 */
@ActionController("BaselineActions")
public class BaselineActionsController {
    /** Context. */
    private final GridKernalContext ctx;

    /**
     * @param ctx Context.
     */
    public BaselineActionsController(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * @param isAutoAdjustEnabled Is auto adjust enabled.
     */
    public void updateAutoAdjustEnabled(boolean isAutoAdjustEnabled) {
        authorizeIfNeeded(ctx.security(), SecurityPermission.ADMIN_OPS);

        ctx.grid().cluster().baselineAutoAdjustEnabled(isAutoAdjustEnabled);
    }

    /**
     * @param awaitingTime Awaiting time in ms.
     */
    public void updateAutoAdjustAwaitingTime(long awaitingTime) {
        authorizeIfNeeded(ctx.security(), SecurityPermission.ADMIN_OPS);

        ctx.grid().cluster().baselineAutoAdjustTimeout(awaitingTime);
    }

    /**
     * @param consIds Node consistent ids.
     */
    public void setBaselineTopology(Collection<String> consIds) {
        authorizeIfNeeded(ctx.security(), SecurityPermission.ADMIN_OPS);

        ctx.grid().cluster().setBaselineTopology(findNodesByConsistentIds(consIds));
    }

    /**
     * @param consIds Node consistent ids.
     */
    private Collection<BaselineNode> findNodesByConsistentIds(Collection<String> consIds) {
        Map<String, BaselineNode> baseline = currentBaseLine();
        Map<String, BaselineNode> srvrs = currentServers();

        Collection<BaselineNode> baselineTop = new ArrayList<>();

        for (String consistentId : consIds) {
            if (srvrs.containsKey(consistentId))
                baselineTop.add(srvrs.get(consistentId));

            else if (baseline.containsKey(consistentId))
                baselineTop.add(baseline.get(consistentId));

            else
                throw new IgniteIllegalStateException("Check arguments. Node not found for consistent ID: " + consistentId);
        }

        return baselineTop;
    }

    /**
     * @return Current baseline.
     */
    private Map<String, BaselineNode> currentBaseLine() {
        Map<String, BaselineNode> nodes = new HashMap<>();

        Collection<BaselineNode> baseline = ctx.grid().cluster().currentBaselineTopology();

        if (!F.isEmpty(baseline)) {
            for (BaselineNode node : baseline)
                nodes.put(node.consistentId().toString(), node);
        }

        return nodes;
    }

    /**
     * @return Current server nodes.
     */
    private Map<String, BaselineNode> currentServers() {
        Map<String, BaselineNode> nodes = new HashMap<>();

        for (ClusterNode node : ctx.grid().cluster().forServers().nodes())
            nodes.put(node.consistentId().toString(), node);

        return nodes;
    }
}
