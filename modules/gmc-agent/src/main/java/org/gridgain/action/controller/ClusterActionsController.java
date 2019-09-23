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

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.gridgain.action.annotation.ActionController;

import java.util.concurrent.CompletableFuture;

import static org.gridgain.utils.AgentUtils.authorizeIfNeeded;

/**
 * Controller for cluster actions.
 */
@ActionController("ClusterActions")
public class ClusterActionsController {
    /** Context. */
    private final GridKernalContext ctx;

    /**
     * @param ctx Context.
     */
    public ClusterActionsController(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Activate cluster.
     */
    public CompletableFuture<Void> activate() {
        authorizeIfNeeded(ctx.security(), SecurityPermission.ADMIN_OPS);

        ctx.grid().cluster().active(true);

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Deactivate cluster.
     */
    public CompletableFuture<Void> deactivate() {
        authorizeIfNeeded(ctx.security(), SecurityPermission.ADMIN_OPS);

        ctx.grid().cluster().active(false);

        return CompletableFuture.completedFuture(null);
    }
}
