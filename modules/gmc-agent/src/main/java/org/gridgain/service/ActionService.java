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

package org.gridgain.service;

import org.apache.ignite.internal.GridKernalContext;
import org.gridgain.action.ActionDispatcher;
import org.gridgain.dto.action.Request;
import org.gridgain.dto.action.Response;
import org.gridgain.dto.action.ResponseError;
import org.gridgain.agent.WebSocketManager;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.gridgain.dto.action.ActionStatus.*;
import static org.gridgain.agent.StompDestinationsUtils.buildActionResponseDest;

/**
 * Action service.
 */
public class ActionService {
    /** Context. */
    private final GridKernalContext ctx;

    /** Manager. */
    private final WebSocketManager mgr;

    /** Action dispatcher. */
    private final ActionDispatcher dispatcher;

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public ActionService(GridKernalContext ctx, WebSocketManager mgr) {
        this.ctx = ctx;
        this.mgr = mgr;

        dispatcher = new ActionDispatcher(ctx);
    }

    /**
     * Handle action requests from GMC.
     *
     * @param req Request.
     */
    public void onActionRequest(Request req) {
        try {
            CompletableFuture<?> fut = dispatcher.dispatch(req);
            Response res = new Response().setId(req.getId()).setStatus(RUNNING);

            if (fut.isDone())
                res.setStatus(COMPLETED).setResult(fut.get());
            else
                fut.thenAccept(r -> sendResponse(new Response().setId(req.getId()).setStatus(COMPLETED).setResult(r)));

            sendResponse(res);
        }
        catch (Exception e) {
            Response res = new Response();
            res.setStatus(ERROR)
                    .setId(req.getId())
                    .setError(new ResponseError(1, e.getMessage(), e.getStackTrace()));

            sendResponse(res);
        }
    }

    /**
     * Send action response to GMC.
     *
     * @param res Response.
     */
    private void sendResponse(Response res) {
        UUID clusterId = ctx.cluster().get().id();

        mgr.send(buildActionResponseDest(clusterId, res.getId()), res);
    }
}
