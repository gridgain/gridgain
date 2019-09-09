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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
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
public class ActionService implements AutoCloseable {
    /** Internal error code. */
    private  static final int INTERNAL_ERROR_CODE = -32603;

    /** Context. */
    private final GridKernalContext ctx;

    /** Manager. */
    private final WebSocketManager mgr;

    /** Action dispatcher. */
    private final ActionDispatcher dispatcher;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public ActionService(GridKernalContext ctx, WebSocketManager mgr) {
        this.ctx = ctx;
        this.mgr = mgr;
        this.log = ctx.log(ActionService.class);

        dispatcher = new ActionDispatcher(ctx);
    }

    /**
     * Handle action requests from GMC.
     *
     * @param req Request.
     */
    public void onActionRequest(Request req) {
        sendResponse(new Response().setId(req.getId()).setStatus(RUNNING));

        try {
            CompletableFuture<CompletableFuture> fut = dispatcher.dispatch(req);

            fut.thenApply(CompletableFuture::join)
                    .thenApply(r -> new Response().setId(req.getId()).setStatus(COMPLETED).setResult(r))
                    .exceptionally(e -> convertToErrorResponse(req.getId(), e))
                    .thenAccept(this::sendResponse);
        }
        catch (Exception e) {
            sendResponse(convertToErrorResponse(req.getId(), e));
        }
    }

    /**
     * @param id Id.
     * @param e Throwable.
     */
    private Response convertToErrorResponse(UUID id, Throwable e) {
        log.error(String.format("Failed to execute action, send error response to GMC: [reqId=%s]", id), e);

        return new Response()
                .setStatus(ERROR)
                .setId(id)
                .setError(new ResponseError(INTERNAL_ERROR_CODE, e.getMessage(), e.getStackTrace()));
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

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        U.closeQuiet(dispatcher);
    }
}
