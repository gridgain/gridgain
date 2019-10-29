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

package org.apache.ignite.agent.service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.agent.WebSocketManager;
import org.apache.ignite.agent.action.ActionDispatcher;
import org.apache.ignite.agent.dto.action.InvalidRequest;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.agent.dto.action.Response;
import org.apache.ignite.agent.dto.action.ResponseError;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.SecurityException;

import static org.apache.ignite.agent.StompDestinationsUtils.buildActionResponseDest;
import static org.apache.ignite.agent.dto.action.ActionStatus.COMPLETED;
import static org.apache.ignite.agent.dto.action.ActionStatus.FAILED;
import static org.apache.ignite.agent.dto.action.ActionStatus.RUNNING;
import static org.apache.ignite.agent.dto.action.ResponseError.AUTHENTICATION_ERROR_CODE;
import static org.apache.ignite.agent.dto.action.ResponseError.AUTHORIZE_ERROR_CODE;
import static org.apache.ignite.agent.dto.action.ResponseError.INTERNAL_ERROR_CODE;
import static org.apache.ignite.agent.dto.action.ResponseError.PARSE_ERROR_CODE;

/**
 * Action service.
 */
public class ActionService implements AutoCloseable {
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
        // Deserialization error occurred.
        if (req instanceof InvalidRequest) {
            Throwable ex = ((InvalidRequest) req).getCause();
            sendResponse(
                new Response()
                    .setId(req.getId())
                    .setStatus(FAILED)
                    .setError(new ResponseError(PARSE_ERROR_CODE, ex.getMessage(), ex.getStackTrace()))
            );

            return;
        }

        try {
            sendResponse(new Response().setId(req.getId()).setStatus(RUNNING));

            dispatcher.dispatch(req)
                    .thenApply(CompletableFuture::join)
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
                .setId(id)
                .setStatus(FAILED)
                .setError(new ResponseError(getErrorCode(e), e.getMessage(), e.getStackTrace()));
    }

    /**
     * @param e Exception.
     * @return Integer error code.
     */
    private int getErrorCode(Throwable e) {
        Throwable cause = e.getCause();

        if (cause instanceof SecurityException)
            return AUTHORIZE_ERROR_CODE;
        else if (cause instanceof IgniteAuthenticationException || cause instanceof IgniteAccessControlException)
            return AUTHENTICATION_ERROR_CODE;

        return INTERNAL_ERROR_CODE;
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
