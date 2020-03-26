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

package org.apache.ignite.agent.processor.action;

import java.util.UUID;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.agent.ManagementConsoleAgent;
import org.apache.ignite.agent.action.Session;
import org.apache.ignite.agent.action.SessionRegistry;
import org.apache.ignite.agent.dto.action.InvalidRequest;
import org.apache.ignite.agent.dto.action.JobResponse;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.agent.dto.action.ResponseError;
import org.apache.ignite.agent.dto.action.TaskResponse;
import org.apache.ignite.agent.ws.WebSocketManager;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.agent.StompDestinationsUtils.buildActionJobResponseDest;
import static org.apache.ignite.agent.StompDestinationsUtils.buildActionTaskResponseDest;
import static org.apache.ignite.agent.dto.action.Status.FAILED;
import static org.apache.ignite.agent.utils.AgentUtils.getErrorCode;

/**
 * Distributed action service.
 */
public class DistributedActionProcessor extends GridProcessorAdapter {
    /** Authenticate action name. */
    public static final String AUTHENTICATE_ACTION_NAME = "SecurityActions.authenticate";

    /** Websocket manager. */
    private final WebSocketManager mgr;

    /** Session registry. */
    private final SessionRegistry sesRegistry;

    /** Local node consistent id. */
    private final String locNodeConsistentId;

    /**
     * @param ctx Context.
     */
    public DistributedActionProcessor(GridKernalContext ctx) {
        super(ctx);

        mgr = ((ManagementConsoleAgent)ctx.managementConsole()).webSocketManager();
        sesRegistry = ((ManagementConsoleAgent)ctx.managementConsole()).sessionRegistry();
        locNodeConsistentId = String.valueOf(ctx.grid().localNode().consistentId());
    }

    /**
     * @param req Request.
     */
    public void onActionRequest(Request req) {
        if (log.isDebugEnabled())
            log.debug("Received request: [sessionId=" + req.getSessionId() + ", reqId=" + req.getId() + "]");

        ClusterGroup grp = F.isEmpty(req.getNodeIds())
            ? ctx.grid().cluster().forServers()
            : ctx.grid().cluster().forNodeIds(req.getNodeIds());

        executeAction(grp, req);
    }

    /**
     * @param grp Cluster group.
     * @param req Request.
     */
    private void executeAction(ClusterGroup grp, Request req) {
        OperationSecurityContext secCtx = null;

        boolean isAuthenticateAct = AUTHENTICATE_ACTION_NAME.equals(req.getAction());
        boolean isSecurityNeeded = !isAuthenticateAct && (ctx.security().enabled() || ctx.authentication().enabled());

        try {
            if (req instanceof InvalidRequest)
                throw ((InvalidRequest) req).getCause();

            if (isSecurityNeeded) {
                UUID sesId = req.getSessionId();
                Session ses = sesRegistry.getSession(sesId);

                if (ses == null)
                    throw new IgniteAuthenticationException(
                        "Failed to authenticate, the session with provided sessionId: " + sesId
                    );

                if (ses.securityContext() != null)
                    secCtx = ctx.security().withContext(ses.securityContext());
            }

            ctx.grid().compute(grp).executeAsync(new ExecuteActionTask(), req).listen(f -> {
                try {
                    sendTaskResponse(f.get());
                }
                catch (Throwable e) {
                    sendFailedTaskResponse(req.getId(), e);
                }
            });
        }
        catch (Throwable e) {
            sendFailedTaskResponse(req.getId(), e);
        }
        finally {
            U.closeQuiet(secCtx);
        }
    }

    /**
     * @param res Response.
     */
    public void sendTaskResponse(TaskResponse res) {
        UUID clusterId = ctx.cluster().get().id();

        mgr.send(buildActionTaskResponseDest(clusterId), res);
    }

    /**
     * @param res Response.
     */
    public void sendJobResponse(JobResponse res) {
        UUID clusterId = ctx.cluster().get().id();

        mgr.send(buildActionJobResponseDest(clusterId), res);
    }

    /**
     * @param reqId Request id.
     * @param e Exception.
     * @return Task response.
     */
    private TaskResponse sendFailedTaskResponse(UUID reqId, Throwable e) {
        TaskResponse taskRes = new TaskResponse()
            .setId(reqId)
            .setJobCount(1)
            .setStatus(FAILED)
            .setNodeConsistentId(locNodeConsistentId);

        JobResponse jobRes = new JobResponse()
            .setRequestId(reqId)
            .setStatus(FAILED)
            .setError(new ResponseError(getErrorCode(e), e.getMessage(), e.getStackTrace()));

        sendTaskResponse(taskRes);
        sendJobResponse(jobRes);

        return taskRes;
    }
}
