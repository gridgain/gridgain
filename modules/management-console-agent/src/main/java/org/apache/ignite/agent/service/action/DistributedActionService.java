package org.apache.ignite.agent.service.action;

import java.util.UUID;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.agent.WebSocketManager;
import org.apache.ignite.agent.action.ActionDispatcher;
import org.apache.ignite.agent.action.Session;
import org.apache.ignite.agent.action.SessionRegistry;
import org.apache.ignite.agent.dto.action.InvalidRequest;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.agent.dto.action.Response;
import org.apache.ignite.agent.dto.action.ResponseError;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.agent.StompDestinationsUtils.buildActionResponseDest;
import static org.apache.ignite.agent.dto.action.ActionStatus.FAILED;
import static org.apache.ignite.agent.dto.action.ActionStatus.RUNNING;
import static org.apache.ignite.agent.utils.AgentUtils.getErrorCode;

/**
 * Distributed action service.
 */
public class DistributedActionService {
    /** Context. */
    private final GridKernalContext ctx;

    /** Websocket manager. */
    private final WebSocketManager mgr;

    /** Session registry. */
    private SessionRegistry sesRegistry;

    /** Logger. */
    private IgniteLogger log;

    /**
     * @param ctx Context.
     * @param mgr Websocket manager.
     */
    public DistributedActionService(GridKernalContext ctx, WebSocketManager mgr) {
        this.ctx = ctx;
        this.mgr = mgr;
        this.log = ctx.log(ActionDispatcher.class);
        this.sesRegistry = SessionRegistry.getInstance(ctx);
    }

    /**
     * @param req Request.
     */
    public void onActionRequest(Request req) {
        if (log.isDebugEnabled())
            log.debug("Received request: [sessionId=" + req.getSessionId() + ", reqId=" + req.getId() + "]");

        // Deserialization error occurred.
        if (req instanceof InvalidRequest) {
            sendFailedResponse(req.getId(), ((InvalidRequest) req).getCause());

            return;
        }

        ClusterGroup grp = req.getNodeId() == null
            ? ctx.grid().cluster().forLocal()
            : ctx.grid().cluster().forNodeId(req.getNodeId());

        sendResponse(new Response().setId(req.getId()).setStatus(RUNNING));

        executeAction(grp, req);
    }

    /**
     * @param grp Cluster group.
     * @param req Request.
     */
    private void executeAction(ClusterGroup grp, Request req) {
        OperationSecurityContext secCtx = null;
        boolean isAuthenticateAct = "SecurityActions.authenticate".equals(req.getAction());
        boolean isSecurityNeeded = !isAuthenticateAct && (ctx.security().enabled() || ctx.authentication().enabled());

        try {
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

            ctx.grid().compute(grp).executeAsync(new ExecuteActionTask(), req).listen(f -> sendResponse(f.get()));
        }
        catch (Exception e) {
            sendFailedResponse(req.getId(), e);
        }
        finally {
            U.closeQuiet(secCtx);
        }
    }

    /**
     * @param reqId Request ID.
     * @param e Exception.
     */
    private void sendFailedResponse(UUID reqId, Throwable e) {
        sendResponse(new Response()
            .setId(reqId)
            .setStatus(FAILED)
            .setError(new ResponseError(getErrorCode(e), e.getMessage(), e.getStackTrace()))
        );
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
