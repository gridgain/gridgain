package org.apache.ignite.agent.service.action;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.agent.Agent;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.agent.dto.action.Response;
import org.apache.ignite.agent.dto.action.ResponseError;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.LoggerResource;

import static java.util.function.Function.identity;
import static org.apache.ignite.agent.dto.action.ActionStatus.COMPLETED;
import static org.apache.ignite.agent.dto.action.ActionStatus.FAILED;
import static org.apache.ignite.agent.utils.AgentUtils.getErrorCode;
import static org.apache.ignite.compute.ComputeJobResultPolicy.WAIT;

/**
 * Execute action task.
 */
public class ExecuteActionTask extends ComputeTaskAdapter<Request, Response> {
    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        return WAIT;
    }

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Request arg) throws IgniteException {
        return subgrid.stream().collect(Collectors.toMap(n -> new ExecuteActionJob(arg), identity()));
    }

    /** {@inheritDoc} */
    @Override public Response reduce(List<ComputeJobResult> results) throws IgniteException {
        ComputeJobResult computeJobRes = results.get(0);

        return computeJobRes.getData();
    }

    /**
     * Execute action job.
     */
    private static class ExecuteActionJob implements ComputeJob {
        /** Ignite. */
        @IgniteInstanceResource
        protected IgniteEx ignite;

        /** Logger. */
        @LoggerResource(categoryClass = ExecuteActionJob.class)
        private IgniteLogger log;

        /** Job context. */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /** Request. */
        private final Request req;

        /** Is canceled. */
        private boolean isCanceled;

        /** Response. */
        private Response res;

        /**
         * @param req Request.
         */
        public ExecuteActionJob(Request req) {
            this.req = req;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            isCanceled = true;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            if (res != null)
                return res;

            jobCtx.holdcc();

            Agent agent = (Agent) ignite.context().gmc();

            agent.actionDispatcher().dispatch(req)
                .thenApply(CompletableFuture::join)
                .thenApply(r -> res = new Response().setId(req.getId()).setStatus(COMPLETED).setResult(r))
                .exceptionally(e -> res = convertToErrorResponse(req.getId(), e))
                .thenAccept(r -> jobCtx.callcc());

            return null;
        }

        /**
         * @param id Request ID.
         * @param e Throwable.
         */
        private Response convertToErrorResponse(UUID id, Throwable e) {
            Throwable cause = e.getCause();

            log.error(String.format("Failed to execute action, send error response: [reqId=%s]", id), e);

            return new Response()
                .setId(id)
                .setStatus(FAILED)
                .setError(new ResponseError(getErrorCode(cause), cause.getMessage(), cause.getStackTrace()));
        }
    }
}
