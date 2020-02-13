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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.agent.ManagementConsoleAgent;
import org.apache.ignite.agent.dto.action.JobResponse;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.agent.dto.action.TaskResponse;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskNoResultCache;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.LoggerResource;

import static java.util.function.Function.identity;
import static org.apache.ignite.agent.dto.action.Status.COMPLETED;
import static org.apache.ignite.agent.dto.action.Status.FAILED;
import static org.apache.ignite.agent.dto.action.Status.RUNNING;
import static org.apache.ignite.agent.utils.AgentUtils.convertToErrorJobResponse;
import static org.apache.ignite.compute.ComputeJobResultPolicy.WAIT;

/**
 * Execute action task.
 */
@GridInternal
@ComputeTaskNoResultCache
public class ExecuteActionTask extends ComputeTaskAdapter<Request, TaskResponse> {
    /** Ignite. */
    @IgniteInstanceResource
    protected IgniteEx ignite;

    /** Logger. */
    @LoggerResource(categoryClass = ExecuteActionTask.class)
    private IgniteLogger log;

    /** Has failed jobs. */
    private boolean hasFailedJobs;

    /** Request id. */
    private UUID reqId;

    /** Consistent id. */
    private String consistentId;

    /** Job count. */
    private int jobCnt;

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        JobResponse jobRes = res.getData();

        DistributedActionProcessor proc =
            ((ManagementConsoleAgent)ignite.context().managementConsole()).distributedActionProcessor();

        if (res.getException() != null || jobRes.getStatus() == FAILED)
            hasFailedJobs = true;

        if (res.getException() == null)
            proc.sendJobResponse(jobRes);
        else {
            log.error("Failed to execute the job, will send response with error to request: " + reqId, res.getException());

            proc.sendJobResponse(convertToErrorJobResponse(reqId, consistentId, res.getException()));
        }

        return WAIT;
    }

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Request arg) throws IgniteException {
        reqId = arg.getId();
        jobCnt = subgrid.size();
        consistentId = String.valueOf(ignite.localNode().consistentId());

        Map<ExecuteActionJob, ClusterNode> map = subgrid.stream()
            .collect(Collectors.toMap(n -> new ExecuteActionJob(arg), identity()));

        DistributedActionProcessor proc =
            ((ManagementConsoleAgent)ignite.context().managementConsole()).distributedActionProcessor();

        proc.sendTaskResponse(
            new TaskResponse()
                .setId(reqId)
                .setStatus(RUNNING)
                .setNodeConsistentId(consistentId)
                .setJobCount(jobCnt)
        );

        return map;
    }

    /** {@inheritDoc} */
    @Override public TaskResponse reduce(List<ComputeJobResult> results) throws IgniteException {
        return new TaskResponse()
            .setId(reqId)
            .setStatus(hasFailedJobs ? FAILED : COMPLETED)
            .setNodeConsistentId(consistentId)
            .setJobCount(jobCnt);
    }

    /**
     * Execute action job.
     */
    private static class ExecuteActionJob extends ComputeJobAdapter {
        /** Ignite. */
        @IgniteInstanceResource
        protected IgniteEx ignite;

        /** Job context. */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /** Response. */
        private JobResponse res;

        /**
         * @param arg Argument.
         */
        public ExecuteActionJob(Request arg) {
            setArguments(arg);
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            Request req = argument(0);
            String consistentId = String.valueOf(ignite.localNode().consistentId());

            if (res != null)
                return res;

            jobCtx.holdcc();

            ManagementConsoleAgent agent = (ManagementConsoleAgent)ignite.context().managementConsole();

            agent.actionDispatcher().dispatch(req)
                .thenApply(IgniteFuture::get)
                .thenApply(r -> res = new JobResponse()
                    .setRequestId(req.getId())
                    .setStatus(COMPLETED)
                    .setResult(r)
                    .setNodeConsistentId(consistentId)
                )
                .exceptionally(e -> res = convertToErrorJobResponse(req.getId(), consistentId, e.getCause()))
                .thenAccept(r -> jobCtx.callcc());

            return null;
        }
    }
}
