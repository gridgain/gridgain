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

package org.apache.ignite.agent.action.controller;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.agent.AgentCommonAbstractTest;
import org.apache.ignite.agent.dto.action.JobResponse;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.agent.dto.action.TaskResponse;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Before;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.agent.StompDestinationsUtils.buildActionJobResponseDest;
import static org.apache.ignite.agent.StompDestinationsUtils.buildActionRequestTopic;
import static org.apache.ignite.agent.StompDestinationsUtils.buildActionTaskResponseDest;
import static org.apache.ignite.agent.utils.AgentObjectMapperFactory.jsonMapper;
import static org.awaitility.Awaitility.with;

/**
 * Abstract test for action controllers.
 */
public abstract class AbstractActionControllerTest extends AgentCommonAbstractTest {
    /** Mapper. */
    protected final ObjectMapper mapper = jsonMapper();

    /** All node ids. */
    protected Set<UUID> allNodeIds = new HashSet<>();

    /** All node consistent ids. */
    protected Set<String> allNodeConsistentIds = new HashSet<>();

    /** Non coordinator node ids. */
    protected Set<UUID> nonCrdNodeIds = new HashSet<>();

    /** Non coordinator node consistent ids. */
    protected Set<String> nonCrdNodeConsistentIds = new HashSet<>();

    /**
     * Start one grid instance.
     */
    @Before
    public void startup() throws Exception {
        startup0(1);
    }

    /**
     * Start grid instances.
     */
    protected void startup0(int instancesCnt) throws Exception {
        IgniteEx ignite = startGrids(instancesCnt);

        changeManagementConsoleConfig(ignite);

        cluster = ignite.cluster();
        cluster.active(true);

        allNodeIds = cluster.forServers().nodes().stream()
            .map(ClusterNode::id)
            .collect(Collectors.toSet());

        allNodeConsistentIds = cluster.forServers().nodes().stream()
            .map(ClusterNode::consistentId)
            .map(String::valueOf)
            .collect(Collectors.toSet());

        nonCrdNodeIds = cluster.forServers().nodes().stream()
            .map(ClusterNode::id)
            .filter(id -> !id.equals(cluster.localNode().id()))
            .collect(Collectors.toSet());

        nonCrdNodeConsistentIds = cluster.forServers().nodes().stream()
            .map(ClusterNode::consistentId)
            .map(String::valueOf)
            .filter(id -> !id.equals(String.valueOf(cluster.localNode().consistentId())))
            .collect(Collectors.toSet());
    }

    /**
     * Send action request and check execution result with assert function.
     *
     * @param req Request.
     * @param assertFn Assert fn.
     */
    protected void executeAction(Request req, Function<List<JobResponse>, Boolean> assertFn) {
        executeActionAndStopNode(req, 0, 0, assertFn);
    }

    /**
     * @param reqId Request ID.
     */
    protected List<TaskResponse> taskResults(UUID reqId) {
        return interceptor.getAllPayloads(buildActionTaskResponseDest(cluster.id()), TaskResponse.class).stream()
            .filter(r -> reqId.equals(r.getId()))
            .collect(toList());
    }

    /**
     * @param reqId Request ID.
     */
    protected TaskResponse taskResult(UUID reqId) {
        List<TaskResponse> taskRes = taskResults(reqId);

        return taskRes.isEmpty() ? null : F.last(taskRes);
    }

    /**
     * @param reqId Request ID.
     */
    protected List<JobResponse> jobResults(UUID reqId) {
        return interceptor.getAllPayloads(buildActionJobResponseDest(cluster.id()), JobResponse.class).stream()
            .filter(r -> reqId.equals(r.getRequestId()))
            .collect(toList());
    }

    /**
     * @param reqId Request ID.
     */
    protected JobResponse jobResult(UUID reqId) {
        List<JobResponse> jobRes = jobResults(reqId);

        return jobRes.isEmpty() ? null : F.last(jobRes);
    }

    /**
     * Send action request and check execution result with assert function and stop non coordinator node after specific timeout in ms.
     *
     * @param req Request.
     * @param timeout Timeout.
     * @param gridIdx Grid instance index.
     * @param assertFn Assert fn.
     */
    protected void executeActionAndStopNode(Request req, long timeout, int gridIdx, Function<List<JobResponse>, Boolean> assertFn) {
        assertWithPoll(
            () -> interceptor.isSubscribedOn(buildActionRequestTopic(cluster.id()))
        );

        template.convertAndSend(buildActionRequestTopic(cluster.id()), req);

        if (timeout > 0) {
            try {
                Thread.sleep(timeout);
                stopAndCancelGrid(gridIdx);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        assertWithPoll(
            () -> {
                List<JobResponse> res = jobResults(req.getId());

                return assertFn.apply(res);
            }
        );
    }

    /** {@inheritDoc} */
    @Override protected void assertWithPoll(Callable<Boolean> cond) {
        with().pollInterval(500, MILLISECONDS).await().atMost(20, SECONDS).until(cond);
    }
}
