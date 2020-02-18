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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.agent.action.controller.AbstractActionControllerWithAuthenticationTest;
import org.apache.ignite.agent.dto.action.AuthenticateCredentials;
import org.apache.ignite.agent.dto.action.JobResponse;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.agent.dto.action.TaskResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.junit.Before;
import org.junit.Test;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.agent.dto.action.ResponseError.AUTHENTICATION_ERROR_CODE;
import static org.apache.ignite.agent.dto.action.Status.COMPLETED;
import static org.apache.ignite.agent.dto.action.Status.FAILED;
import static org.apache.ignite.agent.dto.action.Status.RUNNING;

/**
 * Distributed action service with enabled authentication.
 */
public class DistributedActionProcessorWithAuthenticationTest extends AbstractActionControllerWithAuthenticationTest {
    /** {@inheritDoc} */
    @Before
    @Override public void startup() throws Exception {
        startup0(3);
    }

    /**
     * Should execute action on coordinator node by specific node ID in request.
     */
    @Test
    public void shouldExecuteActionOnCoordinatorNode() {
        UUID sesId = authenticate(new AuthenticateCredentials().setCredentials(new SecurityCredentials("ignite", "ignite")));

        UUID crdId = cluster.localNode().id();
        String consistentId = String.valueOf(cluster.localNode().consistentId());

        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("IgniteTestActionController.nodeIdAction")
            .setNodeIds(singleton(crdId))
            .setSessionId(sesId);

        executeAction(req, jobRes -> {
            List<TaskResponse> taskRes = taskResults(req.getId());

            Optional<TaskResponse> runningTask = taskRes.stream().filter(r -> r.getStatus() == RUNNING).findFirst();
            Optional<TaskResponse> completedTask = taskRes.stream().filter(r -> r.getStatus() == COMPLETED).findFirst();

            if (runningTask.isPresent() && completedTask.isPresent())
                return jobRes.size() == completedTask.get().getJobCount();

            return false;
        });

        JobResponse res = F.first(jobResults(req.getId()));

        assertEquals(consistentId, res.getNodeConsistentId());
        assertEquals(crdId, UUID.fromString((String) res.getResult()));
    }

    /**
     * Should execute action on nodes by specific node ID's in request.
     */
    @Test
    public void shouldExecuteActionOnNonCoordinatorNodes() {
        UUID sesId = authenticate(new AuthenticateCredentials().setCredentials(new SecurityCredentials("ignite", "ignite")));

        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("IgniteTestActionController.nodeIdAction")
            .setNodeIds(nonCrdNodeIds)
            .setSessionId(sesId);

        executeAction(req, jobRes -> {
            List<TaskResponse> taskRes = taskResults(req.getId());

            Optional<TaskResponse> runningTask = taskRes.stream().filter(r -> r.getStatus() == RUNNING).findFirst();
            Optional<TaskResponse> completedTask = taskRes.stream().filter(r -> r.getStatus() == COMPLETED).findFirst();

            if (runningTask.isPresent() && completedTask.isPresent()) {
                Set<UUID> results = jobRes.stream()
                    .map(r -> UUID.fromString(r.getResult().toString()))
                    .collect(Collectors.toSet());

                return jobRes.size() == completedTask.get().getJobCount() && results.equals(nonCrdNodeIds);
            }

            return false;
        });

        List<JobResponse> responses = jobResults(req.getId());
        
        boolean responsesHasCorrectConsistentIds = nonCrdNodeConsistentIds.containsAll(
            responses
                .stream()
                .map(JobResponse::getNodeConsistentId)
                .collect(toSet())
        );

        assertTrue(responsesHasCorrectConsistentIds);
    }

    /**
     * Should execute action on all nodes.
     */
    @Test
    public void shouldExecuteActionOnAllNodes() {
        UUID sesId = authenticate(new AuthenticateCredentials().setCredentials(new SecurityCredentials("ignite", "ignite")));

        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("IgniteTestActionController.nodeIdAction")
            .setSessionId(sesId);

        executeAction(req, jobRes -> {
            List<TaskResponse> taskRes =taskResults(req.getId());

            Optional<TaskResponse> runningTask = taskRes.stream().filter(r -> r.getStatus() == RUNNING).findFirst();
            Optional<TaskResponse> completedTask = taskRes.stream().filter(r -> r.getStatus() == COMPLETED).findFirst();

            if (runningTask.isPresent() && completedTask.isPresent()) {
                Set<UUID> results = jobRes.stream()
                    .map(r -> UUID.fromString(r.getResult().toString()))
                    .collect(Collectors.toSet());

                return results.equals(allNodeIds) && completedTask.get().getJobCount() == allNodeIds.size();
            }

            return false;
        });

        List<JobResponse> responses = jobResults(req.getId());
        
        boolean responsesHasCorrectConsistentIds = allNodeConsistentIds.containsAll(
            responses
                .stream()
                .map(JobResponse::getNodeConsistentId)
                .collect(toSet())
        );

        assertTrue(responsesHasCorrectConsistentIds);
    }

    /**
     * Should execute action on all nodes with one node stop after 1 second.
     */
    @Test
    public void shouldExecuteActionOnAllNodesWithNodeStop() {
        UUID sesId = authenticate(new AuthenticateCredentials().setCredentials(new SecurityCredentials("ignite", "ignite")));

        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("IgniteTestActionController.nodeIdActionWithSleep")
            .setArgument(5000)
            .setSessionId(sesId);

        executeActionAndStopNode(req, 1000, 1, res -> {
            List<TaskResponse> taskRes =taskResults(req.getId());

            Optional<TaskResponse> runningTask = taskRes.stream().filter(r -> r.getStatus() == RUNNING).findFirst();
            Optional<TaskResponse> failedTask = taskRes.stream().filter(r -> r.getStatus() == FAILED).findFirst();

            if (runningTask.isPresent() && failedTask.isPresent()) {
                long failedJobCnt = res.stream()
                    .filter(r -> r.getStatus() == FAILED)
                    .count();

                return res.size() == failedTask.get().getJobCount() && failedJobCnt == 1;
            }

            return false;
        });
    }

    /**
     * Should send error response when user don't provide session id for executing secure action.
     */
    @Test
    public void shouldSendErrorResponseOnExecutingSecuredActionWithoutAuthentication() {
        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("IgniteTestActionController.numberAction")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setArgument(10);

        executeAction(req, (res) -> {
            JobResponse r = F.first(res);
            TaskResponse taskRes = taskResult(req.getId());

            return taskRes.getStatus() == FAILED && r.getError().getCode() == AUTHENTICATION_ERROR_CODE;
        });
    }

    /**
     * Should send error response when user provide invalid session id for executing secure action.
     */
    @Test
    public void shouldSendErrorResponseOnExecutingSecuredActionWithInvalidSessionId() {
        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("IgniteTestActionController.numberAction")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setArgument(10)
            .setSessionId(UUID.randomUUID());

        executeAction(req, (res) -> {
            JobResponse r = F.first(res);
            TaskResponse taskRes = taskResult(req.getId());

            return taskRes.getStatus() == FAILED && r.getError().getCode() == AUTHENTICATION_ERROR_CODE;
        });
    }
}
