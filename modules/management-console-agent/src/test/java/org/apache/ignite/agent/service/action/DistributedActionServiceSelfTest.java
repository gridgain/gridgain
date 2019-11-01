package org.apache.ignite.agent.service.action;

import java.util.UUID;
import org.apache.ignite.agent.action.controller.AbstractActionControllerTest;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

import static org.apache.ignite.agent.dto.action.ActionStatus.COMPLETED;
import static org.apache.ignite.agent.dto.action.ActionStatus.FAILED;
import static org.apache.ignite.agent.dto.action.ResponseError.INTERNAL_ERROR_CODE;
import static org.apache.ignite.agent.dto.action.ResponseError.PARSE_ERROR_CODE;

/**
 * Test for distributed action service.
 */
public class DistributedActionServiceSelfTest extends AbstractActionControllerTest {
    /**
     * Should execute action on node by specific node in request.
     */
    @Test
    public void shouldExecuteActionOnNonCoordinatorNode() throws Exception {
        IgniteEx ignite = startGrid(1);

        UUID nid = ignite.cluster().localNode().id();
        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("ActionControllerForTests.nodeIdAction")
            .setNodeId(nid);

        executeAction(req, r -> r.getStatus() == COMPLETED && r.getResult().equals(nid.toString()));
    }

    /**
     * Should send error response on response with invalid node id.
     */
    @Test
    public void shouldSendErrorResponseWithInvalidNodeId() {
        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("ActionControllerForTests.nodeIdAction")
            .setNodeId(UUID.randomUUID());

        executeAction(req, (r) -> r.getStatus() == FAILED && r.getError().getCode() == INTERNAL_ERROR_CODE);
    }

    /**
     * Should send error response on response with invalid argument.
     */
    @Test
    public void shouldSendErrorResponseWithInvalidArgument() {
        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("BaselineActions.updateAutoAdjustAwaitingTime")
            .setArgument("value");

        executeAction(req, (r) -> r.getStatus() == FAILED && r.getError().getCode() == PARSE_ERROR_CODE);
    }

    /**
     * Should send error response on response with incorrect action.
     */
    @Test
    public void shouldSendErrorResponseWithIncorrectAction() {
        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("InvalidAction.updateAutoAdjustEnabled")
            .setArgument(true);

        executeAction(req, (r) -> r.getStatus() == FAILED && r.getError().getCode() == PARSE_ERROR_CODE);
    }
}
