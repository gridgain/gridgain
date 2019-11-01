package org.apache.ignite.agent.service.action;

import java.util.UUID;
import org.apache.ignite.agent.action.controller.AbstractActionControllerWithAuthenticationTest;
import org.apache.ignite.agent.dto.action.AuthenticateCredentials;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.junit.Test;

import static org.apache.ignite.agent.dto.action.ActionStatus.COMPLETED;
import static org.apache.ignite.agent.dto.action.ActionStatus.FAILED;
import static org.apache.ignite.agent.dto.action.ResponseError.AUTHENTICATION_ERROR_CODE;

public class DistributedActionServiceWithSecuritySelfTest extends AbstractActionControllerWithAuthenticationTest {
    /**
     * Should execute action on node by specific node in request.
     */
    @Test
    public void shouldExecuteActionOnNonCoordinatorNode() throws Exception {
        IgniteEx ignite = startGrid(1);
        UUID nid = ignite.cluster().localNode().id();

        UUID sesId = authenticate(new AuthenticateCredentials().setCredentials(new SecurityCredentials("ignite", "ignite")));

        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("ActionControllerForTests.nodeIdAction")
            .setNodeId(nid)
            .setSessionId(sesId);

        executeAction(req, r -> r.getStatus() == COMPLETED && r.getResult().equals(nid.toString()));
    }

    /**
     * Should authenticate and execute action.
     */
    @Test
    public void shouldExecuteActionWithAuthentication() {
        UUID sesId = authenticate(new AuthenticateCredentials().setCredentials(new SecurityCredentials("ignite", "ignite")));

        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("ActionControllerForTests.numberAction")
            .setArgument(10)
            .setSessionId(sesId);

        executeAction(req, (r) -> r.getStatus() == COMPLETED);
    }

    /**
     * Should send error response when user don't provide session id for executing secure action.
     */
    @Test
    public void shouldSendErrorResponseOnExecutingSecuredActionWithoutAthentication() {
        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("ActionControllerForTests.numberAction")
            .setArgument(10);

        executeAction(req, (r) -> r.getStatus() == FAILED && r.getError().getCode() == AUTHENTICATION_ERROR_CODE);
    }

    /**
     * Should send error response when user provide invalid session id for executing secure action.
     */
    @Test
    public void shouldSendErrorResponseOnExecutingSecuredActionWithInvalidSessionId() {
        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("ActionControllerForTests.numberAction")
            .setArgument(10)
            .setSessionId(UUID.randomUUID());

        executeAction(req, (r) -> r.getStatus() == FAILED && r.getError().getCode() == AUTHENTICATION_ERROR_CODE);
    }
}
