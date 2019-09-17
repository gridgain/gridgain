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

package org.gridgain.action.controller;

import org.apache.ignite.plugin.security.SecurityCredentials;
import org.gridgain.dto.action.AuthenticateCredentials;
import org.gridgain.dto.action.Request;
import org.junit.Test;

import java.util.UUID;

import static org.gridgain.dto.action.ActionStatus.COMPLETED;
import static org.gridgain.dto.action.ActionStatus.FAILED;

/**
 * Security actions controller test.
 */
public class SecurityActionsControllerTest extends AbstractActionControllerWithSecurityTest {
    /**
     * Should authenticate with login and password.
     */
    @Test
    public void shouldSeccesfullyAuthenticate() {
        Request req = new Request()
                .setId(UUID.randomUUID())
                .setAction("SecurityActions.authenticate")
                .setArgument(new AuthenticateCredentials().setCredentials(new SecurityCredentials("ignite", "ignite")));

        executeAction(req, (r) -> r.getStatus() == COMPLETED && UUID.fromString((String) r.getResult()) != null);
    }

    /**
     * Should send error response when user don't provide credentials.
     */
    @Test
    public void shouldSendErrorResponseWithEmptyCredentials() {
        Request req = new Request()
                .setId(UUID.randomUUID())
                .setAction("SecurityActions.authenticate")
                .setArgument(new AuthenticateCredentials());

        executeAction(req, (r) -> r.getStatus() == FAILED && r.getError().getCode() == -32001);
    }

    /**
     * Should send error response when user provide invalid credentials.
     */
    @Test
    public void shouldSendErrorResponseWithInvalidCredentials() {
        Request req = new Request()
                .setId(UUID.randomUUID())
                .setAction("SecurityActions.authenticate")
                .setArgument(new AuthenticateCredentials().setCredentials(new SecurityCredentials("ignite", "ignite2")));

        executeAction(req, (r) -> r.getStatus() == FAILED && r.getError().getCode() == -32001);
    }
}
