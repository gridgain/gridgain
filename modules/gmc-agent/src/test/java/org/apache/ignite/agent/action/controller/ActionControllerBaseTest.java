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

import org.apache.ignite.agent.dto.action.ActionStatus;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.agent.dto.action.ResponseError;
import org.junit.Test;

import java.util.UUID;

/**
 * Action controller base test.
 */
public class ActionControllerBaseTest extends AbstractActionControllerTest {
    /**
     * Should send error response with on response with invalid argument.
     */
    @Test
    public void shouldSendErrorResponseWithInvalidArgument() {
        Request req = new Request()
                .setId(UUID.randomUUID())
                .setAction("BaselineActions.updateAutoAdjustAwaitingTime")
                .setArgument("value");

        executeAction(req, (r) -> r.getStatus() == ActionStatus.FAILED && r.getError().getCode() == ResponseError.PARSE_ERROR_CODE);
    }

    /**
     * Should send error response with on response with incorrect action.
     */
    @Test
    public void shouldSendErrorResponseWithIncorrectAction() {
        Request req = new Request()
                .setId(UUID.randomUUID())
                .setAction("InvalidAction.updateAutoAdjustEnabled")
                .setArgument(true);

        executeAction(req, (r) -> r.getStatus() == ActionStatus.FAILED && r.getError().getCode() == ResponseError.PARSE_ERROR_CODE);
    }
}
