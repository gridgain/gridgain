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

import org.gridgain.dto.action.Request;
import org.junit.Test;

import java.util.UUID;

import static org.gridgain.dto.action.ActionStatus.FAILED;

public class ActionControllerBaseTest extends AbstractActionControllerTest{
    @Test
    public void shouldSendErrorResponseWithInvalidArgument() {
        Request req = new Request()
                .setId(UUID.randomUUID())
                .setAction("BaselineActions.updateAutoAdjustAwaitingTime")
                .setArgument("value");

        executeAction(req, (r) -> r.getStatus() == FAILED && r.getError().getCode() == -32700);
    }

    @Test
    public void shouldSendErrorResponseWithIncorrectAction() {
        Request req = new Request()
                .setId(UUID.randomUUID())
                .setAction("InvalidAction.updateAutoAdjustEnabled")
                .setArgument(true);

        executeAction(req, (r) -> r.getStatus() == FAILED && r.getError().getCode() == -32700);
    }
}
