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

import static org.gridgain.dto.action.ActionStatus.COMPLETED;

/**
 * Baseline actions controller test.
 */
public class BaselineActionsControllerTest extends AbstractActionControllerTest {
    /**
     * Should set the baselineAutoAdjustEnabled cluster property to {@code True}.
     */
    @Test
    public void updateAutoAdjustEnabledToTrue() {
        Request req = new Request()
                .setId(UUID.randomUUID())
                .setActionName("BaselineActions.updateAutoAdjustEnabled")
                .setArgument(true);

        executeAction(req, (r) -> r.getStatus() == COMPLETED && cluster.isBaselineAutoAdjustEnabled());
    }

    /**
     * Should set the baselineAutoAdjustEnabled cluster property to {@code False}.
     */
    @Test
    public void updateAutoAdjustEnabledToFalse() {
        Request req = new Request()
                .setId(UUID.randomUUID())
                .setActionName("BaselineActions.updateAutoAdjustEnabled")
                .setArgument(false);

        executeAction(req, (r) -> r.getStatus() == COMPLETED && !cluster.isBaselineAutoAdjustEnabled());
    }

    /**
     * Should set the baselineAutoAdjustTimeout cluster property to 10_000ms.
     */
    @Test
    public void updateAutoAdjustAwaitingTime() {
        Request req = new Request()
                .setId(UUID.randomUUID())
                .setActionName("BaselineActions.updateAutoAdjustAwaitingTime")
                .setArgument(10_000);

        executeAction(req, (r) -> r.getStatus() == COMPLETED && cluster.baselineAutoAdjustTimeout() == 10_000);
    }
}
