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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.agent.dto.action.AuthenticateCredentials;
import org.apache.ignite.agent.dto.action.JobResponse;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.IgniteTestResources;

import static java.util.Collections.singleton;
import static org.apache.ignite.agent.dto.action.Status.COMPLETED;
import static org.apache.ignite.agent.dto.action.Status.FAILED;

/**
 * Abstract action controller with authentication config.
 */
public abstract class AbstractActionControllerWithAuthenticationTest extends AbstractActionControllerTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, IgniteTestResources rsrcs) {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName, rsrcs);

        configuration.setAuthenticationEnabled(true);

        return configuration;
    }

    /**
     * @param creds Credentials.
     */
    protected UUID authenticate(AuthenticateCredentials creds) {
        final AtomicReference<UUID> sesId = new AtomicReference<>();
        Request authReq = new Request()
            .setId(UUID.randomUUID())
            .setAction("SecurityActions.authenticate")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setArgument(creds);

        executeAction(authReq, (res) -> {
            JobResponse r = F.first(res);

            if (r != null && r.getStatus() == COMPLETED && r.getResult() != null) {
                sesId.set((UUID.fromString((String) r.getResult())));

                return true;
            }

            if (r != null && r.getStatus() == FAILED) {
                sesId.set(null);

                return true;
            }

            return false;
        });

        return sesId.get();
    }
}
