/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Thin client connections limit tests.
 */
public class ConnectionLimitTest extends AbstractThinClientTest {
    /**
     * Default timeout value.
     */
    private static final int MAX_CONNECTIONS = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setClientConnectorConfiguration(
            new ClientConnectorConfiguration().setMaxConnectionCount(MAX_CONNECTIONS));
    }

    /** */
    @SuppressWarnings("MismatchedReadAndWriteOfArray")
    @Test
    public void testConnectionsRejectedOnLimitReached() throws Exception {
        try (Ignite ignored = startGrid(0)) {
            IgniteClient[] clients = new IgniteClient[MAX_CONNECTIONS];
            for (int i = 0; i < MAX_CONNECTIONS; ++i) {
                //noinspection resource
                clients[i] = startClient(0);
            }

            GridTestUtils.assertThrows(log(),
                    () -> startClient(0),
                    ClientProtocolError.class,
                    "Connection limit reached: " + MAX_CONNECTIONS);
        }
    }
}
