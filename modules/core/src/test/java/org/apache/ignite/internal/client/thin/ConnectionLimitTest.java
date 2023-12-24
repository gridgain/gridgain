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
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.mxbean.ClientProcessorMXBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Thin client connections limit tests.
 */
public class ConnectionLimitTest extends AbstractThinClientTest {
    /**
     * Default timeout value.
     */
    private static final int MAX_CONNECTIONS = 4;

    /** */
    @Test
    public void testConnectionsRejectedOnLimitReached() throws Exception {
        try (Ignite ignite = startGrid(0)) {
            ClientProcessorMXBean mxBean = getMxBean(ignite.name(), "Clients",
                    ClientProcessorMXBean.class, ClientListenerProcessor.class);

            mxBean.setMaxConnectionsPerNode(MAX_CONNECTIONS);

            List<IgniteClient> clients = new ArrayList<>();
            try {
                for (int i = 0; i < MAX_CONNECTIONS; ++i) {
                    clients.add(startClient(0));
                }

                GridTestUtils.assertThrows(log(),
                        () -> startClient(0),
                        ClientProtocolError.class,
                        "Connection limit reached: " + MAX_CONNECTIONS);
            }
            finally {
                clients.forEach(IgniteClient::close);
            }
        }
    }
}
