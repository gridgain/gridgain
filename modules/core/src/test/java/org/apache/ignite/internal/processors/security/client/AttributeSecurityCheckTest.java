/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.security.client;

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.processors.security.UserAttributesFactory;
import org.apache.ignite.internal.processors.security.impl.TestAuthenticationContextSecurityPluginProvider;
import org.apache.ignite.plugin.PluginProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Checks user attributes presents in GridClient messages.
 */
@RunWith(JUnit4.class)
public class AttributeSecurityCheckTest extends CommonSecurityCheckTest {
    /** */
    private static Map<String, Object> userAttrs;

    /** {@inheritDoc} */
    @Override protected PluginProvider<?> getPluginProvider(String name) {
        return new TestAuthenticationContextSecurityPluginProvider(name, null, ALLOW_ALL,
            globalAuth, true, (ctx) -> userAttrs = ctx.nodeAttributes(), clientData());
    }

    /** {@inheritDoc} */
    @Override protected GridClientConfiguration getGridClientConfiguration() {
        return super.getGridClientConfiguration()
            .setUserAttributes(userAttributes());
    }

    /** */
    @Test
    public void testUserAttributesInMessage() throws Exception {
        Ignite ignite = startGrids(2);

        assertEquals(2, ignite.cluster().topologyVersion());

        ignite.cluster().active(true);

        try (GridClient client = GridClientFactory.start(getGridClientConfiguration())) {
            assertTrue(client.connected());

            assertEquals("val", userAttrs.get("key"));

            GridClientClusterState state = client.state();

            // Close a coordinator to force the client to send a CLUSTER_CURRENT_STATE message to the other node
            // in the state.state() statement.
            ignite.close();

            userAttrs = null;

            state.active();

            assertEquals("val", userAttrs.get("key"));
        }
    }

    /**
     * @return User attributes.
     */
    private Map<String, String> userAttributes() {
        Map<String, String> attrs = new UserAttributesFactory().create();

        attrs.put("key", "val");

        return attrs;
    }
}
