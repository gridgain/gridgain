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

package org.apache.ignite.console.agent;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import org.junit.Test;

import static junit.framework.TestCase.assertNull;

/**
 * Agent configuration tests.
 */
public class AgentConfigurationTest {
    /**
     * GG-25379 Test case 1:
     * 1. Start agent with test-empty.properties
     * 2. Verify the User is prompted for token(s)
     * 3. Enter single token
     * 4. Verify the Agent configuration initialized with the token provided, without throwing errors/exceptions.
     */
    @Test
    public void shoudPromptForToken() {
        InputStream prev = System.in;

        try {
            URL cfgUrl = AgentConfiguration.class.getClassLoader().getResource("test-empty.properties");

            System.setIn(new ByteArrayInputStream("test-token\n\n".getBytes()));

            AgentLauncher.parseArgs(new String[] {"-c", cfgUrl.getFile()});
        }
        finally {
            System.setIn(prev);
        }
    }

    /**
     * Should correctly load values from file with empty properties.
     *
     * @throws Exception If failed.
     */
    @Test
    public void shouldLoadConfigWithEmptyValues() throws Exception {
        AgentConfiguration cfg = new AgentConfiguration();

        URL cfgUrl = AgentConfiguration.class.getClassLoader().getResource("test-empty.properties");

        assertNull(cfg.tokens());
        assertNull(cfg.serverUri());
        assertNull(cfg.nodeURIs());
        assertNull(cfg.nodeLogin());
        assertNull(cfg.nodePassword());
        assertNull(cfg.driversFolder());
        assertNull(cfg.nodeKeyStore());
        assertNull(cfg.nodeKeyStorePassword());
        assertNull(cfg.nodeTrustStore());
        assertNull(cfg.nodeTrustStorePassword());
        assertNull(cfg.serverKeyStore());
        assertNull(cfg.serverKeyStorePassword());
        assertNull(cfg.serverTrustStore());
        assertNull(cfg.serverTrustStorePassword());
        assertNull(cfg.cipherSuites());

        cfg.load(cfgUrl);

        assertNull(cfg.tokens());
        assertNull(cfg.serverUri());
        assertNull(cfg.nodeURIs());
        assertNull(cfg.nodeLogin());
        assertNull(cfg.nodePassword());
        assertNull(cfg.driversFolder());
        assertNull(cfg.nodeKeyStore());
        assertNull(cfg.nodeKeyStorePassword());
        assertNull(cfg.nodeTrustStore());
        assertNull(cfg.nodeTrustStorePassword());
        assertNull(cfg.serverKeyStore());
        assertNull(cfg.serverKeyStorePassword());
        assertNull(cfg.serverTrustStore());
        assertNull(cfg.serverTrustStorePassword());
        assertNull(cfg.cipherSuites());
    }

    /**
     * Should correctly load values from file with empty properties.
     *
     * @throws Exception If failed.
     */
    @Test
    public void shouldLoadConfigWithEmptyLists() throws Exception {
        AgentConfiguration cfg = new AgentConfiguration();

        URL cfgUrl = AgentConfiguration.class.getClassLoader().getResource("test-empty-lists.properties");

        assertNull(cfg.tokens());
        assertNull(cfg.nodeURIs());
        assertNull(cfg.cipherSuites());

        cfg.load(cfgUrl);

        assertNull(cfg.tokens());
        assertNull(cfg.nodeURIs());
        assertNull(cfg.cipherSuites());
    }
}
