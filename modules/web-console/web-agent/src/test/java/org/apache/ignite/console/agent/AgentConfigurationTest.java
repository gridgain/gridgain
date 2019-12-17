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
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.apache.ignite.console.agent.AgentUtils.split;

/**
 * Agent configuration tests.
 */
public class AgentConfigurationTest {
    /**
     * @param fileName Configuration file name.
     * @return Parsed configuration.
     */
    private AgentConfiguration parse(String fileName) {
        URL cfgUrl = AgentConfiguration.class.getClassLoader().getResource(fileName);

        assertNotNull(cfgUrl);

        AgentConfiguration cfg = AgentLauncher.parseArgs(new String[]{"-c", cfgUrl.getFile()});

        assertNotNull(cfg);

        return cfg;
    }


    /**
     * Test that agent will accept tokens from user input.
     *
     * @param userInput User input.
     */
    private void testWithUserPrompt(String userInput) {
        InputStream prev = System.in;

        try {
            // Emulate user input from keyboard.
            System.setIn(new ByteArrayInputStream((userInput + "\n\n").getBytes()));

            AgentConfiguration cfg = parse("test-empty.properties");

            assertNotNull(cfg);

            List<String> tokens = split(userInput);

            assertEquals(tokens.size(), cfg.tokens().size());

            for (int i = 0; i < tokens.size(); i++)
                assertEquals(tokens.get(i), cfg.tokens().get(i));
        }
        finally {
            System.setIn(prev);
        }
    }

    /**
     * GG-25379 Test case 1:
     * 1. Start agent with "test-empty.properties".
     * 2. Verify the User is prompted for token.
     * 3. Enter single token.
     * 4. Verify the Agent configuration initialized with the token provided, without throwing errors/exceptions.
     */
    @Test
    public void shouldPromptForSingleToken() {
        testWithUserPrompt("one-token");
    }

    /**
     * GG-25379 Testcase 2:
     * 1. Start agent with "test-empty.properties".
     * 2. Verify the User is prompted for tokens.
     * 3. Enter multiple tokens: "aa, bb,cc, ".
     * 4. Verify the Agent configuration initialized with the tokens provided, without throwing errors/exceptions.
     */
    @Test
    public void shouldPromptForSeveralTokens() {
        testWithUserPrompt("aa, bb,cc, ");
    }

    /**
     * GG-25379 Testcase 3:
     * 1. Load configuration from "test-empty.properties".
     * 2. Verify that all fields [tokens, serverUri, nodeURIs, nodeLogin, nodePassword, driversFolder, nodeKeyStore,
     *     nodeKeyStorePassword, nodeTrustStore, nodeTrustStorePassword, serverKeyStore, serverKeyStorePassword,
     *     serverTrustStore, serverTrustStorePassword, cipherSuites] are populated with null.
     *
     * @throws Exception If failed.
     */
    @Test
    public void shouldLoadConfigWithEmptyValues() throws Exception {
        AgentConfiguration cfg = new AgentConfiguration();

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

        URL cfgUrl = AgentConfiguration.class.getClassLoader().getResource("test-empty.properties");

        assertNotNull(cfgUrl);

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
     * GG-25379 Testcase 4:
     * 1. Load configuration from "test-empty-lists.properties".
     * 2. Verify that all fields [tokens, serverUri, nodeURIs] are populated with null.
     *
     * @throws Exception If failed.
     */
    @Test
    public void shouldLoadConfigWithEmptyLists() throws Exception {
        AgentConfiguration cfg = new AgentConfiguration();

        assertNull(cfg.tokens());
        assertNull(cfg.nodeURIs());
        assertNull(cfg.cipherSuites());

        URL cfgUrl = AgentConfiguration.class.getClassLoader().getResource("test-empty-lists.properties");

        assertNotNull(cfgUrl);

        cfg.load(cfgUrl);

        assertNull(cfg.tokens());
        assertNull(cfg.nodeURIs());
        assertNull(cfg.cipherSuites());
    }

    /**
     * GG-25379 Testcase 7:
     * 1. Start agent with "test-token-only.properties".
     * 2. Verify the values used:
     *    tokens: token1234
     *    node-uri: http:localhost:8080
     *    server-uri: http:localhost:3000
     */
    @Test
    public void shouldLoadTokenAndUseDefaults() {
        AgentConfiguration cfg = parse("test-token-only.properties");

        assertEquals("token1234", cfg.tokens().get(0));
        assertEquals("http://localhost:8080", cfg.nodeURIs().get(0));
        assertEquals("ws://localhost:3000", cfg.serverUri());
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
}
