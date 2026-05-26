/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.rest;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;

import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SECURITY_CHECK_FAILED;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Verifies over real HTTP that the distributed-property REST commands are wired to the correct
 * {@link SecurityPermission} entries in {@code GridRestProcessor.prepareCommandAccess}:
 * read commands require {@link SecurityPermission#ADMIN_READ_DISTRIBUTED_PROPERTY},
 * write commands require {@link SecurityPermission#ADMIN_WRITE_DISTRIBUTED_PROPERTY}.
 */
public class JettyRestProcessorPropertySecurityTest extends JettyRestProcessorCommonSelfTest {
    /** Login granted only the read distributed-property permission. */
    private static final String READ_LOGIN = "rest-read";

    /** Login granted only the write distributed-property permission. */
    private static final String WRITE_LOGIN = "rest-write";

    /** Empty password used by both test logins. */
    private static final String EMPTY_PWD = "";

    /** {@inheritDoc} */
    @Override protected boolean securityEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected String signature() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // checkpoint.deviation is registered only when persistence is enabled.
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(100 * 1024 * 1024)
                .setPersistenceEnabled(true))
            .setWalMode(WALMode.NONE);

        cfg.setDataStorageConfiguration(dsCfg);

        SecurityPermissionSet readOnly = SecurityPermissionSetBuilder.create()
            .defaultAllowAll(false)
            .appendSystemPermissions(SecurityPermission.ADMIN_READ_DISTRIBUTED_PROPERTY)
            .build();

        SecurityPermissionSet writeOnly = SecurityPermissionSetBuilder.create()
            .defaultAllowAll(false)
            .appendSystemPermissions(SecurityPermission.ADMIN_WRITE_DISTRIBUTED_PROPERTY)
            .build();

        cfg.setPluginProviders(new TestSecurityPluginProvider(
            "server", EMPTY_PWD, ALLOW_ALL, false,
            new TestSecurityData(READ_LOGIN, EMPTY_PWD, readOnly),
            new TestSecurityData(WRITE_LOGIN, EMPTY_PWD, writeOnly)
        ));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        grid(0).cluster().state(ClusterState.ACTIVE);
    }

    /**
     * Client with only ADMIN_READ_DISTRIBUTED_PROPERTY: list and get are allowed; set is denied.
     */
    @Test
    public void testReadPermissionAllowsListAndGetButDeniesSet() throws Exception {
        assertResponseSucceeded(asUser(READ_LOGIN, GridRestCommand.PROPERTY_LIST), false);
        assertResponseSucceeded(asUser(READ_LOGIN, GridRestCommand.PROPERTY_GET, "name", "checkpoint.deviation"), false);
        assertSecurityDenied(asUser(READ_LOGIN, GridRestCommand.PROPERTY_SET, "name", "checkpoint.deviation", "val", "5"));
    }

    /**
     * Client with only ADMIN_WRITE_DISTRIBUTED_PROPERTY: set is allowed; list and get are denied.
     */
    @Test
    public void testWritePermissionAllowsSetButDeniesListAndGet() throws Exception {
        assertResponseSucceeded(asUser(WRITE_LOGIN, GridRestCommand.PROPERTY_SET, "name", "checkpoint.deviation", "val", "5"), false);
        assertSecurityDenied(asUser(WRITE_LOGIN, GridRestCommand.PROPERTY_LIST));
        assertSecurityDenied(asUser(WRITE_LOGIN, GridRestCommand.PROPERTY_GET, "name", "checkpoint.deviation"));
    }

    /**
     * Execute a REST command authenticated as {@code login}.
     */
    private String asUser(String login, GridRestCommand cmd, String... params) throws Exception {
        String[] paramsWithCredentials = new String[params.length + 4];

        System.arraycopy(params, 0, paramsWithCredentials, 0, params.length);

        paramsWithCredentials[params.length] = "ignite.login";
        paramsWithCredentials[params.length + 1] = login;
        paramsWithCredentials[params.length + 2] = "ignite.password";
        paramsWithCredentials[params.length + 3] = EMPTY_PWD;

        return content(null, cmd, paramsWithCredentials);
    }

    /**
     * Asserts the security check denied the request with {@code STATUS_SECURITY_CHECK_FAILED}.
     * Kept as a custom assertion because {@code assertResponseContainsError} only verifies
     * the status is non-success — too loose for distinguishing a security denial from any other failure.
     */
    private void assertSecurityDenied(String response) throws IOException {
        JsonNode node = JSON_MAPPER.readTree(response);

        int status = node.get("successStatus").asInt();

        assertEquals("Expected STATUS_SECURITY_CHECK_FAILED, got status=" + status
                + " error=" + node.get("error").asText(),
            STATUS_SECURITY_CHECK_FAILED, status);
    }
}
