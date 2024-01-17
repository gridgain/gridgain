/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.security.tracing;

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.spi.tracing.NoopTracingSpi;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.junit.Test;

import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.spi.tracing.Scope.EXCHANGE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests tracing configuration security.
 */
public class TracingSecurityTest extends AbstractSecurityTest {
    /** EXCHANGE scope specific coordinates. */
    static final TracingConfigurationCoordinates EXCHANGE_SCOPE_SPECIFIC_COORDINATES =
        new TracingConfigurationCoordinates.Builder(EXCHANGE).build();

    /** Test tracing configuration parameter. */
    private static final TracingConfigurationParameters EXCHANGE_TRACING_CONFIGURATION_PARAMETER =
        new TracingConfigurationParameters.Builder().withIncludedScopes(Collections.singleton(EXCHANGE)).build();

    /** Node name that allows to change tracing configuration. */
    private static final String SERVER_NAME = "server-node";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setTracingSpi(new NoopTracingSpi());

        return cfg;
    }

    @Override
    protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Tests that updating tracing configuration requires {@link SecurityPermission#TRACING_CONFIGURATION_UPDATE} permission.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdatingTracingConfigurationAllowed() throws Exception {
        Ignite srv = startGrid(
            SERVER_NAME,
            SecurityPermissionSetBuilder.create()
                .appendTracingPermissions(SecurityPermission.TRACING_CONFIGURATION_UPDATE)
                .appendSystemPermissions(JOIN_AS_SERVER)
                .defaultAllowAll(false)
                .build(),
            false);

        srv.tracingConfiguration()
            .set(EXCHANGE_SCOPE_SPECIFIC_COORDINATES, EXCHANGE_TRACING_CONFIGURATION_PARAMETER);

        srv.tracingConfiguration()
            .reset(EXCHANGE_SCOPE_SPECIFIC_COORDINATES);

        srv.tracingConfiguration()
            .resetAll(EXCHANGE);
    }

    /**
     * Tests that updating tracing configuration throws {@link SecurityException}
     * if node has no {@link SecurityPermission#TRACING_CONFIGURATION_UPDATE} permission.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdatingTracingConfigurationDisallowed() throws Exception {
        Ignite srv = startGrid(
            SERVER_NAME,
            SecurityPermissionSetBuilder.create()
                .appendSystemPermissions(JOIN_AS_SERVER)
                .defaultAllowAll(false)
                .build(),
            false);

        assertThrows(
            log,
            () -> srv.tracingConfiguration().set(EXCHANGE_SCOPE_SPECIFIC_COORDINATES, EXCHANGE_TRACING_CONFIGURATION_PARAMETER),
            SecurityException.class,
            null);

        assertThrows(
            log,
            () -> srv.tracingConfiguration().reset(EXCHANGE_SCOPE_SPECIFIC_COORDINATES),
            SecurityException.class,
            null);

        assertThrows(
            log,
            () -> srv.tracingConfiguration().resetAll(EXCHANGE),
            SecurityException.class,
            null);
    }

    /**
     * Tests that reading tracing configuration do not require any permissions.
     */
    @Test
    public void testGettingTracingParametersAllowed() throws Exception {
        Ignite srv = startGrid(
            SERVER_NAME,
            SecurityPermissionSetBuilder.create()
                .appendSystemPermissions(JOIN_AS_SERVER)
                .defaultAllowAll(false)
                .build(),
            false);

        srv.tracingConfiguration().get(EXCHANGE_SCOPE_SPECIFIC_COORDINATES);

        srv.tracingConfiguration().getAll(EXCHANGE);
    }
}
