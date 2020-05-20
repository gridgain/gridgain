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

package org.apache.ignite.internal.processors.security.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

/**
 *
 */
public class TestAuthenticationContextSecurityPluginProvider extends TestAdditionalSecurityPluginProvider {
    /** Authentication handler. */
    private Consumer<AuthenticationContext> hndlr;

    /** */
    public TestAuthenticationContextSecurityPluginProvider(String login, String pwd, SecurityPermissionSet perms,
        boolean globalAuth, boolean checkAddPass, Consumer<AuthenticationContext> hndlr,
        TestSecurityData... clientData) {
        super(login, pwd, perms, globalAuth, checkAddPass, clientData);

        this.hndlr = hndlr;
    }

    /** {@inheritDoc} */
    @Override protected GridSecurityProcessor securityProcessor(GridKernalContext ctx) {
        return new TestAuthenticationContextSecurityProcessor(ctx,
            new TestSecurityData(login, pwd, perms),
            Arrays.asList(clientData),
            globalAuth,
            checkAddPass,
            hndlr);
    }

    /**
     * Security processor for test AuthenticationContext with user attributes.
     */
    private static class TestAuthenticationContextSecurityProcessor extends TestAdditionalSecurityProcessor {
        /** Authentication context handler. */
        private Consumer<AuthenticationContext> hndlr;

        /**
         * Constructor.
         */
        public TestAuthenticationContextSecurityProcessor(GridKernalContext ctx, TestSecurityData nodeSecData,
            Collection<TestSecurityData> predefinedAuthData, boolean globalAuth, boolean checkSslCerts,
            Consumer<AuthenticationContext> hndlr) {
            super(ctx, nodeSecData, predefinedAuthData, globalAuth, checkSslCerts);

            this.hndlr = hndlr;
        }

        /** {@inheritDoc} */
        @Override public SecurityContext authenticate(AuthenticationContext authCtx) throws IgniteCheckedException {
            hndlr.accept(authCtx);

            return super.authenticate(authCtx);
        }
    }
}
