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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

/**
 *
 */
public class TestAdditionalSecurityPluginProvider extends TestSecurityPluginProvider {
    /** Security additional attribute name. */
    public static final String ADDITIONAL_SECURITY_CLIENT_VERSION_ATTR = "add.sec.cliVer";

    /** Security additional attribute value. */
    public static final String ADDITIONAL_SECURITY_CLIENT_VERSION = "client v1";

    /** Check ssl certificates. */
    protected final boolean checkAddPass;

    /** */
    public TestAdditionalSecurityPluginProvider(String login, String pwd, SecurityPermissionSet perms,
        boolean globalAuth, boolean checkAddPass, TestSecurityData... clientData) {
        super(login, pwd, perms, globalAuth, clientData);

        this.checkAddPass = checkAddPass;
    }

    /** {@inheritDoc} */
    @Override protected GridSecurityProcessor securityProcessor(GridKernalContext ctx) {
        return new TestAdditionalSecurityProcessor(ctx,
            new TestSecurityData(login, pwd, perms),
            Arrays.asList(clientData),
            globalAuth,
            checkAddPass);
    }
}
