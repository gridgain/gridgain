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
import org.apache.ignite.internal.processors.security.AbstractTestSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

/** */
public class TestSecurityPluginProvider extends AbstractTestSecurityPluginProvider {
    /** Login. */
    private final String login;

    /** Password. */
    private final String pwd;

    /** Permissions. */
    private final SecurityPermissionSet perms;

    /** Global auth. */
    private final boolean globalAuth;

    /** Users security data. */
    private final TestSecurityData[] clientData;

    /** */
    public TestSecurityPluginProvider(String login, String pwd, SecurityPermissionSet perms, boolean globalAuth,
        TestSecurityData... clientData) {
        this.login = login;
        this.pwd = pwd;
        this.perms = perms;
        this.globalAuth = globalAuth;
        this.clientData = clientData.clone();
    }

    /** {@inheritDoc} */
    @Override protected GridSecurityProcessor securityProcessor(GridKernalContext ctx) {
        return new TestSecurityProcessor(ctx,
            new TestSecurityData(login, pwd, perms),
            Arrays.asList(clientData),
            globalAuth);
    }
}
