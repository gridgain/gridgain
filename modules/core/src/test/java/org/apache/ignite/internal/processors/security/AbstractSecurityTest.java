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

package org.apache.ignite.internal.processors.security;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Common class for security tests.
 */
public class AbstractSecurityTest extends GridCommonAbstractTest {
    /** Empty array of permissions. */
    protected static final SecurityPermission[] EMPTY_PERMS = new SecurityPermission[0];

    /** Global authentication flag. */
    protected boolean globalAuth;

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    protected IgniteEx startGridAllowAll(String name) throws Exception {
        return startGrid(name, ALLOW_ALL, false);
    }

    /** */
    protected IgniteEx startClientAllowAll(String name) throws Exception {
        return startGrid(name, ALLOW_ALL, true);
    }

    /**
     * @param cfg Config.
     * @param login Login.
     * @param prmSet Prm set.
     * @param clientData Client data.
     */
    protected void initSecurity(
        IgniteConfiguration cfg,
        String login,
        SecurityPermissionSet prmSet,
        TestSecurityData... clientData
    ) {
        cfg.setPluginProviders(new TestSecurityPluginProvider(login, "", prmSet, globalAuth, clientData));
    }

    /**
     * @param name Name.
     * @param prmSet Security permission set.
     * @param isClient Is client.
     */
    protected IgniteEx startGrid(
        String name,
        SecurityPermissionSet prmSet,
        boolean isClient,
        TestSecurityData... clientData
    ) throws Exception {
        IgniteConfiguration cfg = getConfiguration(name);

        initSecurity(cfg, name, prmSet, clientData);

        return startGrid(cfg
            .setClientMode(isClient)
            .setConsistentId(name));
    }
}
