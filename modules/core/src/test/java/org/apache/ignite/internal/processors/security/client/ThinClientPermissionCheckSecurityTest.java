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

package org.apache.ignite.internal.processors.security.client;

import java.util.Map;
import org.apache.ignite.internal.processors.security.AbstractTestSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.UserAttributesFactory;
import org.apache.ignite.internal.processors.security.impl.TestAdditionalSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Security tests for thin client.
 */
@RunWith(JUnit4.class)
public class ThinClientPermissionCheckSecurityTest extends ThinClientPermissionCheckTest {
    /** {@inheritDoc} */
    @Override protected AbstractTestSecurityPluginProvider securityPluginProvider(String instanceName,
        TestSecurityData... clientData) {
        return new TestAdditionalSecurityPluginProvider(
            "srv_" + instanceName,
            null,
            ALLOW_ALL,
            false,
            true,
            clientData
        );
    }

    /** {@inheritDoc} */
    @Override protected Map<String, String> userAttributres() {
        return new UserAttributesFactory().create();
    }
}
