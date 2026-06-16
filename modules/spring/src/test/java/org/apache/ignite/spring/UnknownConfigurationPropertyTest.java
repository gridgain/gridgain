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

package org.apache.ignite.spring;

import java.net.URL;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteComponentType.SPRING;

/**
 * Checks that an unknown configuration property in the Spring XML produces a user-friendly error instead of
 * the opaque Spring "Bean property '...' is not writable" message.
 */
public class UnknownConfigurationPropertyTest extends GridCommonAbstractTest {
    /** Config that sets a property which does not exist on {@code TransactionConfiguration}. */
    private final URL cfgLocation = U.resolveIgniteUrl(
        "modules/spring/src/test/java/org/apache/ignite/spring/unknown-property.xml");

    /** Loading a config with an unknown property must fail with a clear "Configuration property ... does not exist" message. */
    @Test
    public void testUnknownPropertyReported() throws Exception {
        IgniteSpringHelper spring = SPRING.create(false);

        GridTestUtils.assertThrows(log,
            () -> spring.loadConfigurations(cfgLocation).get1(),
            IgniteCheckedException.class,
            "Configuration property TransactionConfiguration.txAwareQueriesEnabled does not exist");
    }
}
