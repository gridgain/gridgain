/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.binary;

import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.List;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;

public class BinaryConfigurationConsistencyMultiJvmSelfTest extends GridCommonAbstractTest {
    @Override protected boolean isMultiJvm() {
        return true;
    }

    @Override protected List<String> additionalRemoteJvmArgs() {
        List<String> defaultArgs = super.additionalRemoteJvmArgs();

        defaultArgs.add("-D" + IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true");

        return defaultArgs;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setBinaryConfiguration(new BinaryConfiguration());

        return cfg;
    }

    /**
     * Tests a situation when nodes having the same binary configuration but different
     * {@link org.apache.ignite.IgniteSystemProperties#IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK} flag values try
     * to join the cluster. It is expected that they must succeed.
     */
    @Test
    public void testSkipConsistencyCheckMismatch() throws Exception {
        startGrid(0);

        // This node is started on a remote JVM with a different system property value.
        startGrid(1);
    }
}
