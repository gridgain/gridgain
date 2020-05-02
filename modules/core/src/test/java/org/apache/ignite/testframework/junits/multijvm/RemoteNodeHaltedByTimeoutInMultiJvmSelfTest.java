/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.testframework.junits.multijvm;

import java.util.List;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.testframework.junits.multijvm.IgniteNodeRunner.IGNITE_HALT_REMOTE_TEST_NODE_TIMEOUT;

/**
 * Checks that remote node will be halted after reaching the {@link #CUSTOM_TIMEOUT} timeout.
 */
public class RemoteNodeHaltedByTimeoutInMultiJvmSelfTest extends GridCommonAbstractTest {
    /** Custom timeout. */
    private static final long CUSTOM_TIMEOUT = 5_000L;

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmArgs() {
        List<String> args = super.additionalRemoteJvmArgs();

        int index = -1;

        for (int i = 0; i < args.size(); i++) {
            if (args.get(i).contains(IGNITE_HALT_REMOTE_TEST_NODE_TIMEOUT)) {
                index = i;

                break;
            }
        }

        args.set(index, "-D" + IGNITE_HALT_REMOTE_TEST_NODE_TIMEOUT + "=" + CUSTOM_TIMEOUT);

        return args;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Checks that remote node will be halted after reaching the {@link #CUSTOM_TIMEOUT} timeout.
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        assertTrue(isRemoteJvm(1));

        IgniteEx crd = startGrids(2);

        assertEquals(2, crd.cluster().nodes().size());

        assertTrue(waitForCondition(() -> crd.cluster().nodes().size() == 1, 2 * CUSTOM_TIMEOUT));
    }
}
