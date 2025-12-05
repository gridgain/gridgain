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

package org.apache.ignite.util;

import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class GridCommandHandlerWithSSLTest extends GridCommandHandlerTest {
    /** {@inheritDoc} */
    @Override protected boolean sslEnabled() {
        return true;
    }

    // TODO: GG-46120 Remove this override after flaky test is fixed.
    /**
     * {@inheritDoc}
     *
     * This override is added here to enable suppression of the testIdleVerifyCheckCrcFailsOnNotIdleCluster() run just for this class.
     */
    @Test
    @Ignore("https://ggsystems.atlassian.net/browse/GG-46120")
    @Override public void testIdleVerifyCheckCrcFailsOnNotIdleCluster() throws Exception {
        super.testIdleVerifyCheckCrcFailsOnNotIdleCluster();
    }
}
