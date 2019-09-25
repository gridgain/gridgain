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

package org.apache.ignite.internal.product;

import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * The test checks that some features is not available in ignite core module.
 */
public class FeaturesIsNotAvailableTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void indexingFeatureIsNotAvailable() throws Exception {
        assertFalse(IgniteFeatures.nodeSupports(IgniteFeatures.allFeatures(grid().context()), IgniteFeatures.INDEXING));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void schedulingFeatureIsNotAvailable() throws Exception {
        assertTrue(IgniteFeatures.nodeSupports(IgniteFeatures.allFeatures(grid().context()), IgniteFeatures.WC_SCHEDULING_NOT_AVAILABLE));
    }
}
