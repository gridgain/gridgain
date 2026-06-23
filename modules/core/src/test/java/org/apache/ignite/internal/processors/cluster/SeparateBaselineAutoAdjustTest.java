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

package org.apache.ignite.internal.processors.cluster;

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_AUTO_ADJUST_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Checks existing scenarios for baseline auto adjust as well as new ones for scale up and scale down separately.
 */
@WithSystemProperty(key = "IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE", value = "true")
public class SeparateBaselineAutoAdjustTest extends BaselineAutoAdjustTest {
    private int scaleUpAutoAdjustTimeout;

    private int scaleDownAutoAdjustTimeout;

    /** {@inheritDoc} */
    @Before
    @Override public void before() throws Exception {
        super.before();

        scaleUpAutoAdjustTimeout = autoAdjustTimeout;

        scaleDownAutoAdjustTimeout = autoAdjustTimeout;
    }

    /** {@inheritDoc} */
    @Override public void setBaselineAutoAdjustEnabled(IgniteEx ignite, boolean enabled) {
        super.setBaselineAutoAdjustEnabled(ignite, enabled);

        ignite.cluster().baselineScaleUpAutoAdjustEnabled(enabled);
        ignite.cluster().baselineScaleDownAutoAdjustEnabled(enabled);
    }

    /** {@inheritDoc} */
    @Override public void setBaselineAutoAdjustTimeout(IgniteEx ignite, int timeout) {
        super.setBaselineAutoAdjustTimeout(ignite, timeout);

        ignite.cluster().baselineScaleUpAutoAdjustTimeout(timeout);
        ignite.cluster().baselineScaleDownAutoAdjustTimeout(timeout);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testAutoAdjustWithSeparateScaleUpBeforeScaleDown() throws Exception {
        scaleDownAutoAdjustTimeout = 5_000 * 2;

        IgniteEx ignite0 = startGrids(3);

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        ignite0.cluster().baselineScaleUpAutoAdjustEnabled(true);
        ignite0.cluster().baselineScaleUpAutoAdjustTimeout(scaleUpAutoAdjustTimeout);

        ignite0.cluster().baselineScaleDownAutoAdjustEnabled(true);
        ignite0.cluster().baselineScaleDownAutoAdjustTimeout(scaleDownAutoAdjustTimeout);

        ignite0.cluster().state(ClusterState.ACTIVE);

        startGrid(3);

        stopGrid(1);

        assertTrue(waitForCondition(
            () -> ignite0.cluster().currentBaselineTopology().size() == 4,
            scaleUpAutoAdjustTimeout * 2
        ));

        assertTrue(waitForCondition(
            () -> ignite0.cluster().currentBaselineTopology().size() == 3,
            scaleDownAutoAdjustTimeout * 2
        ));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testAutoAdjustWithSeparateScaleUpAfterScaleDown() throws Exception {
        scaleUpAutoAdjustTimeout = 5_000 * 2;

        IgniteEx ignite0 = startGrids(3);

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        ignite0.cluster().baselineScaleUpAutoAdjustEnabled(true);
        ignite0.cluster().baselineScaleUpAutoAdjustTimeout(scaleUpAutoAdjustTimeout);

        ignite0.cluster().baselineScaleDownAutoAdjustEnabled(true);
        ignite0.cluster().baselineScaleDownAutoAdjustTimeout(scaleDownAutoAdjustTimeout);

        ignite0.cluster().state(ClusterState.ACTIVE);

        stopGrid(1);

        startGrid(3);

        assertTrue(waitForCondition(
            () -> ignite0.cluster().currentBaselineTopology().size() == 2,
            scaleDownAutoAdjustTimeout * 2
        ));

        assertTrue(waitForCondition(
            () -> ignite0.cluster().currentBaselineTopology().size() == 3,
            scaleUpAutoAdjustTimeout * 2
        ));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testAutoAdjustWithOnlyScaleUpEnabled() throws Exception {
        IgniteEx ignite0 = startGrids(3);

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        if (isPersistent())
            ignite0.cluster().baselineScaleUpAutoAdjustEnabled(true);
        else
            ignite0.cluster().baselineScaleDownAutoAdjustEnabled(false);

        ignite0.cluster().baselineScaleUpAutoAdjustTimeout(scaleUpAutoAdjustTimeout);

        ignite0.cluster().state(ClusterState.ACTIVE);

        stopGrid(1);

        startGrid(3);

        assertTrue(waitForCondition(
            () -> ignite0.cluster().currentBaselineTopology().size() == 4,
            scaleUpAutoAdjustTimeout * 2
        ));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testAutoAdjustWithOnlyScaleDownEnabled() throws Exception {
        IgniteEx ignite0 = startGrids(3);

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        if (isPersistent())
            ignite0.cluster().baselineScaleDownAutoAdjustEnabled(true);
        else
            ignite0.cluster().baselineScaleUpAutoAdjustEnabled(false);

        ignite0.cluster().baselineScaleDownAutoAdjustTimeout(scaleDownAutoAdjustTimeout);

        ignite0.cluster().state(ClusterState.ACTIVE);

        startGrid(3);

        stopGrid(1);

        assertTrue(waitForCondition(
            () -> ignite0.cluster().currentBaselineTopology().size() == 2,
            scaleDownAutoAdjustTimeout * 2
        ));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testSeparateAutoAdjustShouldBeEnabledExplicitly() throws Exception {
        IgniteEx ignite0 = startGrids(3);

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        ignite0.cluster().state(ClusterState.ACTIVE);

        startGrid(3);

        stopGrid(1);

        assertTrue(waitForCondition(
            () -> ignite0.cluster().currentBaselineTopology().size() == 3,
            autoAdjustTimeout * 2
        ));
    }

    /**
     * @throws Exception if failed.
     */
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_FEATURE, value = "false")
    @WithSystemProperty(key = IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE, value = "false")
    @Override public void testBaselineAutoAdjustDisableBecauseFlagIsSetToFalse() throws Exception {
        super.testBaselineAutoAdjustDisableBecauseFlagIsSetToFalse();
    }
}
