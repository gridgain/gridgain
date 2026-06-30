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

package org.apache.ignite.internal.processors.cluster;

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.BaselineAutoAdjustMXBean;
import org.apache.ignite.testframework.junits.SystemPropertiesList;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_AUTO_ADJUST_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_DISTRIBUTED_META_STORAGE_FEATURE;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class BaselineAutoAdjustMXBeanTest extends GridCommonAbstractTest {
    /** */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testTimeoutAndEnabledFlag() throws Exception {
        IgniteEx ignite = (IgniteEx)startGrid();

        IgniteClusterEx cluster = ignite.cluster();

        BaselineAutoAdjustMXBean bltMxBean = bltMxBean();

        assertTrue(cluster.isBaselineAutoAdjustEnabled());
        assertTrue(bltMxBean.isAutoAdjustmentEnabled());

        assertEquals(0L, cluster.baselineAutoAdjustTimeout());
        assertEquals(0L, bltMxBean.getAutoAdjustmentTimeout());

        cluster.baselineAutoAdjustEnabled(false);
        assertFalse(bltMxBean.isAutoAdjustmentEnabled());

        cluster.baselineAutoAdjustTimeout(30_000L);
        assertEquals(30_000L, bltMxBean.getAutoAdjustmentTimeout());

        bltMxBean.setAutoAdjustmentEnabled(true);
        assertTrue(cluster.isBaselineAutoAdjustEnabled());

        bltMxBean.setAutoAdjustmentEnabled(false);
        assertFalse(cluster.isBaselineAutoAdjustEnabled());

        bltMxBean.setAutoAdjustmentTimeout(60_000L);
        assertEquals(60_000L, cluster.baselineAutoAdjustTimeout());

        assertEquals("NOT_SCHEDULED", bltMxBean.getTaskState());

        bltMxBean.setAutoAdjustmentEnabled(true);

        startGrid(1);

        long timeUntilAutoAdjust = bltMxBean.getTimeUntilAutoAdjust();
        assertTrue(Long.toString(timeUntilAutoAdjust), timeUntilAutoAdjust > 0 && timeUntilAutoAdjust < 60_000L);

        assertEquals("SCHEDULED", bltMxBean.getTaskState());
    }

    /** */
    @Test
    @WithSystemProperty(key = "IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE", value = "true")
    public void testTimeoutAndEnabledFlagForScaleUpAndScaleDownSeparately() throws Exception {
        IgniteEx ignite = (IgniteEx)startGrid();

        IgniteClusterEx cluster = ignite.cluster();

        BaselineAutoAdjustMXBean bltMxBean = bltMxBean();

        assertTrue(cluster.isBaselineAutoAdjustEnabled());
        assertTrue(bltMxBean.isAutoAdjustmentEnabled());

        assertTrue(cluster.isBaselineAutoAdjustEnabled(true));
        assertTrue(bltMxBean.isAutoAdjustmentEnabled(true));

        assertEquals(0L, cluster.baselineAutoAdjustTimeout(true));
        assertEquals(0L, bltMxBean.getAutoAdjustmentTimeout(true));

        assertTrue(cluster.isBaselineAutoAdjustEnabled(false));
        assertTrue(bltMxBean.isAutoAdjustmentEnabled(false));

        assertEquals(0L, cluster.baselineAutoAdjustTimeout(false));
        assertEquals(0L, bltMxBean.getAutoAdjustmentTimeout(false));

        cluster.baselineAutoAdjustEnabled(true, false);
        assertFalse(bltMxBean.isAutoAdjustmentEnabled(true));
        assertTrue(bltMxBean.isAutoAdjustmentEnabled(false));

        cluster.baselineAutoAdjustTimeout(true, 31_000L);
        cluster.baselineAutoAdjustTimeout(false, 32_000L);
        assertEquals(31_000L, bltMxBean.getAutoAdjustmentTimeout(true));
        assertEquals(32_000L, bltMxBean.getAutoAdjustmentTimeout(false));

        bltMxBean.setAutoAdjustmentEnabled(true, true);
        bltMxBean.setAutoAdjustmentEnabled(false, false);
        assertTrue(cluster.isBaselineAutoAdjustEnabled(true));
        assertFalse(cluster.isBaselineAutoAdjustEnabled(false));

        bltMxBean.setAutoAdjustmentEnabled(true, false);
        bltMxBean.setAutoAdjustmentEnabled(false, false);
        assertFalse(cluster.isBaselineAutoAdjustEnabled(true));
        assertFalse(cluster.isBaselineAutoAdjustEnabled(false));

        bltMxBean.setAutoAdjustmentTimeout(true, 20_000L);
        assertEquals(20_000L, cluster.baselineAutoAdjustTimeout(true));

        bltMxBean.setAutoAdjustmentTimeout(false, 20_000L);
        assertEquals(20_000L, cluster.baselineAutoAdjustTimeout(false));

        assertEquals("NOT_SCHEDULED", bltMxBean.getTaskState(true));
        assertEquals("NOT_SCHEDULED", bltMxBean.getTaskState(false));

        bltMxBean.setAutoAdjustmentEnabled(true, true);

        startGrid(1);

        long timeUntilAutoAdjust = bltMxBean.getTimeUntilAutoAdjust(true);
        assertTrue(Long.toString(timeUntilAutoAdjust), timeUntilAutoAdjust > 0 && timeUntilAutoAdjust < 20_000L);

        assertEquals("SCHEDULED", bltMxBean.getTaskState(true));
        assertEquals("NOT_SCHEDULED", bltMxBean.getTaskState(false));

        assertTrue(
            "Failed to wait for changing baseline in " + 20_000 * 2 + " ms.",
            waitForCondition(() -> ignite.cluster().currentBaselineTopology().size() == 2, 20_000 * 2)
        );

        bltMxBean.setAutoAdjustmentEnabled(true, false);
        bltMxBean.setAutoAdjustmentEnabled(false, true);

        stopGrid(1);

        timeUntilAutoAdjust = bltMxBean.getTimeUntilAutoAdjust(false);
        assertTrue(Long.toString(timeUntilAutoAdjust), timeUntilAutoAdjust > 0 && timeUntilAutoAdjust < 20_000L);

        assertEquals("SCHEDULED", bltMxBean.getTaskState(false));
        assertEquals("NOT_SCHEDULED", bltMxBean.getTaskState(true));
    }

    /** */
    @Test
    @SystemPropertiesList({
        @WithSystemProperty(key = IGNITE_DISTRIBUTED_META_STORAGE_FEATURE, value = "true"),
        @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_FEATURE, value = "false"),
    })
    public void testBeanNotFoundBecauseFeatureDisabled() throws Exception {
        startGrid();

        assertNull(bltMxBean());
    }

    /**
     *
     */
    private BaselineAutoAdjustMXBean bltMxBean() throws Exception {
        ObjectName mBeanName = U.makeMBeanName(getTestIgniteInstanceName(), "Baseline",
            BaselineAutoAdjustMXBeanImpl.class.getSimpleName());

        MBeanServer mBeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mBeanSrv.isRegistered(mBeanName))
            return null;

        Class<BaselineAutoAdjustMXBean> itfCls = BaselineAutoAdjustMXBean.class;

        return MBeanServerInvocationHandler.newProxyInstance(mBeanSrv, mBeanName, itfCls, true);
    }
}
