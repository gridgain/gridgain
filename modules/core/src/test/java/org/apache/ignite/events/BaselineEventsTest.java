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

package org.apache.ignite.events;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public abstract class BaselineEventsTest extends GridCommonAbstractTest {
    /** */
    private int[] includedEvtTypes = EventType.EVTS_ALL;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConnectorConfiguration(new ConnectorConfiguration())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                    )
                    .setWalSegments(3)
                    .setWalSegmentSize(512 * 1024)
            )
            .setConsistentId(igniteInstanceName)
            .setIncludeEventTypes(includedEvtTypes);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    protected abstract void listen(IgniteEx ignite, IgnitePredicate<Event> lsnr, int... types);

    /** */
    @Test
    public void testChangeBltWithPublicApi() throws Exception {
        startGrid(0).cluster().active(true);

        AtomicBoolean baselineChanged = new AtomicBoolean();

        listen(
            startGrid(1),
            event -> {
                baselineChanged.set(true);

                BaselineChangedEvent baselineChangedEvt = (BaselineChangedEvent)event;

                assertEquals(2, baselineChangedEvt.baselineNodes().size());

                return true;
            },
            EventType.EVT_BASELINE_CHANGED
        );

        grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

        assertTrue(GridTestUtils.waitForCondition(baselineChanged::get, 3_000));
    }

    /** */
    @Test
    public void testDeactivateActivate() throws Exception {
        IgniteEx ignite = startGrids(2);

        AtomicBoolean baselineChanged = new AtomicBoolean();

        listen(
            ignite,
            event -> {
                baselineChanged.set(true);

                return true;
            },
            EventType.EVT_BASELINE_CHANGED
        );

        ignite.cluster().active(true);

        assertTrue(GridTestUtils.waitForCondition(baselineChanged::get, 3_000));
        baselineChanged.set(false);

        ignite.cluster().active(false);
        ignite.cluster().active(true);

        assertFalse(GridTestUtils.waitForCondition(baselineChanged::get, 3_000));
    }
}
