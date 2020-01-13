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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class ClusterActivationStartedEventTest extends GridCommonAbstractTest {
    /** */
    private int[] includedEvtTypes = EventType.EVTS_ALL;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setIncludeEventTypes(includedEvtTypes);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testEventsDisabledByDefault() throws Exception {
        //noinspection ZeroLengthArrayAllocation
        includedEvtTypes = new int[0];

        IgniteEx ignite = startGrid(0);

        AtomicInteger evtsTriggered = new AtomicInteger();

        ignite.events().localListen(
            event -> {
                evtsTriggered.incrementAndGet();

                return true;
            },
            EventType.EVT_CLUSTER_ACTIVATION_STARTED,
            EventType.EVT_CLUSTER_DEACTIVATION_STARTED
        );

        ignite.cluster().active(false);
        ignite.cluster().active(true);

        assertEquals(0, evtsTriggered.get());
    }

    /** */
    @Test
    public void testActivationDeactivationStartedLocalEvents() throws Exception {
        AtomicBoolean activationStarted = new AtomicBoolean();
        AtomicBoolean activationFinished = new AtomicBoolean();

        AtomicBoolean deactivationStarted = new AtomicBoolean();
        AtomicBoolean deactivationFinished = new AtomicBoolean();

        IgniteEx ignite = startGrid(0);
        ignite.cluster().active(false);

        ignite.events().localListen(
            event -> {
                activationStarted.set(true);

                return true;
            },
            EventType.EVT_CLUSTER_ACTIVATION_STARTED
        );

        ignite.events().localListen(
            event -> {
                assertTrue(activationStarted.get());

                activationFinished.set(true);

                return true;
            },
            EventType.EVT_CLUSTER_ACTIVATED
        );

        ignite.events().localListen(
            event -> {
                deactivationStarted.set(true);

                return true;
            },
            EventType.EVT_CLUSTER_DEACTIVATION_STARTED
        );

        ignite.events().localListen(
            event -> {
                assertTrue(deactivationStarted.get());

                deactivationFinished.set(true);

                return true;
            },
            EventType.EVT_CLUSTER_DEACTIVATED
        );

        ignite.cluster().active(true);

        assertTrue(activationStarted.get());
        assertTrue(GridTestUtils.waitForCondition(activationFinished::get, 5_000));
        assertFalse(deactivationStarted.get());

        activationStarted.set(false);
        activationFinished.set(false);

        ignite.cluster().active(false);

        assertTrue(deactivationStarted.get());
        assertTrue(GridTestUtils.waitForCondition(deactivationFinished::get, 5_000));
        assertFalse(activationStarted.get());
    }

    /** */
    @Test
    public void testActivationDeactivationStartedRemoteEvents() throws Exception {
        AtomicBoolean activationStarted = new AtomicBoolean();
        AtomicBoolean activationFinished = new AtomicBoolean();

        AtomicBoolean deactivationStarted = new AtomicBoolean();
        AtomicBoolean deactivationFinished = new AtomicBoolean();

        IgniteEx ignite = startGrid(0);
        ignite.cluster().active(false);

        startGrid(1);

        ignite.events(ignite.cluster().forRemotes()).remoteListen(
            (id, event) -> {
                activationStarted.set(true);

                return true;
            },
            type -> true,
            EventType.EVT_CLUSTER_ACTIVATION_STARTED
        );

        ignite.events(ignite.cluster().forRemotes()).remoteListen(
            (id, event) -> {
                activationFinished.set(true);

                return true;
            },
            type -> true,
            EventType.EVT_CLUSTER_ACTIVATED
        );

        ignite.events(ignite.cluster().forRemotes()).remoteListen(
            (id, event) -> {
                deactivationStarted.set(true);

                return true;
            },
            type -> true,
            EventType.EVT_CLUSTER_DEACTIVATION_STARTED
        );

        ignite.events(ignite.cluster().forRemotes()).remoteListen(
            (id, event) -> {
                deactivationFinished.set(true);

                return true;
            },
            type -> true,
            EventType.EVT_CLUSTER_DEACTIVATED
        );

        ignite.cluster().active(true);

        assertTrue(GridTestUtils.waitForCondition(activationStarted::get, 5_000));
        assertTrue(GridTestUtils.waitForCondition(activationFinished::get, 5_000));
        assertFalse(deactivationStarted.get());

        activationStarted.set(false);
        activationFinished.set(false);

        ignite.cluster().active(false);

        assertTrue(GridTestUtils.waitForCondition(deactivationStarted::get, 5_000));
        assertTrue(GridTestUtils.waitForCondition(deactivationFinished::get, 5_000));
        assertFalse(activationStarted.get());
    }
}