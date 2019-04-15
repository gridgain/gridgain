/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.service;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

/**
 * Tests of {@link ServiceDeploymentProcessId}.
 */
@RunWith(Parameterized.class)
public class ServiceDeploymentProcessIdSelfTest {
    /** Tests discovery event. */
    private final DiscoveryEvent evt;

    /** Tests topology version. */
    private final AffinityTopologyVersion topVer;

    /** Subject under test. */
    private final ServiceDeploymentProcessId sut;

    /**
     * @param data Tests data.
     */
    public ServiceDeploymentProcessIdSelfTest(IgniteBiTuple<DiscoveryEvent, AffinityTopologyVersion> data) {
        this.evt = data.get1();
        this.topVer = data.get2();

        if (evt instanceof DiscoveryCustomEvent)
            this.sut = new ServiceDeploymentProcessId(((DiscoveryCustomEvent)evt).customMessage().id());
        else
            this.sut = new ServiceDeploymentProcessId(topVer);
    }

    /**
     * @return Tests data.
     */
    @Parameterized.Parameters(name = "Test event={0}")
    public static Collection<Object[]> instancesToTest() {
        DiscoveryEvent evt = new DiscoveryEvent(
            new GridTestNode(UUID.randomUUID()), "", 10, new GridTestNode(UUID.randomUUID()));

        DiscoveryCustomEvent customEvt = new DiscoveryCustomEvent();

        customEvt.customMessage(
            new ServiceChangeBatchRequest(Collections.singletonList(
                new ServiceUndeploymentRequest(IgniteUuid.randomUuid())))
        );

        ClusterNode node = new GridTestNode(UUID.randomUUID());

        customEvt.node(node);
        customEvt.eventNode(node);

        return Arrays.asList(new Object[][] {
            {new IgniteBiTuple<>(customEvt, new AffinityTopologyVersion(ThreadLocalRandom.current().nextLong()))},
            {new IgniteBiTuple<>(evt, new AffinityTopologyVersion(ThreadLocalRandom.current().nextLong()))}});
    }

    /** */
    @Test
    public void topologyVersion() {
        AffinityTopologyVersion topVer = evt instanceof DiscoveryCustomEvent ? null : this.topVer;

        assertEquals(topVer, sut.topologyVersion());
    }

    /** */
    @Test
    public void requestId() {
        IgniteUuid reqId = evt instanceof DiscoveryCustomEvent ? ((DiscoveryCustomEvent)evt).customMessage().id() : null;

        assertEquals(reqId, sut.requestId());
    }
}