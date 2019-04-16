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

import java.util.HashMap;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

/**
 * Tests of {@link ServiceInfo} class.
 */
public class ServiceInfoSelfTest {
    /** Origin node id. */
    private UUID nodeId = UUID.randomUUID();

    /** Service id. */
    private IgniteUuid srvcId = IgniteUuid.randomUuid();

    /** Service configuration. */
    private ServiceConfiguration cfg = configuration();

    /** Subject under test. */
    private ServiceInfo sut = new ServiceInfo(nodeId, srvcId, cfg);

    /**
     * Tests {@link ServiceInfo#configuration()}.
     */
    @Test
    public void testConfigurationEquality() {
        assertSame(cfg, sut.configuration());

        assertEquals(cfg.getService().getClass(), sut.serviceClass());

        assertEquals(cfg.getName(), sut.name());

        assertEquals(cfg.getCacheName(), sut.cacheName());

        assertEquals(cfg.getAffinityKey(), sut.affinityKey());

        assertEquals(cfg.getService().getClass(), sut.serviceClass());
    }

    /**
     * Tests {@link ServiceInfo#originNodeId()}.
     */
    @Test
    public void testOriginNodeIdEquality() {
        assertEquals(nodeId, sut.originNodeId());
    }

    /**
     * Tests {@link ServiceInfo#serviceId()}.
     */
    @Test
    public void testServiceNodeEquality() {
        assertEquals(srvcId, sut.serviceId());
    }

    /**
     * Tests {@link ServiceInfo#topologySnapshot()}.
     */
    @Test
    public void testTopologySnapshotEquality() {
        assertEquals(new HashMap<>(), sut.topologySnapshot());

        HashMap<UUID, Integer> top = new HashMap<>();

        top.put(nodeId, 5);

        sut.topologySnapshot(top);

        assertEquals(top, sut.topologySnapshot());

        assertNotSame(top, sut.topologySnapshot());
    }

    /**
     * @return Service configuration.
     */
    private ServiceConfiguration configuration() {
        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName("testConfig");
        cfg.setTotalCount(10);
        cfg.setMaxPerNodeCount(3);
        cfg.setCacheName("testCacheName");
        cfg.setAffinityKey("testAffKey");
        cfg.setService(new TestService());
        cfg.setNodeFilter(ClusterNode::isLocal);

        return cfg;
    }

    /**
     * Tests service implementation.
     */
    private static class TestService implements Service {
        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }
    }
}
