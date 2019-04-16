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

package org.apache.ignite.spi.discovery;

import java.util.HashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.internal.processors.cache.CacheMetricsSnapshot;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class ClusterMetricsSnapshotSerializeCompatibilityTest extends GridCommonAbstractTest {
    /** */
    public ClusterMetricsSnapshotSerializeCompatibilityTest() {
        super(false /*don't start grid*/);
    }

    /** Marshaller. */
    private OptimizedMarshaller marshaller = marshaller();

    /** */
    @Test
    public void testSerializationAndDeserialization() throws IgniteCheckedException {
        HashMap<Integer, CacheMetricsSnapshot> metrics = new HashMap<>();

        metrics.put(1, createMetricsWithBorderMarker());

        metrics.put(2, createMetricsWithBorderMarker());

        byte[] zipMarshal = U.zip(U.marshal(marshaller, metrics));

        HashMap<Integer, CacheMetricsSnapshot> unmarshalMetrics = U.unmarshalZip(marshaller, zipMarshal, null);

        assertTrue(isMetricsEquals(metrics.get(1), unmarshalMetrics.get(1)));

        assertTrue(isMetricsEquals(metrics.get(2), unmarshalMetrics.get(2)));
    }

    /**
     * @return Test metrics.
     */
    private CacheMetricsSnapshot createMetricsWithBorderMarker() {
        CacheMetricsSnapshot metrics = new CacheMetricsSnapshot();

        GridTestUtils.setFieldValue(metrics, "rebalancingKeysRate", 1234);
        GridTestUtils.setFieldValue(metrics, "reads", 3232);

        return metrics;
    }

    /**
     * @param obj Object.
     * @param obj1 Object 1.
     */
    private boolean isMetricsEquals(CacheMetricsSnapshot obj, CacheMetricsSnapshot obj1) {
        return obj.getRebalancingKeysRate() == obj1.getRebalancingKeysRate() && obj.getCacheGets() == obj1.getCacheGets();
    }

    /**
     * @return Marshaller.
     */
    private OptimizedMarshaller marshaller() {
        U.clearClassCache();

        OptimizedMarshaller marsh = new OptimizedMarshaller();

        marsh.setContext(new MarshallerContextTestImpl());

        return marsh;
    }
}
