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

package org.apache.ignite.ml.clustering.gmm;

import java.util.Arrays;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.stat.MultivariateGaussianDistribution;
import org.apache.ignite.ml.structures.LabeledVector;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link GmmPartitionDataTest}.
 */
public class GmmPartitionDataTest {
    private GmmPartitionData data;

    /** */
    @Before
    public void setUp() throws Exception {
        data = new GmmPartitionData(
            Arrays.asList(
                new LabeledVector<>(VectorUtils.of(1, 0), 0.),
                new LabeledVector<>(VectorUtils.of(0, 1), 0.),
                new LabeledVector<>(VectorUtils.of(1, 1), 0.)
            ),
            new double[3][2]
        );
    }

    /** */
    @Test
    public void testEstimateLikelihoodClusters() {
        GmmPartitionData.estimateLikelihoodClusters(data, new Vector[] {
            VectorUtils.of(1.0, 0.5),
            VectorUtils.of(0.0, 0.5)
        });

        assertEquals(1.0, data.pcxi(0, 0), 1e-4);
        assertEquals(0.0, data.pcxi(1, 0), 1e-4);

        assertEquals(0.0, data.pcxi(0, 1), 1e-4);
        assertEquals(1.0, data.pcxi(1, 1), 1e-4);

        assertEquals(1.0, data.pcxi(0, 2), 1e-4);
        assertEquals(0.0, data.pcxi(1, 2), 1e-4);
    }

    /** */
    @Test
    public void testUpdatePcxi() {
        GmmPartitionData.updatePcxi(
            data,
            VectorUtils.of(0.3, 0.7),
            Arrays.asList(
                new MultivariateGaussianDistribution(VectorUtils.of(1.0, 0.5), new DenseMatrix(new double[] {0.5, 0., 0., 1.}, 2)),
                new MultivariateGaussianDistribution(VectorUtils.of(0.0, 0.5), new DenseMatrix(new double[] {1.0, 0., 0., 1.}, 2))
            )
        );

        assertEquals(0.49, data.pcxi(0, 0), 1e-2);
        assertEquals(0.50, data.pcxi(1, 0), 1e-2);

        assertEquals(0.18, data.pcxi(0, 1), 1e-2);
        assertEquals(0.81, data.pcxi(1, 1), 1e-2);

        assertEquals(0.49, data.pcxi(0, 2), 1e-2);
        assertEquals(0.50, data.pcxi(1, 2), 1e-2);
    }
}
