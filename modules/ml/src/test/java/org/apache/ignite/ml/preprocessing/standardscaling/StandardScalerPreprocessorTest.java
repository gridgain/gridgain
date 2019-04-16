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

package org.apache.ignite.ml.preprocessing.standardscaling;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link StandardScalerPreprocessor}.
 */
public class StandardScalerPreprocessorTest {
    /** Test {@code apply()} method. */
    @Test
    public void testApply() {
        double[][] inputData = new double[][] {
            {0, 2., 4., .1},
            {0, 1., -18., 2.2},
            {1, 4., 10., -.1},
            {1, 0., 22., 1.3}
        };
        double[] means = new double[] {0.5, 1.75, 4.5, 0.875};
        double[] sigmas = new double[] {0.5, 1.47901995, 14.51723114, 0.93374247};

        StandardScalerPreprocessor<Integer, Vector> preprocessor = new StandardScalerPreprocessor<>(
            means,
            sigmas,
            (k, v) -> v
        );

        double[][] expectedData = new double[][] {
            {-1., 0.16903085, -0.03444183, -0.82999331},
            {-1., -0.50709255, -1.54988233, 1.41902081},
            {1., 1.52127766, 0.37886012, -1.04418513},
            {1., -1.18321596, 1.20546403, 0.45515762}
        };

        for (int i = 0; i < inputData.length; i++)
            assertArrayEquals(expectedData[i], preprocessor.apply(i, VectorUtils.of(inputData[i])).asArray(), 1e-8);
    }
}
