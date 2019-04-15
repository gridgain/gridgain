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

package org.apache.ignite.ml.math.stat;

import java.util.Arrays;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class DistributionMixtureTest {
    /** */
    private DistributionMixture<Constant> mixture;

    @Before
    public void setUp() throws Exception {
        mixture = new DistributionMixture<Constant>(
            VectorUtils.of(0.3, 0.3, 0.4),
            Arrays.asList(new Constant(0.5), new Constant(1.0), new Constant(0.))
        ) {
        };

        assertEquals(1, mixture.dimension());
        assertEquals(3, mixture.countOfComponents());
    }

    /** */
    @Test
    public void testLikelihood() {
        assertArrayEquals(
            new double[] {0.15, 0.3, 0.},
            mixture.likelihood(VectorUtils.of(1.)).asArray(), 1e-4
        );
    }

    /** */
    @Test
    public void testProb() {
        assertEquals(0.45, mixture.prob(VectorUtils.of(1.)), 1e-4);
    }

    /** */
    private static class Constant implements Distribution {
        /** Value. */
        private final double value;

        /** */
        public Constant(double value) {
            this.value = value;
        }

        /** {@inheritDoc} */
        @Override public double prob(Vector x) {
            return value;
        }

        /** {@inheritDoc} */
        @Override public int dimension() {
            return 1;
        }
    }
}
