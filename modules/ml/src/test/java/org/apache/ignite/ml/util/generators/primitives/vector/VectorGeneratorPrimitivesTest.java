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

package org.apache.ignite.ml.util.generators.primitives.vector;

import java.util.stream.IntStream;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link VectorGeneratorPrimitives}.
 */
public class VectorGeneratorPrimitivesTest {
    /** */
    @Test
    public void testConstant() {
        Vector vec = VectorUtils.of(1.0, 0.0);
        assertArrayEquals(vec.copy().asArray(), VectorGeneratorPrimitives.constant(vec).get().asArray(), 1e-7);
    }

    /** */
    @Test
    public void testZero() {
        assertArrayEquals(new double[] {0., 0.}, VectorGeneratorPrimitives.zero(2).get().asArray(), 1e-7);
    }

    /** */
    @Test
    public void testRing() {
        VectorGeneratorPrimitives.ring(1., 0, 2 * Math.PI)
            .asDataStream().unlabeled().limit(1000)
            .forEach(v -> assertEquals(v.getLengthSquared(), 1., 1e-7));

        VectorGeneratorPrimitives.ring(1., 0, Math.PI / 2)
            .asDataStream().unlabeled().limit(1000)
            .forEach(v -> {
                assertTrue(v.get(0) >= 0.);
                assertTrue(v.get(1) >= 0.);
            });
    }

    /** */
    @Test
    public void testCircle() {
        VectorGeneratorPrimitives.circle(1.)
            .asDataStream().unlabeled().limit(1000)
            .forEach(v -> assertTrue(Math.sqrt(v.getLengthSquared()) <= 1.));
    }

    /** */
    @Test
    public void testParallelogram() {
        VectorGeneratorPrimitives.parallelogram(VectorUtils.of(2., 100.))
            .asDataStream().unlabeled().limit(1000)
            .forEach(v -> {
                assertTrue(v.get(0) <= 2.);
                assertTrue(v.get(0) >= -2.);
                assertTrue(v.get(1) <= 100.);
                assertTrue(v.get(1) >= -100.);
            });
    }

    /** */
    @Test
    public void testGauss() {
        VectorGenerator gen = VectorGeneratorPrimitives.gauss(VectorUtils.of(2., 100.), VectorUtils.of(20., 1.), 10L);

        final double[] mean = new double[] {2., 100.};
        final double[] variance = new double[] {20., 1.};

        final int N = 50000;
        Vector meanStat = IntStream.range(0, N).mapToObj(i -> gen.get()).reduce(Vector::plus).get().times(1. / N);
        Vector varianceStat = IntStream.range(0, N).mapToObj(i -> gen.get().minus(meanStat))
            .map(v -> v.times(v)).reduce(Vector::plus).get().times(1. / N);

        assertArrayEquals(mean, meanStat.asArray(), 0.1);
        assertArrayEquals(variance, varianceStat.asArray(), 0.1);
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testGaussFail1() {
        VectorGeneratorPrimitives.gauss(VectorUtils.of(), VectorUtils.of());
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testGaussFail2() {
        VectorGeneratorPrimitives.gauss(VectorUtils.of(0.5, -0.5), VectorUtils.of(1.0, -1.0));
    }
}
