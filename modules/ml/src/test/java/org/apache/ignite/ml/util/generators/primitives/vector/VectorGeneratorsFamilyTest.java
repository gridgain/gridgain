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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link VectorGeneratorsFamily}.
 */
public class VectorGeneratorsFamilyTest {
    /** */
    @Test
    public void testSelection() {
        VectorGeneratorsFamily family = new VectorGeneratorsFamily.Builder()
            .add(() -> VectorUtils.of(1., 2.), 0.5)
            .add(() -> VectorUtils.of(1., 2.), 0.25)
            .add(() -> VectorUtils.of(1., 4.), 0.25)
            .build(0L);

        Map<Integer, Vector> counters = new HashMap<>();
        for (int i = 0; i < 3; i++)
            counters.put(i, VectorUtils.zeroes(2));

        int N = 50000;
        IntStream.range(0, N).forEach(i -> {
            VectorGeneratorsFamily.VectorWithDistributionId vector = family.getWithId();
            int id = vector.distributionId();
            counters.put(id, counters.get(id).plus(vector.vector()));
        });

        for (int i = 0; i < 3; i++)
            counters.put(i, counters.get(i).divide(N));

        assertArrayEquals(new double[] {0.5, 1.0}, counters.get(0).asArray(), 1e-2);
        assertArrayEquals(new double[] {0.25, .5}, counters.get(1).asArray(), 1e-2);
        assertArrayEquals(new double[] {0.25, 1.}, counters.get(2).asArray(), 1e-2);
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidParameters1() {
        new VectorGeneratorsFamily.Builder().build();
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidParameters2() {
        new VectorGeneratorsFamily.Builder().add(() -> VectorUtils.of(1.), -1.).build();
    }

    /** */
    @Test
    public void testMap() {
        VectorGeneratorsFamily family = new VectorGeneratorsFamily.Builder()
            .add(() -> VectorUtils.of(1., 2.))
            .map(g -> g.move(VectorUtils.of(1, -1)))
            .build(0L);

        assertArrayEquals(new double[] {2., 1.}, family.get().asArray(), 1e-7);
    }

    /** */
    @Test
    public void testGet() {
        VectorGeneratorsFamily family = new VectorGeneratorsFamily.Builder()
            .add(() -> VectorUtils.of(0.))
            .add(() -> VectorUtils.of(1.))
            .add(() -> VectorUtils.of(2.))
            .build(0L);

        Set<Double> validValues = DoubleStream.of(0., 1., 2.).boxed().collect(Collectors.toSet());
        for (int i = 0; i < 100; i++) {
            Vector vector = family.get();
            assertTrue(validValues.contains(vector.get(0)));
        }
    }

    /** */
    @Test
    public void testAsDataStream() {
        VectorGeneratorsFamily family = new VectorGeneratorsFamily.Builder()
            .add(() -> VectorUtils.of(0.))
            .add(() -> VectorUtils.of(1.))
            .add(() -> VectorUtils.of(2.))
            .build(0L);

        family.asDataStream().labeled().limit(100).forEach(v -> {
            assertEquals(v.features().get(0), v.label(), 1e-7);
        });
    }
}
