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

package org.apache.ignite.ml.util.generators.primitives.scalar;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link DiscreteRandomProducer}.
 */
public class DiscreteRandomProducerTest {
    /** */
    @Test
    public void testGet() {
        double[] probs = new double[] {0.1, 0.2, 0.3, 0.4};
        DiscreteRandomProducer producer = new DiscreteRandomProducer(0L, probs);

        Map<Integer, Double> counters = new HashMap<>();
        IntStream.range(0, probs.length).forEach(i -> counters.put(i, 0.0));

        final int N = 500000;
        Stream.generate(producer::getInt).limit(N).forEach(i -> counters.put(i, counters.get(i) + 1));
        IntStream.range(0, probs.length).forEach(i -> counters.put(i, counters.get(i) / N));

        for (int i = 0; i < probs.length; i++)
            assertEquals(probs[i], counters.get(i), 0.01);

        assertEquals(probs.length, producer.size());
    }

    /** */
    @Test
    public void testSeedConsidering() {
        DiscreteRandomProducer producer1 = new DiscreteRandomProducer(0L, 0.1, 0.2, 0.3, 0.4);
        DiscreteRandomProducer producer2 = new DiscreteRandomProducer(0L, 0.1, 0.2, 0.3, 0.4);

        assertEquals(producer1.get(), producer2.get(), 0.0001);
    }

    /** */
    @Test
    public void testUniformGeneration() {
        int N = 10;
        DiscreteRandomProducer producer = DiscreteRandomProducer.uniform(N);

        Map<Integer, Double> counters = new HashMap<>();
        IntStream.range(0, N).forEach(i -> counters.put(i, 0.0));

        final int sampleSize = 500000;
        Stream.generate(producer::getInt).limit(sampleSize).forEach(i -> counters.put(i, counters.get(i) + 1));
        IntStream.range(0, N).forEach(i -> counters.put(i, counters.get(i) / sampleSize));

        for (int i = 0; i < N; i++)
            assertEquals(1.0 / N, counters.get(i), 0.01);
    }

    /** */
    @Test
    public void testDistributionGeneration() {
        double[] probs = DiscreteRandomProducer.randomDistribution(5, 0L);
        assertArrayEquals(new double[] {0.23, 0.27, 0.079, 0.19, 0.20}, probs, 0.01);
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidDistribution1() {
        new DiscreteRandomProducer(0L, 0.1, 0.2, 0.3, 0.0);
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidDistribution2() {
        new DiscreteRandomProducer(0L, 0.1, 0.2, 0.3, 1.0);
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidDistribution3() {
        new DiscreteRandomProducer(0L, 0.1, 0.2, 0.3, 1.0, -0.6);
    }
}
