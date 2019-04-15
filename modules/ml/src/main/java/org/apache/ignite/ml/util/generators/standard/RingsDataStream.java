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

package org.apache.ignite.ml.util.generators.standard;

import java.util.stream.Stream;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.scalar.GaussRandomProducer;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorsFamily;

import static org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorPrimitives.ring;

/**
 * Represents a data stream of vectors produced by family of ring-like distributions around zero blurred
 * by gauss distribution. First ring equals minRadius next ring radius = prev_radius + distanceBetweenRings.
 */
public class RingsDataStream implements DataStreamGenerator {
    /** Count of rings. */
    private final int cntOfRings;

    /** Min radius. */
    private final double minRadius;

    /** Distance between circles. */
    private final double distanceBetweenRings;

    /** Seed. */
    private long seed;

    /**
     * Create an intance of RingsDataStream.
     *
     * @param cntOfRings Count of circles.
     * @param minRadius Min radius.
     * @param distanceBetweenRings Distance between circles.
     */
    public RingsDataStream(int cntOfRings, double minRadius, double distanceBetweenRings) {
        this(cntOfRings, minRadius, distanceBetweenRings, System.currentTimeMillis());
    }

    /**
     * Create an intance of RingsDataStream.
     *
     * @param cntOfRings Count of circles.
     * @param minRadius Min radius.
     * @param distanceBetweenRings Distance between circles.
     * @param seed Seed.
     */
    public RingsDataStream(int cntOfRings, double minRadius, double distanceBetweenRings, long seed) {
        A.ensure(cntOfRings > 0, "countOfRings > 0");
        A.ensure(minRadius > 0, "minRadius > 0");
        A.ensure(distanceBetweenRings > 0, "distanceBetweenRings > 0");

        this.cntOfRings = cntOfRings;
        this.minRadius = minRadius;
        this.distanceBetweenRings = distanceBetweenRings;
        this.seed = seed;
    }

    /** {@inheritDoc} */
    @Override public Stream<LabeledVector<Double>> labeled() {
        VectorGeneratorsFamily.Builder builder = new VectorGeneratorsFamily.Builder();
        for (int i = 0; i < cntOfRings; i++) {
            final double radius = minRadius + distanceBetweenRings * i;
            final double variance = 0.1 * (i + 1);

            GaussRandomProducer gauss = new GaussRandomProducer(0, variance, seed);
            builder = builder.add(ring(radius, 0, 2 * Math.PI).noisify(gauss));
            seed *= 2;
        }

        return builder.build().asDataStream().labeled();
    }
}
