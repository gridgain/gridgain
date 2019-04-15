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
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.scalar.UniformRandomProducer;

/**
 * 2D-Vectors data stream with two separable classes.
 */
public class TwoSeparableClassesDataStream implements DataStreamGenerator {
    /** Margin. */
    private final double margin;

    /** Variance. */
    private final double variance;

    /** Seed. */
    private long seed;

    /**
     * Create an instance of TwoSeparableClassesDataStream. Note that margin can be less than zero.
     *
     * @param margin Margin.
     * @param variance Variance.
     */
    public TwoSeparableClassesDataStream(double margin, double variance) {
        this(margin, variance, System.currentTimeMillis());
    }

    /**
     * Create an instance of TwoSeparableClassesDataStream. Note that margin can be less than zero.
     *
     * @param margin Margin.
     * @param variance Variance.
     * @param seed Seed.
     */
    public TwoSeparableClassesDataStream(double margin, double variance, long seed) {
        this.margin = margin;
        this.variance = variance;
        this.seed = seed;
    }

    /** {@inheritDoc} */
    @Override public Stream<LabeledVector<Double>> labeled() {
        seed *= 2;

        double minCordVal = -variance - Math.abs(margin);
        double maxCordVal = variance + Math.abs(margin);

        return new UniformRandomProducer(minCordVal, maxCordVal, seed)
            .vectorize(2).asDataStream().labeled(this::classify)
            .map(v -> new LabeledVector<>(applyMargin(v.features()), v.label()))
            .filter(v -> between(v.features().get(0), -variance, variance))
            .filter(v -> between(v.features().get(1), -variance, variance));
    }

    /** */
    private boolean between(double x, double min, double max) {
        return x >= min && x <= max;
    }

    /** */
    private double classify(Vector v) {
        return v.get(0) - v.get(1) > 0 ? -1.0 : 1.0;
    }

    /** */
    private Vector applyMargin(Vector v) {
        Vector cp = v.copy();

        cp.set(0, cp.get(0) + Math.signum(v.get(0) - v.get(1)) * margin);
        cp.set(1, cp.get(1) - Math.signum(v.get(0) - v.get(1)) * margin);

        return cp;
    }
}
