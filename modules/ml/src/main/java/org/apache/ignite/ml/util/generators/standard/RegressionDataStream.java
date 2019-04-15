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
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.scalar.UniformRandomProducer;

/**
 * Represents a generator of regression data stream based on Vector->Double function where each Vector
 * was produced from hypercube with sides = [minXValue, maxXValue].
 */
public class RegressionDataStream implements DataStreamGenerator {
    /** Function. */
    private final IgniteFunction<Vector, Double> function;

    /** Min x value for each dimension. */
    private final double minXVal;

    /** Max x value. */
    private final double maxXVal;

    /** Vector size. */
    private final int vectorSize;

    /** Seed. */
    private long seed;

    /**
     * Creates an instance of RegressionDataStream.
     *
     * @param vectorSize Vector size.
     * @param function Function.
     * @param minXVal Min x value.
     * @param maxXVal Max x value.
     * @param seed Seed.
     */
    private RegressionDataStream(int vectorSize, IgniteFunction<Vector, Double> function,
        double minXVal, double maxXVal, long seed) {

        A.ensure(vectorSize > 0, "vectorSize > 0");
        A.ensure(minXVal <= maxXVal, "minXValue <= maxXValue");

        this.function = function;
        this.minXVal = minXVal;
        this.maxXVal = maxXVal;
        this.seed = seed;
        this.vectorSize = vectorSize;
    }

    /**
     * Creates an instance of RegressionDataStream.
     *
     * @param vectorSize Vector size.
     * @param function Function.
     * @param minXVal Min x value.
     * @param maxXVal Max x value.
     */
    public RegressionDataStream(int vectorSize, IgniteFunction<Vector, Double> function, double minXVal,
        double maxXVal) {
        this(vectorSize, function, minXVal, maxXVal, System.currentTimeMillis());
    }

    /** {@inheritDoc} */
    @Override public Stream<LabeledVector<Double>> labeled() {
        seed *= 2;
        return new UniformRandomProducer(minXVal, maxXVal, seed)
            .vectorize(vectorSize).asDataStream()
            .labeled(function);
    }

    /**
     * Creates two dimensional regression data stream.
     *
     * @param function Double->double function.
     * @param minXVal Min x value.
     * @param maxXVal Max x value.
     * @return RegressionDataStream instance.
     */
    public static RegressionDataStream twoDimensional(IgniteFunction<Double, Double> function,
        double minXVal, double maxXVal) {

        return twoDimensional(function, minXVal, maxXVal, System.currentTimeMillis());
    }

    /**
     * Creates two dimensional regression data stream.
     *
     * @param function Double->double function.
     * @param minXVal Min x value.
     * @param maxXVal Max x value.
     * @param seed Seed.
     * @return RegressionDataStream instance.
     */
    public static RegressionDataStream twoDimensional(IgniteFunction<Double, Double> function,
        double minXVal, double maxXVal, long seed) {

        return new RegressionDataStream(1, v -> function.apply(v.get(0)), minXVal, maxXVal, seed);
    }
}
