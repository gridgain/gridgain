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

import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Pseudorandom producer generating values from gauss distribution.
 */
public class GaussRandomProducer extends RandomProducerWithGenerator {
    /** Mean. */
    private final double mean;
    /** Variance. */
    private final double variance;

    /**
     * Creates an instance of GaussRandomProducer with mean = 0 and variance = 1.0.
     */
    public GaussRandomProducer() {
        this(0.0, 1.0, System.currentTimeMillis());
    }

    /**
     * Creates an instance of GaussRandomProducer with mean = 0 and variance = 1.0.
     *
     * @param seed Seed.
     */
    public GaussRandomProducer(long seed) {
        this(0.0, 1.0, seed);
    }

    /**
     * Creates an instance of GaussRandomProducer.
     *
     * @param mean Mean.
     * @param variance Variance.
     */
    public GaussRandomProducer(double mean, double variance) {
        this(mean, variance, System.currentTimeMillis());
    }

    /**
     * Creates an instance of GaussRandomProducer.
     *
     * @param mean Mean.
     * @param variance Variance.
     * @param seed Seed.
     */
    public GaussRandomProducer(double mean, double variance, long seed) {
        super(seed);

        A.ensure(variance > 0, "variance > 0");

        this.mean = mean;
        this.variance = variance;
    }

    /** {@inheritDoc} */
    @Override public Double get() {
        return mean + generator().nextGaussian() * Math.sqrt(variance);
    }
}
