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

import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.DoubleStream;

/**
 * Mixture of distributions class where each component has own probability and probability of input vector can be
 * computed as a sum of likelihoods of each component.
 *
 * @param <C> distributions mixture component class.
 */
public abstract class DistributionMixture<C extends Distribution> implements Distribution {
    /** Component probabilities. */
    private final Vector componentProbs;

    /** Distributions. */
    private final List<C> distributions;

    /** Dimension. */
    private final int dimension;

    /**
     * Creates an instance of DistributionMixture.
     *
     * @param componentProbs Component probabilities.
     * @param distributions Distributions.
     */
    public DistributionMixture(Vector componentProbs, List<C> distributions) {
        A.ensure(DoubleStream.of(componentProbs.asArray()).allMatch(v -> v > 0), "All distribution components should be greater than zero");
        componentProbs = componentProbs.divide(componentProbs.sum());

        A.ensure(!distributions.isEmpty(), "Distribution mixture should have at least one component");

        final int dimension = distributions.get(0).dimension();
        A.ensure(dimension > 0, "Dimension should be greater than zero");
        A.ensure(distributions.stream().allMatch(d -> d.dimension() == dimension), "All distributions should have same dimension");

        this.distributions = distributions;
        this.componentProbs = componentProbs;
        this.dimension = dimension;
    }

    /** {@inheritDoc} */
    @Override public double prob(Vector x) {
        return likelihood(x).sum();
    }

    /**
     * @param x Vector.
     * @return Vector consists of likelihoods of each mixture components.
     */
    public Vector likelihood(Vector x) {
        return VectorUtils.of(distributions.stream().mapToDouble(f -> f.prob(x)).toArray())
            .times(componentProbs);
    }

    /**
     * @return An amount of components.
     */
    public int countOfComponents() {
        return componentProbs.size();
    }

    /**
     * @return Component probabilities.
     */
    public Vector componentsProbs() {
        return componentProbs.copy();
    }

    /**
     * @return List of components.
     */
    public List<C> distributions() {
        return Collections.unmodifiableList(distributions);
    }

    /** {@inheritDoc} */
    @Override public int dimension() {
        return dimension;
    }
}
