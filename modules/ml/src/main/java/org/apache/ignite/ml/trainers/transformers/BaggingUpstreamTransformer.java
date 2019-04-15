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

package org.apache.ignite.ml.trainers.transformers;

import java.util.stream.Stream;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.UpstreamTransformer;
import org.apache.ignite.ml.dataset.UpstreamTransformerBuilder;

/**
 * This class encapsulates the logic needed to do bagging (bootstrap aggregating) by features.
 * The action of this class on a given upstream is to replicate each entry in accordance to
 * Poisson distribution.
 */
public class BaggingUpstreamTransformer implements UpstreamTransformer {
    /** Serial version uid. */
    private static final long serialVersionUID = -913152523469994149L;

    /** Ratio of subsample to entire upstream size */
    private double subsampleRatio;

    /** Seed used for generating poisson distribution. */
    private long seed;

    /**
     * Get builder of {@link BaggingUpstreamTransformer} for a model with a specified index in ensemble.
     *
     * @param subsampleRatio Subsample ratio.
     * @param mdlIdx Index of model in ensemble.
     * @param <K> Type of upstream keys.
     * @param <V> Type of upstream values.
     * @return Builder of {@link BaggingUpstreamTransformer}.
     */
    public static <K, V> UpstreamTransformerBuilder builder(double subsampleRatio, int mdlIdx) {
        return env -> new BaggingUpstreamTransformer(env.randomNumbersGenerator().nextLong() + mdlIdx, subsampleRatio);
    }

    /**
     * Construct instance of this transformer with a given subsample ratio.
     *
     * @param seed Seed used for generating poisson distribution which in turn used to make subsamples.
     * @param subsampleRatio Subsample ratio.
     */
    public BaggingUpstreamTransformer(long seed, double subsampleRatio) {
        this.subsampleRatio = subsampleRatio;
        this.seed = seed;
    }

    /** {@inheritDoc} */
    @Override public Stream<UpstreamEntry> transform(Stream<UpstreamEntry> upstream) {
        PoissonDistribution poisson = new PoissonDistribution(
            new Well19937c(seed),
            subsampleRatio,
            PoissonDistribution.DEFAULT_EPSILON,
            PoissonDistribution.DEFAULT_MAX_ITERATIONS);

        return upstream.sequential().flatMap(en -> Stream.generate(() -> en).limit(poisson.sample()));
    }
}
