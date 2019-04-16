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

package org.apache.ignite.examples.ml.clustering;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.clustering.gmm.GmmModel;
import org.apache.ignite.ml.clustering.gmm.GmmTrainer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.LabeledDummyVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.stat.MultivariateGaussianDistribution;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.scalar.GaussRandomProducer;
import org.apache.ignite.ml.util.generators.primitives.scalar.RandomProducer;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorsFamily;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Example of using GMM clusterization algorithm. Gaussian Mixture Algorithm (GMM, see {@link GmmModel}, {@link
 * GmmTrainer}) can be used for input dataset data distribution representation as mixture of multivariance gaussians.
 * More info: https://en.wikipedia.org/wiki/Mixture_model#Gaussian_mixture_model .
 *
 * In this example GMM are used for gaussians shape recovering - means and covariances of them.
 */
public class GmmClusterizationExample {
    /**
     * Runs example.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        System.out.println();
        System.out.println(">>> GMM clustering algorithm over cached dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            long seed = 0;

            IgniteCache<Integer, LabeledVector<Double>> dataCache = null;
            try {
                dataCache = ignite.createCache(
                    new CacheConfiguration<Integer, LabeledVector<Double>>("GMM_EXAMPLE_CACHE")
                        .setAffinity(new RendezvousAffinityFunction(false, 10))
                );

                // Dataset consists of three gaussians where two from them are rotated onto PI/4.
                DataStreamGenerator dataStream = new VectorGeneratorsFamily.Builder().add(
                    RandomProducer.vectorize(
                        new GaussRandomProducer(0, 2., seed++),
                        new GaussRandomProducer(0, 3., seed++)
                    ).rotate(Math.PI / 4).move(VectorUtils.of(10., 10.))).add(
                    RandomProducer.vectorize(
                        new GaussRandomProducer(0, 1., seed++),
                        new GaussRandomProducer(0, 2., seed++)
                    ).rotate(-Math.PI / 4).move(VectorUtils.of(-10., 10.))).add(
                    RandomProducer.vectorize(
                        new GaussRandomProducer(0, 3., seed++),
                        new GaussRandomProducer(0, 3., seed++)
                    ).move(VectorUtils.of(0., -10.))
                ).build(seed++).asDataStream();

                AtomicInteger keyGen = new AtomicInteger();
                dataStream.fillCacheWithCustomKey(50000, dataCache, v -> keyGen.getAndIncrement());
                GmmTrainer trainer = new GmmTrainer(1);

                GmmModel mdl = trainer
                    .withMaxCountIterations(10)
                    .withMaxCountOfClusters(4)
                    .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(seed))
                    .fit(ignite, dataCache, new LabeledDummyVectorizer<>());

                System.out.println(">>> GMM means and covariances");
                for (int i = 0; i < mdl.countOfComponents(); i++) {
                    MultivariateGaussianDistribution distribution = mdl.distributions().get(i);
                    System.out.println();
                    System.out.println("============");
                    System.out.println("Component #" + i);
                    System.out.println("============");
                    System.out.println("Mean vector = ");
                    Tracer.showAscii(distribution.mean());
                    System.out.println();
                    System.out.println("Covariance matrix = ");
                    Tracer.showAscii(distribution.covariance());
                }

                System.out.println(">>>");
            } finally {
                dataCache.destroy();
            }
        }
    }
}
