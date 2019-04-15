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

package org.apache.ignite.ml.naivebayes.discrete;

import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.ArraysVectorizer;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Integration tests for Bernoulli naive Bayes algorithm with different datasets.
 */
public class DiscreteNaiveBayesTest {
    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;

    /** Example from book Barber D. Bayesian reasoning and machine learning. Chapter 10. */
    @Test
    public void testLearnsAndPredictCorrently() {
        double english = 1.;
        double scottish = 2.;
        Map<Integer, double[]> data = new HashMap<>();
        data.put(0, new double[] {0, 0, 1, 1, 1, english});
        data.put(1, new double[] {1, 0, 1, 1, 0, english});
        data.put(2, new double[] {1, 1, 0, 0, 1, english});
        data.put(3, new double[] {1, 1, 0, 0, 0, english});
        data.put(4, new double[] {0, 1, 0, 0, 1, english});
        data.put(5, new double[] {0, 0, 0, 1, 0, english});
        data.put(6, new double[] {1, 0, 0, 1, 1, scottish});
        data.put(7, new double[] {1, 1, 0, 0, 1, scottish});
        data.put(8, new double[] {1, 1, 1, 1, 0, scottish});
        data.put(9, new double[] {1, 1, 0, 1, 0, scottish});
        data.put(10, new double[] {1, 1, 0, 1, 1, scottish});
        data.put(11, new double[] {1, 0, 1, 1, 0, scottish});
        data.put(12, new double[] {1, 0, 1, 0, 0, scottish});
        double[][] thresholds = new double[][] {{.5}, {.5}, {.5}, {.5}, {.5}};
        DiscreteNaiveBayesTrainer trainer = new DiscreteNaiveBayesTrainer().setBucketThresholds(thresholds);

        DiscreteNaiveBayesModel model = trainer.fit(
            new LocalDatasetBuilder<>(data, 2),
            new ArraysVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        );
        Vector observation = VectorUtils.of(1, 0, 1, 1, 0);

        Assert.assertEquals(scottish, model.predict(observation), PRECISION);
    }
}
