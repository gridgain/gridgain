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

package org.apache.ignite.ml.naivebayes.gaussian;

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
 * Complex tests for naive Bayes algorithm with different datasets.
 */
public class GaussianNaiveBayesTest {
    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;

    /**
     * An example data set from wikipedia article about Naive Bayes https://en.wikipedia.org/wiki/Naive_Bayes_classifier#Sex_classification
     */
    @Test
    public void wikipediaSexClassificationDataset() {
        Map<Integer, double[]> data = new HashMap<>();
        double male = 0.;
        double female = 1.;
        data.put(0, new double[] {male, 6, 180, 12});
        data.put(2, new double[] {male, 5.92, 190, 11});
        data.put(3, new double[] {male, 5.58, 170, 12});
        data.put(4, new double[] {male, 5.92, 165, 10});
        data.put(5, new double[] {female, 5, 100, 6});
        data.put(6, new double[] {female, 5.5, 150, 8});
        data.put(7, new double[] {female, 5.42, 130, 7});
        data.put(8, new double[] {female, 5.75, 150, 9});
        GaussianNaiveBayesTrainer trainer = new GaussianNaiveBayesTrainer();
        GaussianNaiveBayesModel model = trainer.fit(
            new LocalDatasetBuilder<>(data, 2),
            new ArraysVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST)
        );
        Vector observation = VectorUtils.of(6, 130, 8);

        Assert.assertEquals(female, model.predict(observation), PRECISION);
    }

    /** Dataset from Gaussian NB example in the scikit-learn documentation */
    @Test
    public void scikitLearnExample() {
        Map<Integer, double[]> data = new HashMap<>();
        double one = 1.;
        double two = 2.;
        data.put(0, new double[] {one, -1, 1});
        data.put(2, new double[] {one, -2, -1});
        data.put(3, new double[] {one, -3, -2});
        data.put(4, new double[] {two, 1, 1});
        data.put(5, new double[] {two, 2, 1});
        data.put(6, new double[] {two, 3, 2});
        GaussianNaiveBayesTrainer trainer = new GaussianNaiveBayesTrainer();
        GaussianNaiveBayesModel model = trainer.fit(
            new LocalDatasetBuilder<>(data, 2),
            new ArraysVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST)
        );
        Vector observation = VectorUtils.of(-0.8, -1);

        Assert.assertEquals(one, model.predict(observation), PRECISION);
    }

}
