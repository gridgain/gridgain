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

package org.apache.ignite.examples.ml.tree;

import java.io.FileNotFoundException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionSGDTrainer;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.classification.BinaryClassificationMetricValues;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.util.MLSandboxDatasets;
import org.apache.ignite.ml.util.SandboxMLCache;

/**
 * Example of using classification algorithms for fraud detection problem.
 *
 * Description of models can be found in:
 *      https://en.wikipedia.org/wiki/Logistic_regression and
 *      https://en.wikipedia.org/wiki/Decision_tree_learning .
 * Original dataset can be downloaded from: https://www.kaggle.com/mlg-ulb/creditcardfraud/ .
 * Copy of dataset are stored in:  modules/ml/src/main/resources/datasets/fraud_detection.csv .
 * Score for clusterizer estimation: accuracy, recall, precision, f1-score .
 * Description of entropy can be found in: https://en.wikipedia.org/wiki/Evaluation_of_binary_classifiers .
 */
public class FraudDetectionExample {
    public static void main(String[] args) throws FileNotFoundException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = null;
            try {
                System.out.println(">>> Fill dataset cache.");
                dataCache = new SandboxMLCache(ignite).fillCacheWith(MLSandboxDatasets.FRAUD_DETECTION);

                // This vectorizer works with values in cache of Vector class.
                Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>()
                    .labeled(Vectorizer.LabelCoordinate.LAST); // LAST means "label are stored at last coordinate of vector"

                // Splits dataset to train and test samples with 80/20 proportion.
                TrainTestSplit<Integer, Vector> split = new TrainTestDatasetSplitter<Integer, Vector>().split(0.8);

                System.out.println(">>> Perform logistic regression.");
                trainAndEstimateModel(ignite, dataCache,
                    new LogisticRegressionSGDTrainer()
                        .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(0)),
                    vectorizer, split
                );

                System.out.println("\n\n>>> Perform decision tree classifier.");
                trainAndEstimateModel(ignite, dataCache,
                    new DecisionTreeClassificationTrainer()
                        .withMaxDeep(10.)
                        .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(0)),
                    vectorizer, split
                );
            }
            finally {
                dataCache.destroy();
            }
        }
    }

    /**
     * Train model and estimate it.
     *
     * @param ignite Ignite
     * @param dataCache Data set cache.
     * @param trainer Trainer.
     * @param vectorizer Upstream vectorizer.
     * @param splitter Train test splitter.
     */
    private static void trainAndEstimateModel(Ignite ignite,
        IgniteCache<Integer, Vector> dataCache,
        DatasetTrainer<? extends IgniteModel<Vector, Double>, Double> trainer,
        Vectorizer<Integer, Vector, Integer, Double> vectorizer, TrainTestSplit<Integer, Vector> splitter) {
        System.out.println(">>> Start traininig.");
        IgniteModel<Vector, Double> model = trainer.fit(
            ignite, dataCache,
            splitter.getTrainFilter(),
            vectorizer
        );

        System.out.println(">>> Perform scoring.");
        BinaryClassificationMetricValues metricValues = Evaluator.evaluate(
            dataCache,
            splitter.getTestFilter(),
            model,
            vectorizer
        );

        System.out.println(String.format(">> Model accuracy: %.2f", metricValues.accuracy()));
        System.out.println(String.format(">> Model precision: %.2f", metricValues.precision()));
        System.out.println(String.format(">> Model recall: %.2f", metricValues.recall()));
        System.out.println(String.format(">> Model f1-score: %.2f", metricValues.f1Score()));
        System.out.println(">> Confusion matrix:");
        System.out.println(">>                    fraud (ans) | not fraud (ans)");
        System.out.println(String.format(">> fraud (pred)     | %1$11.2f | %2$15.2f ", metricValues.tp(), metricValues.fp()));
        System.out.println(String.format(">> not fraud (pred) | %1$11.2f | %2$15.2f ", metricValues.fn(), metricValues.tn()));
    }
}
