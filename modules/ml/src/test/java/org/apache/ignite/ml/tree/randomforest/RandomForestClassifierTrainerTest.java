/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.tree.randomforest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.math3.util.Precision;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.OnMajorityPredictionsAggregator;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.LabeledDummyVectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.randomforest.data.TreeNode;
import org.apache.ignite.ml.tree.randomforest.data.TreeRoot;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import static org.apache.ignite.ml.tree.randomforest.data.FeaturesCountSelectionStrategies.SQRT;
import static org.apache.ignite.ml.tree.randomforest.data.TreeNode.Type.LEAF;
import static org.junit.Assert.*;

/**
 * Tests for {@link RandomForestClassifierTrainer}.
 */
public class RandomForestClassifierTrainerTest extends TrainerTest {
    /** */
    @Test
    public void testFit() throws IOException {
        System.out.println("##### Reading data...");

        Map<Integer, Vector> data = readDataFromCsvAsVector();

        System.out.println("##### Training data...");

        RandomForestClassifierTrainer classifier = createTrainer();

        Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST);
        ModelsComposition randomForestModel = classifier.fit(data, 1, vectorizer);

        System.out.println("##### Finished training, checking the accurancy...");

        AtomicInteger amountOfErrors = new AtomicInteger(0);
        AtomicInteger totalAmount = new AtomicInteger(0);

        data.entrySet().stream().forEach(entry -> {
            Vector val = entry.getValue();
            Vector inputs = val.copyOfRange(1, val.size());
            double groundTruth = val.get(0);

            double prediction = randomForestModel.predict(inputs);

            totalAmount.incrementAndGet();
            if (!Precision.equals(groundTruth, prediction, Precision.EPSILON)) {
                amountOfErrors.incrementAndGet();
            }
        });

        System.out.println("\n##### Evaluated model on " + totalAmount + " data points.");

        // >>> Accuracy 0.9706862091938707
        System.out.println("\n##### Absolute amount of errors " + amountOfErrors);
        System.out.println("\n##### Accuracy " + (1 - amountOfErrors.get() / (double) totalAmount.get()));

    }

    private static RandomForestClassifierTrainer createTrainer() {
        List<FeatureMeta> features = new ArrayList<>();
        String[] featureName = WfDeltaVectorizer.features();
        for (int i = 0; i < featureName.length; i++) {
            features.add(new FeatureMeta(featureName[i], i, false));
        }

        return new RandomForestClassifierTrainer(features)
                .withAmountOfTrees(101) // 101
                .withFeaturesCountSelectionStrgy(SQRT)
                .withMaxDepth(60)
                .withSeed(777); // we get a different tree each run despite specifying a seed
    }

    private static Map<Integer, Vector> readDataFromCsvAsVector() throws IOException {
        Resource file = new ClassPathResource("data.txt");
        Map<Integer, Vector> data = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            List<String> lines = reader.lines().collect(Collectors.toList());
            List<List<Double>> doublest = lines.stream()
                    .map(l -> Arrays.stream(l.split(",")).map(Double::parseDouble).collect(Collectors.toList()))
                    .collect(Collectors.toList());
            for (int i = 0; i < doublest.size(); i++) {
                List<Double> doubles = doublest.get(i);
                Vector v = new DenseVector(doubles.stream().mapToDouble(Double::doubleValue).toArray());
                data.put(i, v);
            }
        }
        return data;
    }
    /**
     * Go through the tree and find a node branch that has repeating feature + value.
     */
    private static TreeNode findDuplicatedNode(TreeNode node) {
        if (node.getType() == LEAF) {
            return null;
        }

        TreeNode left = node.getLeft();
        if (getFeatureId(node) == getFeatureId(left) && getVal(node) == getVal(left)) {
            return left;
        }

        TreeNode inLeftBranch = findDuplicatedNode(left);
        if (inLeftBranch != null) {
            return inLeftBranch;
        }

        TreeNode right = node.getRight();
        if (getFeatureId(node) == getFeatureId(right) && getVal(node) == getVal(right)) {
            return right;
        }

        return findDuplicatedNode(right);
    }

    /**
     * Get node's value
     */
    private static double getVal(TreeNode node) {
        return GridTestUtils.getFieldValue(node, TreeNode.class, "val");
    }

    /**
     * Get node's feature id
     */
    private static int getFeatureId(TreeNode node) {
        return GridTestUtils.getFieldValue(node, TreeNode.class, "featureId");
    }
}
