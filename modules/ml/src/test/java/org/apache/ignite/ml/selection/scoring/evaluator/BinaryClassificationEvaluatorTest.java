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

package org.apache.ignite.ml.selection.scoring.evaluator;

import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.knn.NNClassificationModel;
import org.apache.ignite.ml.knn.classification.KNNClassificationTrainer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.selection.scoring.metric.classification.Accuracy;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Evaluator}.
 */
public class BinaryClassificationEvaluatorTest extends TrainerTest {
    /**
     * Test evalutor and trainer on classification model y = x.
     */
    @Test
    public void testEvaluatorWithoutFilter() {
        Map<Integer, Vector> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            cacheMock.put(i, VectorUtils.of(twoLinearlySeparableClasses[i]));

        KNNClassificationTrainer trainer = new KNNClassificationTrainer();


        NNClassificationModel mdl = trainer.fit(
            cacheMock, parts,
            new DummyVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST)
        ).withK(3);

        IgniteBiFunction<Integer, Vector, Vector> featureExtractor = (k, v) -> v.copyOfRange(1, v.size());
        IgniteBiFunction<Integer, Vector, Double> lbExtractor = (k, v) -> v.get(0);
        double score = Evaluator.evaluate(cacheMock, mdl, featureExtractor, lbExtractor, new Accuracy<>());

        assertEquals(0.9839357429718876, score, 1e-12);
    }

    /**
     * Test evalutor and trainer on classification model y = x.
     */
    @Test
    public void testEvaluatorWithFilter() {
        Map<Integer, Vector> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            cacheMock.put(i, VectorUtils.of(twoLinearlySeparableClasses[i]));

        KNNClassificationTrainer trainer = new KNNClassificationTrainer();


        TrainTestSplit<Integer, Vector> split = new TrainTestDatasetSplitter<Integer, Vector>()
            .split(0.75);

        NNClassificationModel mdl = trainer.fit(
            cacheMock,
            split.getTrainFilter(),
            parts,
            new DummyVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST)
        ).withK(3);

        IgniteBiFunction<Integer, Vector, Vector> featureExtractor = (k, v) -> v.copyOfRange(1, v.size());
        IgniteBiFunction<Integer, Vector, Double> lbExtractor = (k, v) -> v.get(0);
        double score = Evaluator.evaluate(cacheMock, mdl, featureExtractor, lbExtractor, new Accuracy<>());

        assertEquals(0.9, score, 1);
    }
}
