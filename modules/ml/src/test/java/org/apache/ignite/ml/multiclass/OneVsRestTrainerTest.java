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

package org.apache.ignite.ml.multiclass;

import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.ArraysVectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionModel;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionSGDTrainer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link OneVsRestTrainer}.
 */
public class OneVsRestTrainerTest extends TrainerTest {
    /**
     * Test trainer on 2 linearly separable sets.
     */
    @Test
    public void testTrainWithTheLinearlySeparableCase() {
        Map<Integer, double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            cacheMock.put(i, twoLinearlySeparableClasses[i]);

        LogisticRegressionSGDTrainer binaryTrainer = new LogisticRegressionSGDTrainer()
            .withUpdatesStgy(new UpdatesStrategy<>(new SimpleGDUpdateCalculator(0.2),
                SimpleGDParameterUpdate.SUM_LOCAL, SimpleGDParameterUpdate.AVG))
            .withMaxIterations(1000)
            .withLocIterations(10)
            .withBatchSize(100)
            .withSeed(123L);

        OneVsRestTrainer<LogisticRegressionModel> trainer = new OneVsRestTrainer<>(binaryTrainer);

        MultiClassModel mdl = trainer.fit(
            cacheMock, parts,
            new ArraysVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST)
        );

        Assert.assertTrue(!mdl.toString().isEmpty());
        Assert.assertTrue(!mdl.toString(true).isEmpty());
        Assert.assertTrue(!mdl.toString(false).isEmpty());

        TestUtils.assertEquals(1, mdl.predict(VectorUtils.of(-100, 0)), PRECISION);
        TestUtils.assertEquals(0, mdl.predict(VectorUtils.of(100, 0)), PRECISION);
    }

    /** */
    @Test
    public void testUpdate() {
        Map<Integer, double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++)
            cacheMock.put(i, twoLinearlySeparableClasses[i]);

        LogisticRegressionSGDTrainer binaryTrainer = new LogisticRegressionSGDTrainer()
            .withUpdatesStgy(new UpdatesStrategy<>(new SimpleGDUpdateCalculator(0.2),
                SimpleGDParameterUpdate.SUM_LOCAL, SimpleGDParameterUpdate.AVG))
            .withMaxIterations(1000)
            .withLocIterations(10)
            .withBatchSize(100)
            .withSeed(123L);

        OneVsRestTrainer<LogisticRegressionModel> trainer = new OneVsRestTrainer<>(binaryTrainer);

        Vectorizer<Integer, double[], Integer, Double> vectorizer = new ArraysVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST);
        MultiClassModel originalMdl = trainer.fit(
            cacheMock, parts,
            vectorizer
        );

        MultiClassModel updatedOnSameDS = trainer.update(
            originalMdl,
            cacheMock,
            parts,
            vectorizer
        );

        MultiClassModel updatedOnEmptyDS = trainer.update(
            originalMdl,
            new HashMap<Integer, double[]>(),
            parts,
            vectorizer
        );

        List<Vector> vectors = Arrays.asList(
            VectorUtils.of(-100, 0),
            VectorUtils.of(100, 0)
        );

        for (Vector vec : vectors) {
            TestUtils.assertEquals(originalMdl.predict(vec), updatedOnSameDS.predict(vec), PRECISION);
            TestUtils.assertEquals(originalMdl.predict(vec), updatedOnEmptyDS.predict(vec), PRECISION);
        }
    }
}
