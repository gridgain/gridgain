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

package org.apache.ignite.ml.composition.boosting.convergence;

import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.boosting.loss.Loss;
import org.apache.ignite.ml.dataset.feature.extractor.impl.LabeledDummyVectorizer;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.structures.LabeledVector;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** */
public abstract class ConvergenceCheckerTest {
    /** Not converged model. */
    protected ModelsComposition notConvergedMdl = new ModelsComposition(Collections.emptyList(), null) {
        @Override public Double predict(Vector features) {
            return 2.1 * features.get(0);
        }
    };

    /** Converged model. */
    protected ModelsComposition convergedMdl = new ModelsComposition(Collections.emptyList(), null) {
        @Override public Double predict(Vector features) {
            return 2 * (features.get(0) + 1);
        }
    };

    /** Features extractor. */
    protected LabeledDummyVectorizer<Integer, Double> vectorizer = new LabeledDummyVectorizer<>();

    /** Data. */
    protected Map<Integer, LabeledVector<Double>> data;

    /** */
    @Before
    public void setUp() throws Exception {
        data = new HashMap<>();
        for(int i = 0; i < 10; i ++)
            data.put(i, VectorUtils.of(i, i + 1).labeled((double)(2 * (i + 1))));
    }

    /** */
    public ConvergenceChecker<Integer, LabeledVector<Double>, Integer> createChecker(ConvergenceCheckerFactory factory,
        LocalDatasetBuilder<Integer, LabeledVector<Double>> datasetBuilder) {

        return factory.create(data.size(),
            x -> x,
            new Loss() {
                @Override public double error(long sampleSize, double lb, double mdlAnswer) {
                    return mdlAnswer - lb;
                }

                @Override public double gradient(long sampleSize, double lb, double mdlAnswer) {
                    return mdlAnswer - lb;
                }
            },
            datasetBuilder, vectorizer
        );
    }
}
