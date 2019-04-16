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

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@code DiscreteNaiveBayesModel} */
public class DiscreteNaiveBayesModelTest {
    /** */
    @Test
    public void testPredictWithTwoClasses() {
        double first = 1;
        double second = 2;
        double[][][] probabilities = new double[][][] {
            {{.5, .5}, {.2, .3, .5}, {2. / 3., 1. / 3.}, {.4, .1, .5}, {.5, .5}},
            {{0, 1}, {1. / 7, 2. / 7, 4. / 7}, {4. / 7, 3. / 7}, {2. / 7, 3. / 7, 2. / 7}, {4. / 7, 3. / 7,}}
        };

        double[] classProbabilities = new double[] {6. / 13, 7. / 13};
        double[][] thresholds = new double[][] {{.5}, {.2, .7}, {.5}, {.5, 1.5}, {.5}};
        DiscreteNaiveBayesModel mdl = new DiscreteNaiveBayesModel(probabilities, classProbabilities, new double[] {first, second}, thresholds, new DiscreteNaiveBayesSumsHolder());
        Vector observation = VectorUtils.of(2, 0, 1, 2, 0);

        Assert.assertEquals(second, mdl.predict(observation), 0.0001);
    }

}
