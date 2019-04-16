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

package org.apache.ignite.ml.clustering.gmm;

import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.stat.MultivariateGaussianDistribution;
import org.apache.ignite.ml.math.util.MatrixUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link GmmModelTest}.
 */
public class GmmModelTest {
    /** */
    @Test
    public void testTrivialCasesWithOneComponent() {
        Vector mean = VectorUtils.of(1., 2.);
        DenseMatrix covariance = MatrixUtil.fromList(Arrays.asList(
            VectorUtils.of(1, -0.5),
            VectorUtils.of(-0.5, 1)),
            true
        );

        GmmModel gmm = new GmmModel(
            VectorUtils.of(1.0),
            Collections.singletonList(new MultivariateGaussianDistribution(mean, covariance))
        );

        Assert.assertEquals(2, gmm.dimension());
        Assert.assertEquals(1, gmm.countOfComponents());
        Assert.assertEquals(VectorUtils.of(1.), gmm.componentsProbs());
        Assert.assertEquals(0., gmm.predict(mean), 0.01);
        Assert.assertEquals(1, gmm.likelihood(mean).size());
        Assert.assertEquals(0.183, gmm.likelihood(mean).get(0), 0.01);
        Assert.assertEquals(0.183, gmm.prob(mean), 0.01);
    }

    /** */
    @Test
    public void testTwoComponents() {
        Vector mean1 = VectorUtils.of(1., 2.);
        DenseMatrix covariance1 = MatrixUtil.fromList(Arrays.asList(
            VectorUtils.of(1, -0.25),
            VectorUtils.of(-0.25, 1)),
            true
        );

        Vector mean2 = VectorUtils.of(2., 1.);
        DenseMatrix covariance2 = MatrixUtil.fromList(Arrays.asList(
            VectorUtils.of(1, 0.5),
            VectorUtils.of(0.5, 1)),
            true
        );

        GmmModel gmm = new GmmModel(
            VectorUtils.of(0.5, 0.5),
            Arrays.asList(
                new MultivariateGaussianDistribution(mean1, covariance1),
                new MultivariateGaussianDistribution(mean2, covariance2)
            )
        );

        Assert.assertEquals(0., gmm.predict(mean1), 0.01);
        Assert.assertEquals(1., gmm.predict(mean2), 0.01);
        Assert.assertEquals(0., gmm.predict(VectorUtils.of(1.5, 1.5)), 0.01);
        Assert.assertEquals(1., gmm.predict(VectorUtils.of(3., 0.)), 0.01);
    }
}
