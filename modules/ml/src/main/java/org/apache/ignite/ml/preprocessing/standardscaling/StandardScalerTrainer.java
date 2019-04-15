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

package org.apache.ignite.ml.preprocessing.standardscaling;

import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;

/**
 * Trainer of the standard scaler preprocessor.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class StandardScalerTrainer<K, V> implements PreprocessingTrainer<K, V, Vector, Vector> {
    /** {@inheritDoc} */
    @Override public StandardScalerPreprocessor<K, V> fit(LearningEnvironmentBuilder envBuilder,
        DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> basePreprocessor) {
        StandardScalerData standardScalerData = computeSum(envBuilder, datasetBuilder, basePreprocessor);

        int n = standardScalerData.sum.length;
        long cnt = standardScalerData.cnt;
        double[] mean = new double[n];
        double[] sigma = new double[n];

        for (int i = 0; i < n; i++) {
            mean[i] = standardScalerData.sum[i] / cnt;
            double variance = (standardScalerData.squaredSum[i] - Math.pow(standardScalerData.sum[i], 2) / cnt) / cnt;
            sigma[i] = Math.sqrt(variance);
        }
        return new StandardScalerPreprocessor<>(mean, sigma, basePreprocessor);
    }

    /** Computes sum, squared sum and row count. */
    private StandardScalerData computeSum(LearningEnvironmentBuilder envBuilder,
        DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> basePreprocessor) {
        try (Dataset<EmptyContext, StandardScalerData> dataset = datasetBuilder.build(
            envBuilder,
            (env, upstream, upstreamSize) -> new EmptyContext(),
            (env, upstream, upstreamSize, ctx) -> {
                double[] sum = null;
                double[] squaredSum = null;
                long cnt = 0;

                while (upstream.hasNext()) {
                    UpstreamEntry<K, V> entity = upstream.next();
                    Vector row = basePreprocessor.apply(entity.getKey(), entity.getValue());

                    if (sum == null) {
                        sum = new double[row.size()];
                        squaredSum = new double[row.size()];
                    }
                    else {
                        assert sum.length == row.size() : "Base preprocessor must return exactly " + sum.length
                            + " features";
                    }

                    ++cnt;
                    for (int i = 0; i < row.size(); i++) {
                        double x = row.get(i);
                        sum[i] += x;
                        squaredSum[i] += x * x;
                    }
                }
                return new StandardScalerData(sum, squaredSum, cnt);
            }
        )) {

            return dataset.compute(data -> data,
                (a, b) -> {
                    if (a == null)
                        return b;
                    if (b == null)
                        return a;

                    return a.merge(b);
                });
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
