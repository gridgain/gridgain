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

package org.apache.ignite.ml.composition;

import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;

import java.io.Serializable;

/**
 * Various utility functions for trainers composition.
 */
public class CompositionUtils {
    /**
     * Perform blurring of model type of given trainer to {@code IgniteModel<I, O>}, where I, O are input and output
     * types of original model.
     *
     * @param trainer Trainer to coerce.
     * @param <I> Type of input of model produced by coerced trainer.
     * @param <O> Type of output of model produced by coerced trainer.
     * @param <M> Type of model produced by coerced trainer.
     * @param <L> Type of labels.
     * @return Trainer coerced to {@code DatasetTrainer<IgniteModel<I, O>, L>}.
     */
    public static <I, O, M extends IgniteModel<I, O>, L> DatasetTrainer<IgniteModel<I, O>, L> unsafeCoerce(
        DatasetTrainer<? extends M, L> trainer) {
        return new DatasetTrainer<IgniteModel<I, O>, L>() {
            /** {@inheritDoc} */
            @Override public <K, V, C extends Serializable> IgniteModel<I, O> fit(DatasetBuilder<K, V> datasetBuilder,
                Vectorizer<K, V, C, L> extractor) {
                return trainer.fit(datasetBuilder, extractor);
            }

            /** {@inheritDoc} */
            @Override public <K, V, C extends Serializable> IgniteModel<I, O> update(IgniteModel<I, O> mdl,
                DatasetBuilder<K, V> datasetBuilder,
                Vectorizer<K, V, C, L> extractor) {
                DatasetTrainer<IgniteModel<I, O>, L> trainer1 = (DatasetTrainer<IgniteModel<I, O>, L>)trainer;
                return trainer1.update(mdl, datasetBuilder, extractor);
            }

            /**
             * This method is never called, instead of constructing logic of update from
             * {@link DatasetTrainer#isUpdateable} and
             * {@link DatasetTrainer#updateModel}
             * in this class we explicitly override update method.
             *
             * @param mdl Model.
             * @return True if current critical for training parameters correspond to parameters from last training.
             */
            @Override public boolean isUpdateable(IgniteModel<I, O> mdl) {
                throw new IllegalStateException();
            }

            /**
             * This method is never called, instead of constructing logic of update from
             * {@link DatasetTrainer#isUpdateable(IgniteModel)} and
             * {@link DatasetTrainer#updateModel(IgniteModel, DatasetBuilder, Vectorizer)} 
             * in this class we explicitly override update method.
             *
             * @param mdl Model.
             * @return Updated model.
             */
            @Override protected <K, V, C extends Serializable> IgniteModel<I, O> updateModel(IgniteModel<I, O> mdl,
                DatasetBuilder<K, V> datasetBuilder,
                Vectorizer<K, V, C, L> extractor) {
                throw new IllegalStateException();
            }
        };
    }

    /**
     * Create feature extractor from given mapping {@code (key, value) -> LabeledVector}.
     *
     * @param extractor Mapping {@code (key, value) -> LabeledVector}.
     * @param <K> Type of keys.
     * @param <V> Type of values.
     * @param <L> Type of labels.
     * @return Feature extractor created from given mapping {@code (key, value) -> LabeledVector}.
     */
    public static <K, V, L> IgniteBiFunction<K, V, Vector> asFeatureExtractor(
        FeatureLabelExtractor<K, V, L> extractor) {
        return (k, v) -> extractor.extract(k, v).features();
    }

    /**
     * Label extractor feature extractor from given mapping {@code (key, value) -> LabeledVector}.
     *
     * @param extractor Mapping {@code (key, value) -> LabeledVector}.
     * @param <K> Type of keys.
     * @param <V> Type of values.
     * @param <L> Type of labels.
     * @return Label extractor created from given mapping {@code (key, value) -> LabeledVector}.
     */
    public static <K, V, L> IgniteBiFunction<K, V, L> asLabelExtractor(FeatureLabelExtractor<K, V, L> extractor) {
        return (k, v) -> extractor.extract(k, v).label();
    }

    /**
     * Label extractor feature extractor from given mapping {@code (key, value) -> LabeledVector}.
     *
     * @param featureExtractor Features extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of keys.
     * @param <V> Type of values.
     * @param <L> Type of labels.
     * @return Label extractor created from given mapping {@code (key, value) -> LabeledVector}.
     */
    public static <K, V, L> FeatureLabelExtractor<K, V, L> asFeatureLabelExtractor(
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        return (k, v) -> new LabeledVector<>(featureExtractor.apply(k, v), lbExtractor.apply(k, v));
    }
}
