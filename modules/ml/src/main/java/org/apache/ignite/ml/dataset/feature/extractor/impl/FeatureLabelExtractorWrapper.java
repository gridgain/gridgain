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

package org.apache.ignite.ml.dataset.feature.extractor.impl;

import org.apache.ignite.ml.composition.CompositionUtils;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;

import java.io.Serializable;
import java.util.List;

/**
 * Temporary class for Features/Label extracting.
 */
public class FeatureLabelExtractorWrapper<K, V, C extends Serializable, L> extends Vectorizer<K, V, C, L> {
    /** Original extractor. */
    private final FeatureLabelExtractor<K, V, L> extractor;

    /**
     * Creates an instance of FeatureLabelExtractorWrapper.
     *
     * @param extractor Features and lavels extractor.
     */
    public FeatureLabelExtractorWrapper(FeatureLabelExtractor<K, V, L> extractor) {
        this.extractor = extractor;
    }

    /**
     * @param featuresEx Method for feature vector extracting.
     * @return wrapper.
     */
    public static <K, V, C extends Serializable> FeatureLabelExtractorWrapper<K, V, C, Double> wrap(IgniteBiFunction<K, V, Vector> featuresEx) {
        return new FeatureLabelExtractorWrapper<>((k, v) -> featuresEx.apply(k, v).labeled(0.0));
    }

    public static <K, V, C extends Serializable, L> FeatureLabelExtractorWrapper<K, V, C, L> wrap(IgniteBiFunction<K, V, Vector> featuresEx,
        IgniteBiFunction<K, V, L> lbExtractor) {

        return new FeatureLabelExtractorWrapper<>(CompositionUtils.asFeatureLabelExtractor(featuresEx, lbExtractor));
    }

    /** {@inheritDoc} */
    @Override public LabeledVector<L> apply(K key, V value) {
        return extractor.extract(key, value);
    }

    /** {@inheritDoc} */
    @Override protected Double feature(C coord, K key, V value) {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override protected L label(C coord, K key, V value) {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override protected L zero() {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override protected List<C> allCoords(K key, V value) {
        throw new IllegalStateException();
    }
}
