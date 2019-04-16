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

import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * This class represents dataset mapping. This is just a tuple of two mappings: one for features and one for labels.
 *
 * @param <L1> Type of labels before mapping.
 * @param <L2> Type of labels after mapping.
 */
public interface DatasetMapping<L1, L2> {
    /**
     * Method used to map feature vectors.
     *
     * @param v Feature vector.
     * @return Mapped feature vector.
     */
    public default Vector mapFeatures(Vector v) {
        return v;
    }

    /**
     * Method used to map labels.
     *
     * @param lbl Label.
     * @return Mapped label.
     */
    public L2 mapLabels(L1 lbl);

    /**
     * Dataset mapping which maps features, leaving labels unaffected.
     *
     * @param mapper Function used to map features.
     * @param <L> Type of labels.
     * @return Dataset mapping which maps features, leaving labels unaffected.
     */
    public static <L> DatasetMapping<L, L> mappingFeatures(IgniteFunction<Vector, Vector> mapper) {
        return new DatasetMapping<L, L>() {
            /** {@inheritDoc} */
            @Override public Vector mapFeatures(Vector v) {
                return mapper.apply(v);
            }

            /** {@inheritDoc} */
            @Override public L mapLabels(L lbl) {
                return lbl;
            }
        };
    }
}
