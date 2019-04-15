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

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.ml.dataset.feature.extractor.ExtractionUtils;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.SparseVector;

/**
 * Vectorizer on binary objects.
 *
 * @param <K> Type of key.
 */
public class BinaryObjectVectorizer<K> extends ExtractionUtils.StringCoordVectorizer<K, BinaryObject> {
    /** Serial version uid. */
    private static final long serialVersionUID = 2152161240934492838L;

    /** Object for denoting default value of feature mapping. */
    public static final String DEFAULT_VALUE = "DEFAULT";
    /** Mapping for feature with non-number values. */
    private HashMap<String, HashMap<Object, Double>> featureValueMappings = new HashMap<>();

    /**
     * Creates an instance of Vectorizer.
     *
     * @param coords Coordinates.
     */
    public BinaryObjectVectorizer(String... coords) {
        super(coords);
    }

    /**
     * Sets values mapping for feature.
     *
     * @param coord Feature coordinate.
     * @param valuesMapping Mapping.
     * @return this.
     */
    public BinaryObjectVectorizer withFeature(String coord, Mapping valuesMapping) {
        featureValueMappings.put(coord, valuesMapping.toMap());
        return this;
    }

    /** {@inheritDoc} */
    @Override protected Double feature(String coord, K key, BinaryObject value) {
        HashMap<Object, Double> mapping = featureValueMappings.get(coord);
        if (mapping != null)
            return mapping.get(coord);

        Number val = value.field(coord);
        return val != null ? val.doubleValue() : null;
    }

    /** {@inheritDoc} */
    @Override protected List<String> allCoords(K key, BinaryObject value) {
        return value.type().fieldNames().stream()
            .filter(fname -> fieldIsDouble(value, fname))
            .collect(Collectors.toList());
    }

    /**
     * @param value Value.
     * @param fname Fname.
     * @return true if field in binary object has double type.
     */
    private boolean fieldIsDouble(BinaryObject value, String fname) {
        return value.type().fieldTypeName(fname).equals(BinaryUtils.fieldTypeName(GridBinaryMarshaller.DOUBLE));
    }

    /** {@inheritDoc} */
    @Override protected Vector createVector(int size) {
        return new SparseVector(size);
    }

    /** Feature values mapping for non-number features. */
    public static class Mapping {
        /** Mapping. */
        private HashMap<Object, Double> value = new HashMap<>();

        /**
         * Creates an instance of Mapping.
         */
        public static Mapping create() {
            return new Mapping();
        }

        /**
         * Add mapping.
         *
         * @param from From value.
         * @param to To double value.
         * @return this.
         */
        public Mapping map(Object from, Double to) {
            this.value.put(from, to);
            return this;
        }

        /**
         * Default value for new feature values.
         *
         * @param value Default value.
         * @return this.
         */
        public Mapping defaultValue(Double value) {
            this.value.put(DEFAULT_VALUE, value);
            return this;
        }

        /**
         * Converts mapping to HashMap.
         */
        private HashMap<Object, Double> toMap() {
            if(!value.containsKey(DEFAULT_VALUE))
                value.put(DEFAULT_VALUE, null);

            return value;
        }
    }
}
