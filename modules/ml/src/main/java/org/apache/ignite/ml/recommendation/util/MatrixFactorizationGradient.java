/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.recommendation.util;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Gradient of matrix factorization loss function.
 *
 * @param <O> Type of an object of recommendation.
 * @param <S> Type of a subject of recommendation.
 */
public class MatrixFactorizationGradient<O extends Serializable, S extends Serializable> implements Serializable {
    /** */
    private static final long serialVersionUID = -213199977280252181L;

    /** Gradient of object of recommendation matrix. */
    private final Map<O, Vector> objGrad;

    /** Gradient of subject of recommendation function. */
    private final Map<S, Vector> subjGrad;

    /**
     * Constructs a new instance of matrix factorization gradient.
     *
     * @param objGrad Gradient of object of recommendation matrix.
     * @param subjGrad Gradient of subject of recommendation matrix.
     */
    public MatrixFactorizationGradient(Map<O, Vector> objGrad, Map<S, Vector> subjGrad) {
        this.objGrad = Collections.unmodifiableMap(objGrad);
        this.subjGrad = Collections.unmodifiableMap(subjGrad);
    }

    /**
     * Returns gradient of object of recommendation matrix (unmodifiable).
     *
     * @return Gradient of object of recommendation matrix.
     */
    public Map<O, Vector> getObjGrad() {
        return objGrad;
    }

    /**
     * Returns gradient of subject of recommendation function (unmodifiable).
     *
     * @return Gradient of subject of recommendation function.
     */
    public Map<S, Vector> getSubjGrad() {
        return subjGrad;
    }
}
