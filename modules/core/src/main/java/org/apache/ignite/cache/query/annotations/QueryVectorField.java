/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.cache.query.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;

/**
 * Annotation for fields to be indexed for vector
 * search using Lucene. For more information
 * refer to {@link CacheQuery} documentation.
 * @see CacheQuery
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE})
public @interface QueryVectorField {
    /**
     * Returns similarityFunction for a vector field.
     * @return similarityFunction for a vector field.
     */
    SimilarityFunction similarityFunction() default SimilarityFunction.COSINE;

    public static enum SimilarityFunction {
        COSINE(1),
        DOT_PRODUCT(2),
        EUCLIDEAN(3),
        MAXIMUM_INNER_PRODUCT(4);

        /** Similarity Function id. */
        private final int similarityFunctionId;

        /**
         * @param id Similarity Function ID.
         */
        SimilarityFunction(int id) {
            similarityFunctionId = id;
        }

        /**
         * Returns the integer ID of this similarity function.
         * @return The similarity function ID.
         */
        public int getSimilarityFunctionId() {
            return similarityFunctionId;
        }

        /**
         * Converts an integer ID to the corresponding SimilarityFunction.
         * @param id The similarity function ID.
         * @return The SimilarityFunction corresponding to the ID.
         * @throws IllegalArgumentException if the ID doesn't match any similarity function.
         */
        public static SimilarityFunction fromId(int id) {
            for (SimilarityFunction func : values()) {
                if (func.getSimilarityFunctionId() == id) {
                    return func;
                }
            }
            return COSINE;
        }
    }
}