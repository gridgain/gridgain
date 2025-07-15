/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.cache;

/**
 * Defines Similarity Function for Vector Search.
 */
public enum SimilarityFunction {
    COSINE,
    DOT_PRODUCT,
    EUCLIDEAN,
    MAXIMUM_INNER_PRODUCT;

    /** Enum values. */
    private static final SimilarityFunction[] VALS = values();

    /**
     * Efficiently gets SimilarityFunction from its ordinal.
     *
     * @param ord Ordinal value.
     * @return The SimilarityFunction corresponding to the ordinal, or COSINE if no match is found
     */
    public static SimilarityFunction fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : COSINE;
    }
}
