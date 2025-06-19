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
