package org.apache.ignite.cache;

/**
 * Defines Similarity Function for Vector Search.
 */
public enum SimilarityFunction {
    COSINE(1),
    DOT_PRODUCT(2),
    EUCLIDEAN(3),
    MAXIMUM_INNER_PRODUCT(4);

    /**
     * Similarity Function id.
     */
    private final int similarityFunctionId;

    /**
     * @param id Similarity Function ID.
     */
    SimilarityFunction(int id) {
        similarityFunctionId = id;
    }

    /**
     * Returns the integer ID of this similarity function.
     *
     * @return The similarity function ID.
     */
    public int getSimilarityFunctionId() {
        return similarityFunctionId;
    }

    /**
     * Converts an integer ID to the corresponding SimilarityFunction.
     *
     * @param id The similarity function ID.
     * @return The SimilarityFunction corresponding to the ID, or COSINE if no match is found
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
