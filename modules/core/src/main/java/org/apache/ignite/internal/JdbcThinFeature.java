package org.apache.ignite.internal;

/**
 * The base feature class.
 */
public interface JdbcThinFeature {
    /**
     * @return Feature ID.
     */
    int featureId();

    /**
     * @return Feature's name.
     */
    String name();

    boolean isFeatureSet(byte[] bytes);
}
