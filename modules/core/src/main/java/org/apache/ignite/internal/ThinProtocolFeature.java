package org.apache.ignite.internal;

/**
 * The base feature class.
 */
public interface ThinProtocolFeature {
    /**
     * @return Feature ID.
     */
    int featureId();

    /**
     * @return Feature's name.
     */
    String name();
}
