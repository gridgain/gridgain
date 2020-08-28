package org.apache.ignite.internal.commandline;

import java.util.UUID;

/**
 * Cluster info wrapper
 */
public class ClusterInfo {
    /** Claster UUID. */
    private final UUID id;
    /** Cluster tag. */
    private final String tag;

    /**
     * Create cluster info wrapper
     * @param id - cluster UUID
     * @param tag - cluster tag
     */
    public ClusterInfo(UUID id, String tag) {
        this.id = id;
        this.tag = tag;
    }

    /**
     * Get cluster UUID
     * @return cluster UUID
     */
    public UUID getId() {
        return id;
    }

    /**
     * Get UUID as string
     * @return formatted UUID
     */
    public String getIdAsString() {
        return getId().toString();
    }

    /**
     * Get cluster tag
     * @return cluster tag
     */
    public String getTag() {
        return tag;
    }
}
