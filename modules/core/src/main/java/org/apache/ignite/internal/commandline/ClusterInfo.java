/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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
        if (getId() != null)
            return getId().toString();

        return "-ID-";
    }

    /**
     * Get cluster tag
     * @return cluster tag
     */
    public String getTag() {
        return tag;
    }
}
