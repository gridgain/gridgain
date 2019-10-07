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

package org.gridgain.dto;

import java.util.UUID;

/**
 * DTO for cluster info.
 */
public class ClusterInfo {
    /** Cluster id. */
    private UUID id;

    /** Cluster tag. */
    private String tag;

    /**
     * Default constructor.
     */
    public ClusterInfo() {
        // No-op.
    }

    /**
     * @param id Cluster ID.
     * @param tag Cluster tag.
     */
    public ClusterInfo(UUID id, String tag) {
        this.id = id;
        this.tag = tag;
    }

    /**
     * @return Cluster id.
     */
    public UUID getId() {
        return id;
    }

    /**
     * @param id Id.
     */
    public void setId(UUID id) {
        this.id = id;
    }

    /**
     * @return Cluster tag.
     */
    public String getTag() {
        return tag;
    }

    /**
     * @param tag Cluster tag.
     */
    public void setTag(String tag) {
        this.tag = tag;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "ClusterInfo [id=" + id + ", " + "tag=" + tag + "]";
    }
}
