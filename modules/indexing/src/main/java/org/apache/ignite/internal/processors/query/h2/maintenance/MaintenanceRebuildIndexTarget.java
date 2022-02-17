/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.query.h2.maintenance;

import java.util.Objects;

/**
 * {@link RebuildIndexAction}'s parameters.
 */
public class MaintenanceRebuildIndexTarget {
    /** Cache id. */
    private final int cacheId;

    /** Name of the index. */
    private final String idxName;

    /**
     * Constructor.
     *
     * @param cacheId Cache id.
     * @param idxName Name of the index.
     */
    public MaintenanceRebuildIndexTarget(int cacheId, String idxName) {
        this.cacheId = cacheId;
        this.idxName = idxName;
    }

    /**
     * @return Cache id.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @return Name of the index.
     */
    public String idxName() {
        return idxName;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        MaintenanceRebuildIndexTarget other = (MaintenanceRebuildIndexTarget)o;

        return cacheId == other.cacheId && idxName.equals(other.idxName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(cacheId, idxName);
    }
}
