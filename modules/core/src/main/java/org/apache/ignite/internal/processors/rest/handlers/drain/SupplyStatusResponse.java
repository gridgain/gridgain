/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.rest.handlers.drain;

import java.util.Collections;
import java.util.List;

/**
 * JSON response for {@code cmd=supply-status}: whether this node is currently supplying partition
 * data (and which groups), the active thin-client count, and whether it is the sole owner of any
 * partitions. {@code uniqueDataHeld}/{@code uniqueDataCacheGroups} are reported under both shutdown
 * policies but only gate the {@code shutdown=true} path under {@code GRACEFUL}.
 */
public class SupplyStatusResponse {
    /** Aggregate flag — {@code true} if any non-system, non-local cache group is currently supplying. */
    private final boolean supplying;

    /** Names of the cache groups currently supplying. */
    private final List<String> supplyingCacheGroups;

    /** Active thin-client connection count at the moment this response was built. */
    private final int activeThinClients;

    /** {@code true} if this node is the only owner of any non-system cache group's partitions. */
    private final boolean uniqueDataHeld;

    /** Names of the cache groups this node is the sole owner of. */
    private final List<String> uniqueDataCacheGroups;

    /**
     * @param supplying Aggregate supplying flag.
     * @param supplyingCacheGroups Names of currently-supplying cache groups.
     * @param activeThinClients Active thin-client connection count.
     * @param uniqueDataHeld Unique-data flag.
     * @param uniqueDataCacheGroups Sole-owner cache group names.
     */
    public SupplyStatusResponse(
        boolean supplying,
        List<String> supplyingCacheGroups,
        int activeThinClients,
        boolean uniqueDataHeld,
        List<String> uniqueDataCacheGroups
    ) {
        this.supplying = supplying;
        this.supplyingCacheGroups = supplyingCacheGroups != null ? supplyingCacheGroups : Collections.emptyList();
        this.activeThinClients = activeThinClients;
        this.uniqueDataHeld = uniqueDataHeld;
        this.uniqueDataCacheGroups = uniqueDataCacheGroups != null ? uniqueDataCacheGroups : Collections.emptyList();
    }

    /**
     * @return Aggregate supplying flag.
     */
    public boolean isSupplying() {
        return supplying;
    }

    /**
     * @return Names of cache groups currently supplying.
     */
    public List<String> getSupplyingCacheGroups() {
        return supplyingCacheGroups;
    }

    /**
     * @return Active thin-client connection count.
     */
    public int getActiveThinClients() {
        return activeThinClients;
    }

    /**
     * @return Unique-data flag.
     */
    public boolean isUniqueDataHeld() {
        return uniqueDataHeld;
    }

    /**
     * @return Sole-owner cache group names.
     */
    public List<String> getUniqueDataCacheGroups() {
        return uniqueDataCacheGroups;
    }
}
