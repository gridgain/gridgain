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

package org.apache.ignite.internal.processors.rest.handlers.drain;

import java.util.Collections;
import java.util.List;

/**
 * JSON response payload for {@code cmd=supply-status}. Reports whether this
 * node is currently supplying partition data to peers and the affected cache
 * groups. Always paired with {@code STATUS_SUCCESS}; informational only — the
 * supply state is never used as a readiness gate. Recognized by
 * {@code GridJettyRestHandler} to emit the {@code X-Supplying} response header
 * for shell-parseable preStop hooks.
 */
public class SupplyStatusResponse {
    /** Aggregate flag — {@code true} if any non-system, non-local cache group is currently supplying. */
    private boolean supplying;

    /** Names of the cache groups currently supplying. */
    private List<String> supplyingCacheGroups = Collections.emptyList();

    /**
     * Default constructor for Jackson.
     */
    public SupplyStatusResponse() {
        // No-op.
    }

    /**
     * @param supplying Aggregate supplying flag.
     * @param supplyingCacheGroups Names of currently-supplying cache groups.
     */
    public SupplyStatusResponse(boolean supplying, List<String> supplyingCacheGroups) {
        this.supplying = supplying;
        this.supplyingCacheGroups = supplyingCacheGroups != null ? supplyingCacheGroups : Collections.<String>emptyList();
    }

    /**
     * @return Aggregate supplying flag.
     */
    public boolean isSupplying() {
        return supplying;
    }

    /**
     * @param supplying Aggregate supplying flag.
     */
    public void setSupplying(boolean supplying) {
        this.supplying = supplying;
    }

    /**
     * @return Names of cache groups currently supplying.
     */
    public List<String> getSupplyingCacheGroups() {
        return supplyingCacheGroups;
    }

    /**
     * @param supplyingCacheGroups Names of cache groups currently supplying.
     */
    public void setSupplyingCacheGroups(List<String> supplyingCacheGroups) {
        this.supplyingCacheGroups = supplyingCacheGroups != null ? supplyingCacheGroups : Collections.<String>emptyList();
    }
}
