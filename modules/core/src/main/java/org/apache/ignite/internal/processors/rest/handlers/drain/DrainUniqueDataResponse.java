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

import java.util.List;

/**
 * JSON body of the {@code 503} from {@code cmd=drain&action=start} when this node is the sole
 * owner of a cache group's partitions under {@code GRACEFUL} shutdown: names the affected groups
 * so the operator can override with {@code force=true}.
 */
public class DrainUniqueDataResponse {
    /** Names of the cache groups this node is the sole owner of. */
    private final List<String> cacheGroups;

    /** @param cacheGroups Sole-owner cache group names. */
    public DrainUniqueDataResponse(List<String> cacheGroups) {
        this.cacheGroups = cacheGroups;
    }

    /** @return Always {@code true} — this body is only built when unique data is held. */
    public boolean isUniqueDataHeld() {
        return true;
    }

    /** @return Sole-owner cache group names. */
    public List<String> getCacheGroups() {
        return cacheGroups;
    }
}
