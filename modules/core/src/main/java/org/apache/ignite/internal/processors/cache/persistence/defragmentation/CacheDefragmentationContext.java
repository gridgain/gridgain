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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** */
public class CacheDefragmentationContext {
    /** */

    /** */
    private final Set<Integer> cacheGroupsForDefragmentation;

    /**
     * @param cacheGroupsForDefragmentation Cache group ids for defragmentation.
     */
    public CacheDefragmentationContext(
        List<Integer> cacheGroupsForDefragmentation
    ) {

        this.cacheGroupsForDefragmentation = new HashSet<>(cacheGroupsForDefragmentation);
    }

    /** */
    public Set<Integer> cacheGroupsForDefragmentation() {
        return cacheGroupsForDefragmentation;
    }

    /** */
    public void onCacheGroupDefragmented(int grpId) {
        // Invalidate page stores.
    }
}
