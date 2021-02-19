/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.IgniteCheckedException;

/**
 * Abstract page replacement policy.
 */
public abstract class PageReplacementPolicy {
    /** Page memory segment. */
    protected final PageMemoryImpl.Segment seg;

    /**
     * @param seg Page memory segment.
     */
    protected PageReplacementPolicy(PageMemoryImpl.Segment seg) {
        this.seg = seg;
    }

    /**
     * Existing page touched.
     *
     * Note: This method can be invoked under segment write lock or segment read lock.
     */
    public void onHit(long relPtr) {
        // No-op.
    }

    /**
     * New page added.
     *
     * Note: This method always invoked under segment write lock.
     */
    public void onMiss(long relPtr) {
        // No-op.
    }

    /**
     * Page removed from the page memory.
     */
    public void onRemove(long relPtr) {
        // No-op.
    }

    /**
     * Finds page to replace.
     *
     * Note: This method always invoked under segment write lock.
     *
     * @return Relative pointer to page.
     */
    public abstract long replace() throws IgniteCheckedException;
}
