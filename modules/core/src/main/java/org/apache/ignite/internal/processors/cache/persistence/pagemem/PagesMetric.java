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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;

public interface PagesMetric {
    /**
     * Gets pages count used by Data pages.
     *
     * @param grpId Group Id.
     * @return Pages count used by Data pages.
     */
    long physicalMemoryDataPagesSize(int grpId);

    /**
     * Gets pages count used by Index pages.
     *
     * @return Pages count used by Index pages.
     */
    long physicalMemoryIndexPagesSize();

    /**
     * Gets pages count located in {@link ReuseList}.
     *
     * @return Pages located in {@link ReuseList}.
     */
    long physicalMemoryFreelistPagesSize();

    /**
     * Gets pages count used by metastore and tracking pages.
     *
     * @return Pages count used by metastore and tracking pages.
     */
    long physicalMemoryMetaPagesSize();

    /**
     * Gets pages count which are allocated but never used.
     *
     * @return Pages count which are allocated but never used.
     */
    long physicalMemoryFreePagesSize();

    /**
     * Page is used first time.
     */
    void freePageUsed();

    /**
     * Page is in use.
     *
     * @param grpId Group id.
     * @param partId Partition id.
     * @param flags Flags.
     */
    void pageAllocated(int grpId, int partId, byte flags);

    /**
     * Page is reused from reuse list.
     *
     * @param grpId Group id.
     * @param partId Partition id.
     * @param flags Flags.
     */
    void pageFromReuseList(int grpId, int partId, byte flags);

    /**
     * Increase free pages count.
     *
     * @param count increased count.
     */
    void freePagesIncreased(int count);

    /**
     * Pages are moved to reuse list.
     *
     * @param count Pages count.
     * @param grpId Group id.
     * @param partId Partition id.
     * @param flags Flags.
     */
    void reusePageIncreased(int count, int grpId, int partId, byte flags);
}
