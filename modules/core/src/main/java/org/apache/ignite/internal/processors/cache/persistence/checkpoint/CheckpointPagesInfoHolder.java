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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;

/**
 * Snapshot of dirty pages which should be stored to disk.
 */
public class CheckpointPagesInfoHolder {
    /** Total pages count in cp. */
    private final int pagesNum;

    /** Collection of pages per PageMemory distribution. */
    private final Collection<Map.Entry<PageMemoryEx, GridMultiCollectionWrapper<FullPageId>>> cpPages;

    /**
     *
     */
    public CheckpointPagesInfoHolder(
        Collection<Map.Entry<PageMemoryEx, GridMultiCollectionWrapper<FullPageId>>> pages,
        int num
    ) {
        cpPages = pages;
        pagesNum = num;
    }

    /** Total pages count in cp. */
    int pagesNum() {
        return pagesNum;
    }

    /** Collection of pages per PageMemory distribution. */
    Collection<Map.Entry<PageMemoryEx, GridMultiCollectionWrapper<FullPageId>>> cpPages() {
        return cpPages;
    }
}
