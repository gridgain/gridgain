/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

import org.apache.ignite.internal.pagemem.FullPageId;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;

/** Helper class for checkpoint testing that may contain useful methods and constants. */
class TestCheckpointUtils {
    /**
     * Creates new full page ID.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     */
    static FullPageId fullPageId(int grpId, int partId) {
        return new FullPageId(pageId(partId, FLAG_DATA, 0), grpId);
    }
}
