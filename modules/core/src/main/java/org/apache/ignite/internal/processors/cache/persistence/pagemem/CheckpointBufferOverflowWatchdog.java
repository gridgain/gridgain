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

import static org.apache.ignite.internal.processors.cache.persistence.pagemem.PagesWriteThrottlePolicy.CP_BUF_FILL_THRESHOLD;

/**
 * Logic used to determine whether Checkpoint Buffer is in danger zone and writer threads should be throttled.
 */
class CheckpointBufferOverflowWatchdog {
    /** Page memory. */
    private final PageMemoryImpl pageMemory;

    /**
     * Creates a new instance.
     *
     * @param pageMemory page memory to use
     */
    CheckpointBufferOverflowWatchdog(PageMemoryImpl pageMemory) {
        this.pageMemory = pageMemory;
    }

    /**
     * Returns true if Checkpoint Buffer is in danger zone (more than
     * {@link PagesWriteThrottlePolicy#CP_BUF_FILL_THRESHOLD} of the buffer is filled) and, hence, writer threads need
     * to be throttled.
     *
     * @return {@code true} iff Checkpoint Buffer is in danger zone
     */
    boolean isInDangerZone() {
        int checkpointBufLimit = (int)(pageMemory.checkpointBufferPagesSize() * CP_BUF_FILL_THRESHOLD);

        return pageMemory.checkpointBufferPagesCount() > checkpointBufLimit;
    }
}
