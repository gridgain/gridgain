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

package org.apache.ignite.internal.processors.query.h2;

/**
 * Memory tracker.
 */
public interface H2MemoryTracker extends AutoCloseable {
    /**
     * Check allocated size is less than query memory pool threshold.
     *
     * @param size Allocated size in bytes.
     * @return {@code True} if memory limit is not exceeded. {@code False} otherwise.
     */
    public boolean reserved(long size);

    /**
     * Memory release callback.
     *
     * @param size Released memory size in bytes.
     */
    public void released(long size);

    /**
     * Reserved memory.
     *
     * @return  Reserved memory in bytes.
     */
    public long memoryReserved();

    /**
     * @return Max memory limit.
     */
    public long memoryLimit();
}
