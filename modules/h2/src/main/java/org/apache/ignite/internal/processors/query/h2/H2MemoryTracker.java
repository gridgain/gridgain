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
     * Tracks reservation of new chunk of bytes.
     *
     * @param size Allocated size in bytes.
     * @return {@code true} if memory limit is not exceeded. {@code false} otherwise.
     */
    public boolean reserve(long size);

    /**
     * Tracks memory releasing.
     *
     * @param size Released memory size in bytes.
     */
    public void release(long size);

    /**
     * Written on disk memory.
     *
     * @return Amount of bytes written on disk.
     */
    public long writtenOnDisk();

    /**
     * Written on disk total.
     *
     * @return Amount of bytes written on disk in total.
     */
    public long totalWrittenOnDisk();

    /**
     * Reserved memory.
     *
     * @return Reserved memory in bytes.
     */
    public long reserved();

    /**
     * Tracks spilling on disk.
     *
     * @param size Amount of bytes written on disk.
     */
    public void spill(long size);

    /**
     * Tracks unspilling from disk.
     *
     * @param size Amount of bytes deleted from disk.
     */
    public void unspill(long size);

    /**
     * Increments the counter of created offloading files.
     */
    public void incrementFilesCreated();

    /**
     * Creates child tracker that uses resources of current tracker.
     *
     * Note: created tracker is not thread-safe
     */
    public H2MemoryTracker createChildTracker();

    /**
     * Callback to release resources allocated for child tracker.
     *
     * @param child Child whose resources should be released.
     */
    public void onChildClosed(H2MemoryTracker child);

    /**
     * Whether current tracker was closed or not.
     *
     * @return {@code true} if current tracker was closed.
     */
    public boolean closed();

    /** {@inheritDoc} */
    @Override public void close();
}
