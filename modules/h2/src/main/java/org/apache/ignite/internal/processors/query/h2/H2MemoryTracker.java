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
     * Reserved memory.
     *
     * @return Reserved memory in bytes.
     */
    public long reserved();

    /**
     * Tracks swapping on disk.
     *
     * @param size Amount of bytes written on disk.
     */
    public void swap(long size);

    /**
     * Tracks unswapping from disk.
     *
     * @param size Amount of bytes deleted from disk.
     */
    public void unswap(long size);

    /**
     * Increments the counter of created offloading files.
     */
    public void incrementFilesCreated();

    public H2MemoryTracker createChildTracker();

    /** {@inheritDoc} */
    @Override public void close();

    /** */
    public H2MemoryTracker NO_OP_TRACKER = new H2MemoryTracker() {
        /** {@inheritDoc} */
        @Override public boolean reserve(long size) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void release(long size) {
        }

        /** {@inheritDoc} */
        @Override public long reserved() {
            return -1;
        }

        /** {@inheritDoc} */
        @Override public void swap(long size) {
        }

        /** {@inheritDoc} */
        @Override public void unswap(long size) {
        }

        /** {@inheritDoc} */
        @Override public void close() {
        }

        /** {@inheritDoc} */
        @Override public void incrementFilesCreated() {
        }

        @Override public H2MemoryTracker createChildTracker() {
            return null;
        }
    };
}
