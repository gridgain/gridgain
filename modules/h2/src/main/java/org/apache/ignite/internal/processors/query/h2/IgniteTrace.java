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
 * Tracing object for SQL queries.
 */
public class IgniteTrace {
    /** No-op tracing object. */
    public static final IgniteTrace NO_OP_TRACE = new NoOpIgniteTrace();

    /** Query descriptor. */
    private final String qryDesc;

    /** Flag whether offloading is started. */
    private boolean offloadStarted;

    /** The number of bytes consumed by the query. */
    private long memoryBytes;

    /** The number of bytes offloaded by the query. */
    private long offloadedBytes;

    /** The number of files created by the query. */
    private int filesCreated;

    /**
     * @param qryDesc Query descriptor,
     */
    public IgniteTrace(String qryDesc) {
        this.qryDesc = qryDesc;
    }

    /**
     * @return Flag whether offloading is started.
     */
    public boolean isOffloadStarted() {
        return offloadStarted;
    }

    /**
     * @param offloadStarted Flag whether offloading is started.
     */
    public void offloadStarted(boolean offloadStarted) {
        this.offloadStarted = offloadStarted;
    }

    /**
     * @return The number of bytes consumed by the query.
     */
    public long memoryBytes() {
        return memoryBytes;
    }

    /**
     * @param memoryBytes The number of bytes consumed by the query.
     */
    public void addMemoryBytes(long memoryBytes) {
        this.memoryBytes += memoryBytes;
    }

    /**
     * @return The number of bytes offloaded by the query.
     */
    public long offloadedBytes() {
        return offloadedBytes;
    }

    /**
     * @param offloadedBytes The number of bytes offloaded by the query.
     */
    public void addOffloadedBytes(long offloadedBytes) {
        this.offloadedBytes += offloadedBytes;
    }

    /**
     * @return The number of files created by the query.
     */
    public int filesCreated() {
        return filesCreated;
    }

    /**
     * @param filesCreated The number of files created by the query.
     */
    public void addFilesCreated(int filesCreated) {
        this.filesCreated += filesCreated;
    }

    /**
     * @return Query descriptor.
     */
    public String queryDescriptor() {
        return qryDesc;
    }

    /** */
    private static class NoOpIgniteTrace extends IgniteTrace {
        /** */
        private NoOpIgniteTrace() {
            super("");
        }

        /** {@inheritDoc} */
        @Override public boolean isOffloadStarted() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void offloadStarted(boolean offloadStarted) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public long memoryBytes() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void addMemoryBytes(long memoryBytes) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public long offloadedBytes() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void addOffloadedBytes(long offloadedBytes) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int filesCreated() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void addFilesCreated(int filesCreated) {
            // No-op.
        }
    }
}
