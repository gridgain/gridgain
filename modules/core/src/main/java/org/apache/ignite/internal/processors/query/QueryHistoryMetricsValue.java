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

package org.apache.ignite.internal.processors.query;

/**
 * Immutable query metrics.
 */
class QueryHistoryMetricsValue {
    /** Number of executions. */
    private final long execs;

    /** Number of failures. */
    private final long failures;

    /** Minimum time of execution. */
    private final long minTime;

    /** Maximum time of execution. */
    private final long maxTime;

    /** Last start time of execution. */
    private final long lastStartTime;

    /** Minimum memory required by query. */
    private final long minMemory;

    /** Maximum memory required by query. */
    private final long maxMemory;

    /** Minimum bytes on disk required by query. */
    private final long minBytesAllocatedOnDisk;

    /** Maximum bytes on disk required by query. */
    private final long maxBytesAllocatedOnDisk;

    /** Minimum bytes written on disk in total by query. */
    private final long minTotalBytesWrittenOnDisk;

    /** Maximum bytes written on disk in total by query. */
    private final long maxTotalBytesWrittenOnDisk;

    /**
     * @param execs Number of executions.
     * @param failures Number of failure.
     * @param minTime Min time of execution.
     * @param maxTime Max time of execution.
     * @param lastStartTime Last start time of execution.
     * @param minMemory Minimum memory required by query.
     * @param maxMemory Maximum memory required by query.
     * @param minBytesAllocatedOnDisk Minimum bytes on disk required by query.
     * @param maxBytesAllocatedOnDisk Maximum bytes on disk required by query.
     * @param minTotalBytesWrittenOnDisk Minimum bytes written on disk in total by query.
     * @param maxTotalBytesWrittenOnDisk Maximum bytes written on disk in total by query.
     */
    public QueryHistoryMetricsValue(long execs, long failures, long minTime, long maxTime, long lastStartTime,
        long minMemory, long maxMemory, long minBytesAllocatedOnDisk, long maxBytesAllocatedOnDisk,
        long minTotalBytesWrittenOnDisk, long maxTotalBytesWrittenOnDisk
        ) {
        this.execs = execs;
        this.failures = failures;
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.lastStartTime = lastStartTime;
        this.minMemory = minMemory;
        this.maxMemory = maxMemory;
        this.minBytesAllocatedOnDisk = minBytesAllocatedOnDisk;
        this.maxBytesAllocatedOnDisk = maxBytesAllocatedOnDisk;
        this.minTotalBytesWrittenOnDisk = minTotalBytesWrittenOnDisk;
        this.maxTotalBytesWrittenOnDisk = maxTotalBytesWrittenOnDisk;
    }

   /**
     * Gets total number execution of query.
     *
     * @return Number of executions.
     */
    public long execs() {
        return execs;
    }

    /**
     * Gets number of times a query execution failed.
     *
     * @return Number of times a query execution failed.
     */
    public long failures() {
        return failures;
    }

    /**
     * Gets minimum execution time of query.
     *
     * @return Minimum execution time of query.
     */
    public long minTime() {
        return minTime;
    }

    /**
     * Gets maximum execution time of query.
     *
     * @return Maximum execution time of query.
     */
    public long maxTime() {
        return maxTime;
    }

    /**
     * Gets latest query start time.
     *
     * @return Latest time query was stared.
     */
    public long lastStartTime() {
        return lastStartTime;
    }

    /**
     * Gets minimum memory required by query.
     *
     * @return Minimum memory required by query.
     */
    public long minMemory() {
        return minMemory;
    }

    /**
     * Gets maximum memory required by query.
     *
     * @return Maximum memory required by query.
     */
    public long maxMemory() {
        return maxMemory;
    }

    /**
     * Gets minimum bytes on disk required by query.
     *
     * @return Minimum bytes on disk required by query.
     */
    public long minBytesAllocatedOnDisk() {
        return minBytesAllocatedOnDisk;
    }

    /**
     * Gets maximum bytes on disk required by query.
     *
     * @return Maximum bytes on disk required by query.
     */
    public long maxBytesAllocatedOnDisk() {
        return maxBytesAllocatedOnDisk;
    }

    /**
     * Gets minimum bytes written on disk in total by query.
     *
     * @return Minimum bytes written on disk in total by query.
     */
    public long minTotalBytesWrittenOnDisk() {
        return minTotalBytesWrittenOnDisk;
    }

    /**
     * Gets maximum bytes written on disk in total by query.
     *
     * @return Maximum bytes written on disk in total by query.
     */
    public long maxTotalBytesWrittenOnDisk() {
        return maxTotalBytesWrittenOnDisk;
    }

}
