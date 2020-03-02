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
 * Interface to retrieve information about memory and disk utilization.
 */
public interface GridQueryMemoryMetricProvider {
    /**
     * Number of bytes reserved at the current moment.
     *
     * @return Number of bytes reserved.
     */
    public long reserved();

    /**
     * Maxim number of bytes reserved at the same time.
     *
     * @return Maximum number of bytes reserved.
     */
    public long maxReserved();

    /**
     * Number of bytes written on disk at the current moment.
     *
     * @return Number of bytes written on disk.
     */
    public long writtenOnDisk();

    /**
     * Maximum number of bytes written on disk at the same time.
     *
     * @return Maximum number of bytes.
     */
    public long maxWrittenOnDisk();

    /**
     * Total number of bytes written on disk.
     *
     * @return Total number of bytes written on disk.
     */
    public long totalWrittenOnDisk();
}
