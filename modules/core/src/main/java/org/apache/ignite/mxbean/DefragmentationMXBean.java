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

package org.apache.ignite.mxbean;

/**
 * JMX bean for defragmentation manager.
 */
@MXBeanDescription("MBean that provides access for defragmentation features.")
public interface DefragmentationMXBean {
    /**
     * Schedule defragmentation for given caches.
     *
     * @param cacheNames Names of caches to run defragmentation on, comma separated.
     * @return {@code true} if defragmentation is scheduled, {@code false} otherwise.
     */
    @MXBeanDescription("Schedule defragmentation.")
    @MXBeanParametersNames("cacheNames")
    @MXBeanParametersDescriptions("Names of caches to run defragmentation on.")
    public boolean schedule(String cacheNames);

    /**
     * Cancel defragmentation.
     *
     * @return {@code true} if defragmentation was canceled, {@code false} otherwise.
     */
    @MXBeanDescription("Cancel current defragmentation.")
    public boolean cancel();

    /**
     * Get defragmentation status.
     *
     * @return {@code true} if defragmentation is in progress right now.
     */
    @MXBeanDescription("Cancel current defragmentation.")
    public boolean inProgress();

    /**
     * Get count of processed partitions.
     *
     * @return {@code true} if defragmentation is in progress right now.
     */
    @MXBeanDescription("Processed partitions.")
    public int processedPartitions();

    /**
     * Get total count of partitions.
     *
     * @return {@code true} if defragmentation is in progress right now.
     */
    @MXBeanDescription("Total partitions.")
    public int totalPartitions();

    /**
     * Get defragmentation's start time.
     *
     * @return {@code true} if defragmentation is in progress right now.
     */
    @MXBeanDescription("Start time.")
    public long startTime();
}
