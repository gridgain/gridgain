/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

/**
 * Enum representing reasons for performing a full rebalance in a distributed cache.
 */
public enum FullRebalanceReason {
    NO_HISTORY_PARTITIONS("Partitions weren't present in any history reservation"),
    INSUFFICIENT_PARTITION_SIZE("Partition size less than required for historical rebalance"),
    COUNTER_EXCEEDS_DEMAND("Minimal available counter is greater than demanded one"),
    EXCESSIVE_PARTITION_UPDATES("Partition has too many updates");

    /** Message describing the rebalance reason. */
    private final String msg;

    /**
     * Constructor for the enum.
     *
     * @param msg Description of the rebalance reason.
     */
    FullRebalanceReason(String msg) {
        this.msg = msg;
    }

    @Override public String toString() {
        return msg;
    }
}
