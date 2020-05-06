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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

/**
 * Represent a reason by that a WAL history was bounded.
 */
public enum ReservationReason {
    /** The message puts down to log when an exception happened during
     * reading a WAL segment or a segment cannot be reserved. */
    WAL_RESERVATION_ERROR,

    /** Reason means no more history reserved for the cache. */
    NO_MORE_HISTORY,

    /** Node does not have owning partitions. */
    NO_PARTITIONS_OWNED,

    /** Reason means a checkpoint in history reserved can not be applied for cache. */
    CHECKPOINT_NOT_APPLICABLE,

    /** That means all history reserved for cache. */
    NO_MORE_DATA_EARLIER_IN_HISTORY;

    /** {@inheritDoc} */
    @Override public String toString() {
        switch (this) {
            case WAL_RESERVATION_ERROR:
                return "Unexpected error during processing of previous checkpoint";

            case NO_MORE_HISTORY:
                return "Reserved checkpoint is the oldest in history";

            case NO_PARTITIONS_OWNED:
                return "Node didn't own any partitions for this group at the time of checkpoint";

            case CHECKPOINT_NOT_APPLICABLE:
                return "Checkpoint was marked as inapplicable for historical rebalancing";

            case NO_MORE_DATA_EARLIER_IN_HISTORY:
                return "Full history were reserved";

            default:
                throw new IllegalArgumentException();
        }
    }
}
