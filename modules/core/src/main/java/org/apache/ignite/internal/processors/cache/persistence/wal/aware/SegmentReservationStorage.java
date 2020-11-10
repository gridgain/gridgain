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
package org.apache.ignite.internal.processors.cache.persistence.wal.aware;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Segment reservations storage: Protects WAL segments from deletion during WAL log cleanup.
 */
class SegmentReservationStorage extends SegmentObservable {
    /**
     * Maps absolute segment index to reservation counter. If counter > 0 then we wouldn't delete all segments which has
     * index >= reserved segment index. Guarded by {@code this}.
     */
    private final NavigableMap<Long, Integer> reserved = new TreeMap<>();

    /** Minimum reserved segment, {@code -1} if none. */
    private volatile long minReservedIdx = -1;

    /**
     * @param absIdx Index for reservation.
     */
    synchronized void reserve(long absIdx) {
        trackingMinResevedSegment(() -> reserved.merge(absIdx, 1, Integer::sum));
    }

    /**
     * Checks if segment is currently reserved (protected from deletion during WAL cleanup).
     *
     * @param absIdx Index for check reservation.
     * @return {@code True} if index is reserved.
     */
    boolean reserved(long absIdx) {
        long minIdx = minReservedIdx;

        return minIdx != -1 && absIdx >= minIdx;
    }

    /**
     * @param absIdx Reserved index.
     */
    synchronized void release(long absIdx) {
        Integer cur = reserved.get(absIdx);

        assert cur != null && cur >= 1 : "cur=" + cur + ", absIdx=" + absIdx;

        if (cur == 1)
            trackingMinResevedSegment(() -> reserved.remove(absIdx));
        else
            reserved.put(absIdx, cur - 1);
    }

    /**
     * Tracking changes to minimum reserved segment.
     * Notifies observers when a change occurs.
     *
     * @param r Function {@link #reserved} changes.
     */
    private synchronized void trackingMinResevedSegment(Runnable r) {
        Map.Entry<Long, Integer> oldMin = reserved.firstEntry();

        r.run();

        Map.Entry<Long, Integer> newMin = reserved.firstEntry();

        Long oldMinIdx = oldMin == null ? null : oldMin.getKey();
        Long newMinIdx = newMin == null ? null : newMin.getKey();

        if (!Objects.equals(oldMinIdx, newMinIdx)) {
            minReservedIdx = newMinIdx == null ? -1 : newMinIdx;

            notifyObservers(newMinIdx);
        }
    }
}
