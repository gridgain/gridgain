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
package org.apache.ignite.internal.processors.query.oom;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryMemoryMetricProvider;
import org.apache.ignite.internal.processors.query.h2.QueryMemoryManager;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Test cases to ensure that offloaded data properly tracked by memory tracker.
 */
public class DiskSpillingMemoryTrackerTest extends DiskSpillingAbstractTest {
    /** */
    @Test
    public void testOffloadedDataTrackedByMemoryTracker() {
        IgniteEx ignite = grid(0);

        final List<GridQueryMemoryMetricProvider> trackers = new ArrayList<>();

        GridTestUtils.setFieldValue(
            ignite.context().query().getIndexing(),
            "memoryMgr",
            new QueryMemoryManager(ignite.context()) {
                @Override public GridQueryMemoryMetricProvider createQueryMemoryTracker(long maxQryMemory) {
                    GridQueryMemoryMetricProvider tracker = super.createQueryMemoryTracker(maxQryMemory);

                    trackers.add(tracker);

                    return tracker;
                }
            }
        );

        String sql = "SELECT * FROM person";

        runSql(sql, false, HUGE_MEM_LIMIT);

        assertFalse(trackers.isEmpty());

        // offloading should not happen
        for (GridQueryMemoryMetricProvider tr : trackers) {
            assertEquals(0, tr.maxWrittenOnDisk());
            assertEquals(0, tr.totalWrittenOnDisk());
        }

        int ind = 0;

        for (int i = 1; i < trackers.size(); i++) {
            if (trackers.get(i).maxReserved() > trackers.get(ind).maxReserved())
                ind = i;
        }

        long memLimit = trackers.get(ind).maxReserved() / 2;

        trackers.clear();

        runSql(sql, false, memLimit);

        trackers.removeIf(tr -> tr.maxWrittenOnDisk() == 0);

        // at least one query should be offloaded
        assertFalse(trackers.isEmpty());

        for (GridQueryMemoryMetricProvider tr : trackers)
            assertTrue(tr.maxWrittenOnDisk() <= tr.totalWrittenOnDisk());
    }
}
