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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.h2.QueryMemoryManager;
import org.apache.ignite.internal.processors.query.h2.QueryMemoryTracker;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SQL_QUERY_GLOBAL_MEMORY_QUOTA;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SQL_QUERY_MEMORY_QUOTA;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SQL_QUERY_OFFLOADING_ENABLED;
import static org.apache.ignite.internal.processors.query.h2.QueryMemoryManager.DFLT_MEMORY_RESERVATION_BLOCK_SIZE;

/**
 * Unit tests for memory manager and memory tracker
 */
public class QueryMemoryManagerConfigurationSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        System.clearProperty(IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testQuotaDefaults() throws IgniteCheckedException {
        QueryMemoryManager memMgr = new QueryMemoryManager(newContext());

        // Check defaults for manager.
        assertManagerState(memMgr,
            DFLT_SQL_QUERY_GLOBAL_MEMORY_QUOTA,
            DFLT_SQL_QUERY_MEMORY_QUOTA,
            DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            DFLT_MEMORY_RESERVATION_BLOCK_SIZE);

        QueryMemoryTracker tracker = memMgr.createQueryMemoryTracker(0);

        // Check defaults for tracker
        assertTrackerState(tracker,
            DFLT_SQL_QUERY_GLOBAL_MEMORY_QUOTA,
            DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            DFLT_MEMORY_RESERVATION_BLOCK_SIZE);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testQuotaNonDefaults() throws IgniteCheckedException {
        IgniteConfiguration cfg = new IgniteConfiguration()
            .setSqlGlobalMemoryQuota(20_000)
            .setSqlQueryMemoryQuota(10_000)
            .setSqlOffloadingEnabled(!DFLT_SQL_QUERY_OFFLOADING_ENABLED);

        System.setProperty(IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE, "5000");

        QueryMemoryManager memMgr = new QueryMemoryManager(newContext(cfg));

        // Check defaults for manager.
        assertManagerState(memMgr,
            20_000,
            10_000,
            !DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            5_000);

        QueryMemoryTracker tracker = memMgr.createQueryMemoryTracker(0);

        assertTrackerState(tracker,
            10_000,
            !DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            5_000);

        tracker = memMgr.createQueryMemoryTracker(1_000);

        assertTrackerState(tracker,
            1_000,
            !DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            1_000);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testIllegalSettings() throws IgniteCheckedException {
        // Negative global quota.
        GridTestUtils.assertThrows(log, () -> {
                IgniteConfiguration cfg = new IgniteConfiguration()
                    .setSqlGlobalMemoryQuota(-1);

                QueryMemoryManager memoryMgr = new QueryMemoryManager(newContext(cfg));
            }, IllegalArgumentException.class,
            "Ouch! Argument is invalid: Sql global memory quota must be >= 0. But was -1");

        // Negative query quota.
        GridTestUtils.assertThrows(log, () -> {
                IgniteConfiguration cfg = new IgniteConfiguration()
                    .setSqlQueryMemoryQuota(-1);

                QueryMemoryManager memoryMgr = new QueryMemoryManager(newContext(cfg));
            }, IllegalArgumentException.class,
            "Ouch! Argument is invalid: Sql query memory quota must be >= 0. But was -1");

        // Zero reservation block size.
        System.setProperty(IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE, "0");

        GridTestUtils.assertThrows(log, () -> {
                IgniteConfiguration cfg = new IgniteConfiguration();

                QueryMemoryManager memoryMgr = new QueryMemoryManager(newContext(cfg));
            }, IllegalArgumentException.class,
            "Ouch! Argument is invalid: Block size must be > 0. But was 0");

        // Negative reservation block size.
        System.setProperty(IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE, "-1");

        GridTestUtils.assertThrows(log, () -> {
                IgniteConfiguration cfg = new IgniteConfiguration();

                QueryMemoryManager memoryMgr = new QueryMemoryManager(newContext(cfg));
            }, IllegalArgumentException.class,
            "Ouch! Argument is invalid: Block size must be > 0. But was -1");
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testGlobalQuotaDisabled() throws IgniteCheckedException {
        IgniteConfiguration cfg = new IgniteConfiguration()
            .setSqlGlobalMemoryQuota(0);

        QueryMemoryManager memMgr = new QueryMemoryManager(newContext(cfg));

        // Check defaults for manager.
        assertManagerState(memMgr,
            0,
            DFLT_SQL_QUERY_MEMORY_QUOTA,
            DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            DFLT_MEMORY_RESERVATION_BLOCK_SIZE);

        QueryMemoryTracker tracker = memMgr.createQueryMemoryTracker(0);

        assertNull(tracker);

        tracker = memMgr.createQueryMemoryTracker(10);

        assertTrackerState(tracker,
            10,
            DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            10);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testTrackingDisabled() throws IgniteCheckedException {
        IgniteConfiguration cfg = new IgniteConfiguration()
            .setSqlGlobalMemoryQuota(0)
            .setSqlQueryMemoryQuota(0);

        QueryMemoryManager memMgr = new QueryMemoryManager(newContext(cfg));

        // Check defaults for manager.
        assertManagerState(memMgr,
            0,
            0,
            DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            DFLT_MEMORY_RESERVATION_BLOCK_SIZE);

        QueryMemoryTracker tracker = memMgr.createQueryMemoryTracker(0);

        assertNull(tracker);

        tracker = memMgr.createQueryMemoryTracker(10);

        assertTrackerState(tracker,
            10,
            DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            10);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testGlobalQuotaDisabledPerQueryQuotaEnabled() throws IgniteCheckedException {
        IgniteConfiguration cfg = new IgniteConfiguration()
            .setSqlGlobalMemoryQuota(0);
        QueryMemoryManager memMgr = new QueryMemoryManager(newContext(cfg));

        // Check defaults for manager.
        assertManagerState(memMgr,
            0,
            DFLT_SQL_QUERY_MEMORY_QUOTA,
            DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            DFLT_MEMORY_RESERVATION_BLOCK_SIZE);

        QueryMemoryTracker tracker = memMgr.createQueryMemoryTracker(30);

        assertTrackerState(tracker,
            30,
            DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            30);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testGlobalQuotaDisabledDefaultPerQueryQuotaEnabled() throws IgniteCheckedException {
        IgniteConfiguration cfg = new IgniteConfiguration()
            .setSqlGlobalMemoryQuota(0)
            .setSqlQueryMemoryQuota(33);
        QueryMemoryManager memMgr = new QueryMemoryManager(newContext(cfg));

        // Check defaults for manager.
        assertManagerState(memMgr,
            0,
            33,
            DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            DFLT_MEMORY_RESERVATION_BLOCK_SIZE);

        QueryMemoryTracker tracker = memMgr.createQueryMemoryTracker(0);

        assertTrackerState(tracker,
            33,
            DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            33);
    }

    /**
     * @param memMgr Memory manager.
     * @param expGlobalQuota Expected global quota.
     * @param expQryQuota Expected query quota.
     * @param expOffloadingEnabled Expected offloading enabled flag.
     * @param expBlockSize Expected block size.
     */
    private static void assertManagerState(QueryMemoryManager memMgr,
        long expGlobalQuota,
        long expQryQuota,
        boolean expOffloadingEnabled,
        long expBlockSize) {
        assertEquals(expGlobalQuota, memMgr.memoryLimit());
        assertEquals(expQryQuota, (long) GridTestUtils.getFieldValue(memMgr, "qryQuota"));
        assertEquals(expOffloadingEnabled, (boolean) GridTestUtils.getFieldValue(memMgr, "offloadingEnabled"));
        assertEquals(expBlockSize, (long) GridTestUtils.getFieldValue(memMgr, "blockSize"));
    }

    /**
     * @param memTracker Memory tracker.
     * @param expQuota Expected query quota.
     * @param expOffloadingEnabled Expected offloading enabled flag.
     * @param expBlockSize Expected block size.
     */
    private static void assertTrackerState(QueryMemoryTracker memTracker,
        long expQuota,
        boolean expOffloadingEnabled,
        long expBlockSize) {
        assertEquals(expQuota, memTracker.memoryLimit());
        assertEquals(expOffloadingEnabled, (boolean) GridTestUtils.getFieldValue(memTracker, "offloadingEnabled"));
        assertEquals(expBlockSize, (long) GridTestUtils.getFieldValue(memTracker, "blockSize"));
    }
}
