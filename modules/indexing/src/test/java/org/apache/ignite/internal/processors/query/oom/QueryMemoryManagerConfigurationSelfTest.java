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
import org.apache.ignite.internal.processors.query.GridQueryMemoryMetricProvider;
import org.apache.ignite.internal.processors.query.h2.QueryMemoryManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SQL_QUERY_GLOBAL_MEMORY_QUOTA;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SQL_QUERY_MEMORY_QUOTA;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SQL_QUERY_OFFLOADING_ENABLED;
import static org.apache.ignite.internal.processors.query.h2.QueryMemoryManager.DFLT_MEMORY_RESERVATION_BLOCK_SIZE;
import static org.apache.ignite.internal.util.IgniteUtils.GB;
import static org.apache.ignite.internal.util.IgniteUtils.KB;
import static org.apache.ignite.internal.util.IgniteUtils.MB;

/**
 * Unit tests for memory manager and memory tracker
 */
public class QueryMemoryManagerConfigurationSelfTest extends GridCommonAbstractTest {
    /** */
    private static long DFLT_GLOBAL_QUOTA = U.parseBytes(DFLT_SQL_QUERY_GLOBAL_MEMORY_QUOTA);

    /** */
    private static long DFLT_QUERY_QUOTA = U.parseBytes(DFLT_SQL_QUERY_MEMORY_QUOTA);

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
            DFLT_GLOBAL_QUOTA,
            DFLT_QUERY_QUOTA,
            DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            DFLT_MEMORY_RESERVATION_BLOCK_SIZE);

        GridQueryMemoryMetricProvider tracker = memMgr.createQueryMemoryTracker(0);

        // Check defaults for tracker
        assertTrackerState(tracker,
            0,
            DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            DFLT_MEMORY_RESERVATION_BLOCK_SIZE);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testQuotaNonDefaults() throws IgniteCheckedException {
        IgniteConfiguration cfg = new IgniteConfiguration()
            .setSqlGlobalMemoryQuota("20k")
            .setSqlQueryMemoryQuota("10K")
            .setSqlOffloadingEnabled(!DFLT_SQL_QUERY_OFFLOADING_ENABLED);

        System.setProperty(IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE, "5000");

        QueryMemoryManager memMgr = new QueryMemoryManager(newContext(cfg));

        assertManagerState(memMgr,
            20 * 1024,
            10 * 1024,
            !DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            5_000);

        GridQueryMemoryMetricProvider tracker = memMgr.createQueryMemoryTracker(0);

        assertTrackerState(tracker,
            10 * 1024,
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
                    .setSqlGlobalMemoryQuota("-1");

                QueryMemoryManager memoryMgr = new QueryMemoryManager(newContext(cfg));
            }, IllegalArgumentException.class,
            "Ouch! Argument is invalid: Sql global memory quota must be >= 0: quotaSize=-1");

        // Negative query quota.
        GridTestUtils.assertThrows(log, () -> {
                IgniteConfiguration cfg = new IgniteConfiguration()
                    .setSqlQueryMemoryQuota("-1");

                QueryMemoryManager memoryMgr = new QueryMemoryManager(newContext(cfg));
            }, IllegalArgumentException.class,
            "Ouch! Argument is invalid: Sql query memory quota must be >= 0: quotaSize=-1");

        // Zero reservation block size.
        System.setProperty(IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE, "0");

        GridTestUtils.assertThrows(log, () -> {
                IgniteConfiguration cfg = new IgniteConfiguration();

                QueryMemoryManager memoryMgr = new QueryMemoryManager(newContext(cfg));
            }, IllegalArgumentException.class,
            "Ouch! Argument is invalid: Block size must be > 0: blockSize=0");

        // Negative reservation block size.
        System.setProperty(IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE, "-1");

        GridTestUtils.assertThrows(log, () -> {
                IgniteConfiguration cfg = new IgniteConfiguration();

                QueryMemoryManager memoryMgr = new QueryMemoryManager(newContext(cfg));
            }, IllegalArgumentException.class,
            "Ouch! Argument is invalid: Block size must be > 0: blockSize=-1");
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testGlobalQuotaDisabled() throws IgniteCheckedException {
        IgniteConfiguration cfg = new IgniteConfiguration()
            .setSqlGlobalMemoryQuota("0");

        QueryMemoryManager memMgr = new QueryMemoryManager(newContext(cfg));

        // Check defaults for manager.
        assertManagerState(memMgr,
            0,
            DFLT_QUERY_QUOTA,
            DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            DFLT_MEMORY_RESERVATION_BLOCK_SIZE);

        GridQueryMemoryMetricProvider tracker = memMgr.createQueryMemoryTracker(0);

        assertTrackerState(tracker,
            0,
            DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            DFLT_MEMORY_RESERVATION_BLOCK_SIZE);

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
    public void testGlobalQuotaDisabledDefaultPerQueryQuotaEnabled() throws IgniteCheckedException {
        IgniteConfiguration cfg = new IgniteConfiguration()
            .setSqlGlobalMemoryQuota("0")
            .setSqlQueryMemoryQuota("33");
        QueryMemoryManager memMgr = new QueryMemoryManager(newContext(cfg));

        // Check defaults for manager.
        assertManagerState(memMgr,
            0,
            33,
            DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            DFLT_MEMORY_RESERVATION_BLOCK_SIZE);

        GridQueryMemoryMetricProvider tracker = memMgr.createQueryMemoryTracker(0);

        assertTrackerState(tracker,
            33,
            DFLT_SQL_QUERY_OFFLOADING_ENABLED,
            33);
    }

    /**
     *
     */
    @Test
    public void testQuotaParser() {
        // Positive cases.
        assertEquals(0, U.parseBytes("0"));
        assertEquals(1, U.parseBytes("1"));
        assertEquals(-1, U.parseBytes("-1 "));
        assertEquals(1001, U.parseBytes("1001"));
        assertEquals(-5050, U.parseBytes(" -5050"));

        assertEquals(0, U.parseBytes("0k"));
        assertEquals(1024, U.parseBytes("1K"));
        assertEquals(-1 * KB, U.parseBytes("-1k"));
        assertEquals(10010 * 1024, U.parseBytes(" 10010k "));
        assertEquals(-50508 * KB, U.parseBytes("-50508K"));

        assertEquals(0, U.parseBytes("0M"));
        assertEquals(1024 * KB, U.parseBytes("1m"));
        assertEquals(-1 * KB * 1024, U.parseBytes("-1M"));
        assertEquals(110 * MB, U.parseBytes("110M"));
        assertEquals(-50508 * MB, U.parseBytes("-50508m"));

        assertEquals(0, U.parseBytes("0G"));
        assertEquals(1024 * KB * KB, U.parseBytes("  1g"));
        assertEquals(-1 * GB, U.parseBytes("  -1G  "));
        assertEquals(110 * MB * KB, U.parseBytes("110g    "));
        assertEquals(-58 * GB, U.parseBytes("-58G    "));

        final long curMaxHeap = Runtime.getRuntime().maxMemory();
        final double delta = curMaxHeap * 0.001;

        assertEquals(0, U.parseBytes("0%"), delta);
        assertEquals(curMaxHeap * 0.01, U.parseBytes("  1%"), delta);
        assertEquals(curMaxHeap * 0.2, U.parseBytes("  20%"), delta);
        assertEquals(curMaxHeap, U.parseBytes("100%    "), delta);

        // Negative cases.
        assertThrows("");
        assertThrows("-");
        assertThrows("o");
        assertThrows("- 1");
        assertThrows("k");
        assertThrows("K");
        assertThrows("m");
        assertThrows("M");
        assertThrows("g");
        assertThrows("G");
        assertThrows("1 g");
        assertThrows("2 m");
        assertThrows("3 g");
        assertThrows("4kk");
        assertThrows("5kg");
        assertThrows("6 %");
        assertThrows("5k5");

        GridTestUtils.assertThrows(log, () -> {
                U.parseBytes("-1%");
            }, IllegalArgumentException.class,
            "The percentage should be in the range from 0 to 100, but was: -1");
        GridTestUtils.assertThrows(log, () -> {
                U.parseBytes("101%");
            }, IllegalArgumentException.class,
            "The percentage should be in the range from 0 to 100, but was: 101");
    }

    /**
     * @param s String to parse.
     */
    private void assertThrows(String s) {
        GridTestUtils.assertThrows(log, () -> {
            U.parseBytes(s);
        }, IllegalArgumentException.class,
            "Wrong format of bytes string. It is expected to be a number or a number followed by one of the symbols: 'k', 'm', 'g', '%'.\n" +
            " For example: '10000', '10k', '33m', '2G'. But was: " + s);
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
    private static void assertTrackerState(GridQueryMemoryMetricProvider memTracker,
        long expQuota,
        boolean expOffloadingEnabled,
        long expBlockSize) {
        assertEquals(expQuota, (long) GridTestUtils.getFieldValue(memTracker, "quota"));
        assertEquals(expOffloadingEnabled, (boolean) GridTestUtils.getFieldValue(memTracker, "offloadingEnabled"));
        assertEquals(expBlockSize, (long) GridTestUtils.getFieldValue(memTracker, "blockSize"));
    }
}
