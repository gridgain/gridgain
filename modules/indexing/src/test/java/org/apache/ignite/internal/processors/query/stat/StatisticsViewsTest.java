package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for statistics related views.
 */
public abstract class StatisticsViewsTest extends StatisticsAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
        cleanPersistenceDir();

        //startGridsMultiThreaded(1);
        startGrid(0);
        grid(0).cluster().state(ClusterState.ACTIVE);

        grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        createSmallTable(null);
        collectStatistics(SMALL_TARGET);
    }

    /**
     * Check small table configuration in statistics column configuration view.
     */
    @Test
    public void testConfigurationView() throws Exception {
        List<List<Object>> config = Arrays.asList(
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "A", 1L),
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "B", 1L),
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "C", 1L)
        );

        U.sleep(999999);
        checkSqlResult("select * from SYS.STATISTICS_CONFIGURATION", null, config::equals);
    }

    /**
     * Check partition from small table in statistics partition data view.
     */
    @Test
    public void testPartitionDataView() throws Exception {
        List<List<Object>> partLines = Arrays.asList(
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "A", 0, null, null, null, 0, null, null, 1L, null)
        );

        checkSqlResult("select * from SYS.STATISTICS_PARTITION_DATA where PARTITION < 10", null, act -> {
            checkContains(partLines, act);
            return true;
        });
    }

    /**
     * Check that all expected line exists in actual.
     *
     * @param expected Expected lines, nulls mean any value.
     * @param actual Actual lines.
     */
    private void checkContains(List<List<Object>> expected, List<List<?>> actual) {
        assertTrue(expected.size() <= actual.size());

        assertTrue("Test may take too long with such datasets of actual = " + actual.size(), actual.size() <= 1024);

        for (List<Object> exp : expected) {
            boolean found = false;
            for (List<?> act : actual) {
                found = checkEqualWithNull(exp, act);
                if (found)
                    break;
            }
            if (!found)
                fail("Unable to found " + exp + " in specified dataset");
        }
    }

    /**
     * Compare expected line with actual one.
     *
     * @param expected Expected line, {@code null} value mean any value.
     * @param actual Actual line.
     * @return {@code true} if line are equal, {@code false} - otherwise.
     */
    private boolean checkEqualWithNull(List<Object> expected, List<?> actual) {
        assertEquals(expected.size(), actual.size());

        for (int i = 0; i < expected.size(); i++) {
            Object exp = expected.get(i);
            Object act = actual.get(i);
            if (exp != null && !exp.equals(act) && act != null)
                return false;
        }

        return true;
    }

    /**
     * Check small table local data in statistics local data view.
     */
    @Test
    public void testLocalDataView() throws Exception {
        long size = SMALL_SIZE;
        ObjectStatisticsImpl smallStat = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

        assertNotNull(smallStat);

        Timestamp tsA = new Timestamp(smallStat.columnStatistics("A").createdAt());
        Timestamp tsB = new Timestamp(smallStat.columnStatistics("B").createdAt());
        Timestamp tsC = new Timestamp(smallStat.columnStatistics("C").createdAt());

        List<List<Object>> localData = Arrays.asList(
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "A", size, size, 0, size, 4, 1L, tsA.toString()),
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "B", size, size, 0, size, 4, 1L, tsB.toString()),
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "C", size, 10L, 0, size, 4, 1L, tsC.toString())
        );

        checkSqlResult("select * from SYS.STATISTICS_LOCAL_DATA", null, localData::equals);
    }
}
