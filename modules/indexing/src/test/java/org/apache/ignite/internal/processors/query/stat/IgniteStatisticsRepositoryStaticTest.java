package org.apache.ignite.internal.processors.query.stat;

import org.gridgain.internal.h2.value.ValueInt;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Test of static methods of Ignite statistics repository.
 */
public class IgniteStatisticsRepositoryStaticTest extends StatisticsAbstractTest {
    /** First default key. */
    protected static final StatisticsKey K1 = new StatisticsKey(SCHEMA, "tab1");

    /** Second default key. */
    protected static final StatisticsKey K2 = new StatisticsKey(SCHEMA, "tab2");

    /** Column statistics with 100 nulls. */
    protected ColumnStatistics cs1 = new ColumnStatistics(null, null, 100, 0, 100,
        0, new byte[0]);

    /** Column statistics with 100 integers 0-100. */
    protected ColumnStatistics cs2 = new ColumnStatistics(ValueInt.get(0), ValueInt.get(100), 0, 100, 100,
        4, new byte[0]);

    /** Column statistics with 0 rows. */
    protected ColumnStatistics cs3 = new ColumnStatistics(null, null, 0, 0, 0, 0, new byte[0]);

    /** Column statistics with 100 integers 0-10. */
    protected ColumnStatistics cs4 = new ColumnStatistics(ValueInt.get(0), ValueInt.get(10), 0, 10, 100,
        4, new byte[0]);

    /**
     * Test object statistics add:
     *
     * 1) Add statistics with partially the same columns.
     * 2) Add statistics with new columns.
     * 3) Add statistics with the same columns.
     */
    @Test
    public void addTest() {
        // 1) Add statistics with partially the same columns.
        HashMap<String, ColumnStatistics> colStat1 = new HashMap<>();
        colStat1.put("col1", cs1);
        colStat1.put("col2", cs2);

        HashMap<String, ColumnStatistics> colStat2 = new HashMap<>();
        colStat2.put("col2", cs3);
        colStat2.put("col3", cs4);

        ObjectStatisticsImpl os1 = new ObjectStatisticsImpl(100, colStat1);
        ObjectStatisticsImpl os2 = new ObjectStatisticsImpl(101, colStat2);

        ObjectStatisticsImpl sumStat1 = IgniteStatisticsRepository.add(os1, os2);

        assertEquals(101, sumStat1.rowCount());
        assertEquals(3, sumStat1.columnsStatistics().size());
        assertEquals(cs3, sumStat1.columnStatistics("col2"));

        // 2) Add statistics with new columns.
        ObjectStatisticsImpl os3 = new ObjectStatisticsImpl(101, Collections.singletonMap("col3", cs3));

        ObjectStatisticsImpl sumStat2 = IgniteStatisticsRepository.add(os1, os3);

        assertEquals(3, sumStat2.columnsStatistics().size());

        // 3) Add statistics with the same columns.
        HashMap<String, ColumnStatistics> colStat3 = new HashMap<>();
        colStat3.put("col1", cs3);
        colStat3.put("col2", cs4);

        ObjectStatisticsImpl os4 = new ObjectStatisticsImpl(99, colStat3);

        ObjectStatisticsImpl sumStat3 = IgniteStatisticsRepository.add(os1, os4);

        assertEquals(99, sumStat3.rowCount());
        assertEquals(2, sumStat3.columnsStatistics().size());
        assertEquals(cs3, sumStat3.columnStatistics("col1"));
    }

    /**
     * 1) Remove not existing column.
     * 2) Remove some columns.
     * 3) Remove all columns.
     */
    @Test
    public void subtractTest() {
        HashMap<String, ColumnStatistics> colStat1 = new HashMap<>();
        colStat1.put("col1", cs1);
        colStat1.put("col2", cs2);

        ObjectStatisticsImpl os = new ObjectStatisticsImpl(100, colStat1);

        // 1) Remove not existing column.
        ObjectStatisticsImpl os1 = IgniteStatisticsRepository.subtract(os, Collections.singleton("col0"));

        assertEquals(os, os1);

        // 2) Remove some columns.
        ObjectStatisticsImpl os2 = IgniteStatisticsRepository.subtract(os, Collections.singleton("col1"));

        assertEquals(1, os2.columnsStatistics().size());
        assertEquals(cs2, os2.columnStatistics("col2"));

        // 3) Remove all columns.
        ObjectStatisticsImpl os3 = IgniteStatisticsRepository.subtract(os,
            Arrays.stream(new String[] {"col2", "col1"}).collect(Collectors.toSet()));

        assertTrue(os3.columnsStatistics().isEmpty());
    }
}
