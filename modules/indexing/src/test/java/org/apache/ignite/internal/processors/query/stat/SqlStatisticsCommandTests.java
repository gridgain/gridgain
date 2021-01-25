package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

public class SqlStatisticsCommandTests extends StatisticsAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
        grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);
    }


    /**
     * Create two table and analyze one by one and in single batch.
     */
    @Test
    public void testAnalyze() throws IgniteCheckedException {
        runSql("DROP TABLE IF EXISTS TEST");
        runSql("DROP TABLE IF EXISTS TEST2");
        clearStat();
        runSql("CREATE TABLE TEST(id int primary key, name varchar)");
        runSql("CREATE TABLE TEST2(id int primary key, name varchar)");

        runSql("CREATE INDEX TEXT_NAME ON TEST(NAME);");
        runSql("ANALYZE TEST");
        testStatsExists(SCHEMA, "TEST");
        runSql("ANALYZE PUBLIC.TEST2");
        testStatsExists(SCHEMA, "TEST2");
        clearStat();
        runSql("ANALYZE PUBLIC.TEST, test2");
        testStatsExists(SCHEMA, "TEST");
        testStatsExists(SCHEMA, "TEST2");

    }

    private void clearStat() throws IgniteCheckedException {
        IgniteStatisticsManager statMgr = grid(0).context().query().getIndexing().statsManager();
        statMgr.clearObjectStatistics(new StatisticsTarget(SCHEMA, "TEST"),
            new StatisticsTarget(SCHEMA, "TEST2"));

    }

    /**
     * Test statistics exists on all nodes.
     *
     * @param schema Schema name.
     * @param obj Object name.
     */
    private void testStatsExists(String schema, String obj) {
        for (Ignite node : G.allGrids())
            assertNotNull("No statistics for " + schema + "." + obj,
                ((IgniteEx)node).context().query().getIndexing().statsManager().getLocalStatistics(schema, obj));

    }
}
