package org.apache.ignite.internal.sql;

import org.apache.ignite.internal.processors.query.stat.StatisticsTarget;
import org.apache.ignite.internal.sql.command.SqlAnalyzeCommand;
import org.apache.ignite.internal.sql.command.SqlDropStatisticsCommand;
import org.junit.Test;

/**
 * Test for SQL parser: DROP STATISTICS command.
 */
public class SqlParserDropStatisticsSelfTest extends SqlParserAbstractSelfTest {
    /**
     * Tests for DROP STATISTICS command.
     */
    @Test
    public void testDrop() {
        parseValidate(null, "DROP STATISTICS tbl", new StatisticsTarget(null, "TBL"));
        parseValidate(null, "DROP STATISTICS tbl;", new StatisticsTarget(null, "TBL"));
        parseValidate(null, "DROP STATISTICS schema.tbl", new StatisticsTarget("SCHEMA", "TBL"));
        parseValidate(null, "DROP STATISTICS schema.tbl;", new StatisticsTarget("SCHEMA", "TBL"));
        parseValidate(null, "DROP STATISTICS schema.tbl(a)",
            new StatisticsTarget("SCHEMA", "TBL", "A"));
        parseValidate(null, "DROP STATISTICS schema.tbl(a);",
            new StatisticsTarget("SCHEMA", "TBL", "A"));
        parseValidate(null, "DROP STATISTICS tbl(a)", new StatisticsTarget(null, "TBL", "A"));
        parseValidate(null, "DROP STATISTICS tbl(a);", new StatisticsTarget(null, "TBL", "A"));
        parseValidate(null, "DROP STATISTICS tbl(a, b, c)",
            new StatisticsTarget(null, "TBL", "A", "B", "C"));
        parseValidate(null, "DROP STATISTICS tbl(a), schema.tbl2(a,B)",
            new StatisticsTarget(null, "TBL", "A"),
            new StatisticsTarget("SCHEMA", "TBL2", "A", "B"));

        assertParseError(null, "DROP STATISTICS p,", "Unexpected token: \",\"");
        assertParseError(null, "DROP STATISTICS p()", "Unexpected token: \")\"");
    }

    /**
     * Parse command and validate it.
     *
     * @param schema Schema.
     * @param sql SQL text.
     * @param targets Expected targets.
     */
    private void parseValidate(String schema, String sql, StatisticsTarget... targets) {
        SqlDropStatisticsCommand cmd = (SqlDropStatisticsCommand)new SqlParser(schema, sql).nextCommand();

        validate(cmd, targets);
    }


    private static void validate(SqlDropStatisticsCommand cmd, StatisticsTarget... targets) {
        assertEquals(cmd.targets().size(), targets.length);

        for(StatisticsTarget target : targets)
            assertTrue(cmd.targets().contains(target));
    }
}
