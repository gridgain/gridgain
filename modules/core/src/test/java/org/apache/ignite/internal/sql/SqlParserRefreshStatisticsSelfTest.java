package org.apache.ignite.internal.sql;

import org.apache.ignite.internal.processors.query.stat.StatisticsTarget;
import org.apache.ignite.internal.sql.command.SqlDropStatisticsCommand;
import org.apache.ignite.internal.sql.command.SqlRefreshStatitsicsCommand;
import org.junit.Test;

/**
 * Test for SQL parser: REFREST STATISTICS command.
 */
public class SqlParserRefreshStatisticsSelfTest extends SqlParserAbstractSelfTest {
    /**
     * Tests for REFRESH STATISTICS command.
     */
    @Test
    public void testRefresh() {
        parseValidate(null, "REFRESH STATISTICS tbl", new StatisticsTarget(null, "TBL"));
        parseValidate(null, "REFRESH STATISTICS tbl;", new StatisticsTarget(null, "TBL"));
        parseValidate(null, "REFRESH STATISTICS schema.tbl",
            new StatisticsTarget("SCHEMA", "TBL"));
        parseValidate(null, "REFRESH STATISTICS schema.tbl;",
            new StatisticsTarget("SCHEMA", "TBL"));
        parseValidate(null, "REFRESH STATISTICS schema.tbl(a)",
            new StatisticsTarget("SCHEMA", "TBL", "A"));
        parseValidate(null, "REFRESH STATISTICS schema.tbl(a);",
            new StatisticsTarget("SCHEMA", "TBL", "A"));
        parseValidate(null, "REFRESH STATISTICS tbl(a)",
            new StatisticsTarget(null, "TBL", "A"));
        parseValidate(null, "REFRESH STATISTICS tbl(a);",
            new StatisticsTarget(null, "TBL", "A"));
        parseValidate(null, "REFRESH STATISTICS tbl(a, b, c)",
            new StatisticsTarget(null, "TBL", "A", "B", "C"));
        parseValidate(null, "REFRESH STATISTICS tbl(a), schema.tbl2(a,B)",
            new StatisticsTarget(null, "TBL", "A"),
            new StatisticsTarget("SCHEMA", "TBL2", "A", "B"));

        assertParseError(null, "REFRESH STATISTICS p,", "Unexpected token: \",\"");
        assertParseError(null, "REFRESH STATISTICS p()", "Unexpected token: \")\"");
    }

    /**
     * Parse command and validate it.
     *
     * @param schema Schema.
     * @param sql SQL text.
     * @param targets Expected targets.
     */
    private void parseValidate(String schema, String sql, StatisticsTarget... targets) {
        SqlRefreshStatitsicsCommand cmd = (SqlRefreshStatitsicsCommand)new SqlParser(schema, sql).nextCommand();

        validate(cmd, targets);
    }

    /**
     * Validate command.
     *
     * @param cmd Command to validate.
     * @param targets Expected targets.
     */
    private static void validate(SqlRefreshStatitsicsCommand cmd, StatisticsTarget... targets) {
        assertEquals(cmd.targets().size(), targets.length);

        for(StatisticsTarget target : targets)
            assertTrue(cmd.targets().contains(target));
    }
}
