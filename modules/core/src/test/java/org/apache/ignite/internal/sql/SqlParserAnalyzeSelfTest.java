package org.apache.ignite.internal.sql;

import org.apache.ignite.internal.processors.query.stat.StatisticsTarget;
import org.apache.ignite.internal.sql.command.SqlAnalyzeCommand;
import org.junit.Test;

import java.util.List;

public class SqlParserAnalyzeSelfTest extends SqlParserAbstractSelfTest {
    /**
     * Tests for ANALYZE command.
     */
    @Test
    public void testAnalyze() {
        parseValidate(null, "ANALYZE tbl", new StatisticsTarget(null, "TBL"));
        parseValidate(null, "ANALYZE tbl;", new StatisticsTarget(null, "TBL"));
        parseValidate(null, "ANALYZE schema.tbl", new StatisticsTarget("SCHEMA", "TBL"));
        parseValidate(null, "ANALYZE schema.tbl;", new StatisticsTarget("SCHEMA", "TBL"));
        parseValidate(null, "ANALYZE schema.tbl(a)",
            new StatisticsTarget("SCHEMA", "TBL", "A"));
        parseValidate(null, "ANALYZE schema.tbl(a);",
            new StatisticsTarget("SCHEMA", "TBL", "A"));
        parseValidate(null, "ANALYZE tbl(a)", new StatisticsTarget(null, "TBL", "A"));
        parseValidate(null, "ANALYZE tbl(a);", new StatisticsTarget(null, "TBL", "A"));
        parseValidate(null, "ANALYZE tbl(a, b, c)",
            new StatisticsTarget(null, "TBL", "A", "B", "C"));
        parseValidate(null, "ANALYZE tbl(a), schema.tbl2(a,B)",
            new StatisticsTarget(null, "TBL", "A"),
            new StatisticsTarget("SCHEMA", "TBL2", "A", "B"));

        assertParseError(null, "ANALYZE p,", "Unexpected token: \",\"");
        assertParseError(null, "ANALYZE p()", "Unexpected token: \")\"");
    }

    /**
     * Parse command and validate it.
     *
     * @param schema Schema.
     * @param sql SQL text.
     * @param targets Expected targets.
     */
    private void parseValidate(String schema, String sql, StatisticsTarget... targets) {
        SqlAnalyzeCommand cmd = (SqlAnalyzeCommand)new SqlParser(schema, sql).nextCommand();

        validate(cmd, targets);
    }

    /**
     * Validate command.
     *
     * @param cmd Command to validate.
     * @param targets Expected targets.
     */
    private static void validate(SqlAnalyzeCommand cmd, StatisticsTarget... targets) {
        assertEquals(cmd.targets().size(), targets.length);

        for(StatisticsTarget target : targets)
            assertTrue(cmd.targets().contains(target));
    }
}
