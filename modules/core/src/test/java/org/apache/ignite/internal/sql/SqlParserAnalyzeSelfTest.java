/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.internal.sql;

import org.apache.ignite.internal.processors.query.stat.StatisticsTarget;
import org.apache.ignite.internal.sql.command.SqlAnalyzeCommand;
import org.junit.Test;

/**
 * Tests for sql parser: ANALYZE command.
 */
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

        parseValidate("SOME_SCHEMA", "ANALYZE tbl, schema.tbl2",
            new StatisticsTarget(null, "TBL"), new StatisticsTarget("SCHEMA", "TBL2"));

        parseValidate(null, "ANALYZE tbl(a, b, c)",
            new StatisticsTarget(null, "TBL", "A", "B", "C"));

        parseValidate(null, "ANALYZE tbl(a), schema.tbl2(a,B)",
            new StatisticsTarget(null, "TBL", "A"),
            new StatisticsTarget("SCHEMA", "TBL2", "A", "B"));

        assertParseError(null, "ANALYZE p,", "Unexpected end of command");
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

        for (StatisticsTarget target : targets)
            assertTrue(cmd.targets().contains(target));
    }
}
