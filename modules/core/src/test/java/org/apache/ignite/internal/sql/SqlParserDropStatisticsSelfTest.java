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

        assertParseError(null, "DROP STATISTICS p,", "Unexpected end of command");
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

    /**
     * Validate targets in Sql drop statistics command.
     *
     * @param cmd Command to validate.
     * @param targets Expected targets.
     */
    private static void validate(SqlDropStatisticsCommand cmd, StatisticsTarget... targets) {
        assertEquals(cmd.targets().size(), targets.length);

        for (StatisticsTarget target : targets)
            assertTrue(cmd.targets().contains(target));
    }
}
