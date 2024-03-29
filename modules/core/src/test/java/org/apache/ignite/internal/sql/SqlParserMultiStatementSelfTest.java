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

package org.apache.ignite.internal.sql;

import org.apache.ignite.internal.sql.command.SqlBeginTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlCommitTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlCreateIndexCommand;
import org.junit.Test;

/**
 * Parser test for multi-statement queries.
 */
public class SqlParserMultiStatementSelfTest extends SqlParserAbstractSelfTest {
    /**
     * Check that empty statements don't affect regular ones.
     */
    @Test
    public void testEmptyStatements() {
        String sql = ";;;CREATE INDEX TEST on TABLE1(id)  ; ;   BEGIN   ;;;";

        SqlParser parser = new SqlParser("schema", sql);

        // Haven't parse anything yet.
        assertEquals(null, parser.lastCommandSql());
        assertEquals(sql, parser.remainingSql());

        SqlCommand create = parser.nextCommand();

        assertTrue(create instanceof SqlCreateIndexCommand);
        assertEquals("CREATE INDEX TEST on TABLE1(id)", parser.lastCommandSql());
        assertEquals(" ;   BEGIN   ;;;", parser.remainingSql());

        SqlCommand begin = parser.nextCommand();

        assertTrue(begin instanceof SqlBeginTransactionCommand);
        assertEquals("BEGIN", parser.lastCommandSql());
        assertEquals(";;", parser.remainingSql());

        SqlCommand emptyCmd = parser.nextCommand();
        assertEquals(null, emptyCmd);
        assertEquals(null, parser.lastCommandSql());
        assertEquals(null, parser.remainingSql());
    }

    /**
     * Check that comments between statements work.
     */
    @Test
    public void testComments() {
        String sql = " -- Creating new index \n" +
            " CREATE INDEX IDX1 on TABLE1(id); \n" +
            " -- Creating one more index \n" +
            " CREATE INDEX IDX2 on TABLE2(id); \n" +
            " -- All done.";

        SqlParser parser = new SqlParser("schema", sql);

        // Haven't parse anything yet.
        assertEquals(null, parser.lastCommandSql());
        assertEquals(sql, parser.remainingSql());

        SqlCommand cmd = parser.nextCommand();

        assertTrue(cmd instanceof SqlCreateIndexCommand);
        assertEquals("CREATE INDEX IDX1 on TABLE1(id)", parser.lastCommandSql());
        assertEquals(" \n -- Creating one more index \n" +
            " CREATE INDEX IDX2 on TABLE2(id); \n" +
            " -- All done.", parser.remainingSql());

        cmd = parser.nextCommand();

        assertTrue(cmd instanceof SqlCreateIndexCommand);
        assertEquals("CREATE INDEX IDX2 on TABLE2(id)", parser.lastCommandSql());
        assertEquals(" \n -- All done.", parser.remainingSql());

        cmd = parser.nextCommand();

        assertNull(cmd);
        assertEquals(null, parser.lastCommandSql());
        assertEquals(null, parser.remainingSql());
    }

    @Test
    public void testEmptyTransaction() {
        String beginSql = "BEGIN  ;" + "  \n";
        String noteSql = "  -- Let's start an empty transaction; $1M idea!\n";
        String commitSql = "COMMIT;" + ";;";

        String sql = beginSql +
            noteSql +
            commitSql;

        SqlParser parser = new SqlParser("schema", sql);

        SqlCommand begin = parser.nextCommand();

        assertTrue(begin instanceof SqlBeginTransactionCommand);

        assertEquals("BEGIN", parser.lastCommandSql());
        assertEquals("  \n" + noteSql + commitSql, parser.remainingSql());

        SqlCommand commit = parser.nextCommand();

        assertTrue(commit instanceof SqlCommitTransactionCommand);
        assertEquals("COMMIT", parser.lastCommandSql());
        assertEquals(";;", parser.remainingSql());

        SqlCommand emptyCmd = parser.nextCommand();

        assertEquals(null, emptyCmd);
        assertEquals(null, parser.lastCommandSql());
        assertEquals(null, parser.remainingSql());
    }
}
