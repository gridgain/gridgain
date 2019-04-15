/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
