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

package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;

// t0d0 affinity key
// t0d0 driver node?
// t0d0 inline size?
// t0d0 asc/desc
public class SqlIndexesSystemViewTest extends AbstractIndexingCommonTest {
    private Ignite driver;

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);

        G.setClientMode(true);
        startGrid(3);
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        driver = grid(0);
    }

    @Override protected void afterTest() throws Exception {
        driver.destroyCaches(driver.cacheNames());

        super.afterTest();
    }

    @Test
    public void testNoTablesNoIndexes() throws Exception {
        for (Ignite ign : G.allGrids()) {
            List<List<?>> indexes = execSql(ign, "SELECT * FROM SYS.INDEXES ORDER BY TABLE_NAME, INDEX_NAME");

            if (!indexes.isEmpty())
                System.err.println(indexes);

            assertTrue(indexes.isEmpty());
        }
    }

    @Test
    public void testDdlTableIndexes() throws Exception {
        execSql(driver, "CREATE TABLE Person(id INT PRIMARY KEY, name VARCHAR)");

        List<Object> expInitial = Arrays.asList(
            Arrays.asList(-1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", "PUBLIC", "PERSON", "__SCAN_", "SCAN", null, false, false, null),
            Arrays.asList(-1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", "PUBLIC", "PERSON", "_key_PK", "BTREE", "\"ID\" ASC", true, true, 5),
            Arrays.asList(-1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", "PUBLIC", "PERSON", "_key_PK_hash", "HASH", "\"ID\" ASC", false, true, null)
        );

        for (Ignite ign : G.allGrids()) {
            List<List<?>> indexes = execSql(ign, "SELECT * FROM SYS.INDEXES ORDER BY INDEX_NAME");

            assertEqualsCollections(expInitial, indexes);
        }

        try {
            execSql(driver, "CREATE INDEX NameIdx on Person(surname)");

            fail("Exception expected");
        }
        catch (IgniteSQLException ignored) {
        }

        for (Ignite ign : G.allGrids()) {
            List<List<?>> indexes = execSql(ign, "SELECT * FROM SYS.INDEXES ORDER BY INDEX_NAME");

            assertEqualsCollections(expInitial, indexes);
        }

        execSql(driver, "CREATE INDEX NameIdx on Person(name)");

        List<Object> expSecondaryIndex = Arrays.asList(
            Arrays.asList(-1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", "PUBLIC", "PERSON", "NAMEIDX", "BTREE", "\"NAME\" ASC, \"ID\" ASC", false, false, 10),
            Arrays.asList(-1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", "PUBLIC", "PERSON", "__SCAN_", "SCAN", null, false, false, null),
            Arrays.asList(-1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", "PUBLIC", "PERSON", "_key_PK", "BTREE", "\"ID\" ASC", true, true, 5),
            Arrays.asList(-1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", "PUBLIC", "PERSON", "_key_PK_hash", "HASH", "\"ID\" ASC", false, true, null)
        );

        for (Ignite ign : G.allGrids()) {
            List<List<?>> indexes = execSql(ign, "SELECT * FROM SYS.INDEXES ORDER BY INDEX_NAME");

            assertEqualsCollections(expSecondaryIndex, indexes);
        }

        execSql(driver, "DROP INDEX NameIdx");

        for (Ignite ign : G.allGrids()) {
            List<List<?>> indexes = execSql(ign, "SELECT * FROM SYS.INDEXES ORDER BY INDEX_NAME");

            assertEqualsCollections(expInitial, indexes);
        }

        execSql(driver, "DROP TABLE Person");

        for (Ignite ign : G.allGrids()) {
            List<List<?>> indexes = execSql(ign, "SELECT * FROM SYS.INDEXES ORDER BY INDEX_NAME");

            assertTrue(indexes.isEmpty());
        }
    }

    private static List<List<?>> execSql(Ignite ign, String sql) {
        return ((IgniteEx)ign).context().query().querySqlFields(new SqlFieldsQuery(sql), false).getAll();
    }
}
