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

package org.apache.ignite.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Verify we are able to escape "_" character in the metadata request.
 */
public class JdbcThinMetadataSqlMatchTest extends GridCommonAbstractTest {
    /** Connection. */
    private static Connection conn;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgniteEx ign = startGrid(0);

        conn = GridTestUtils.connect(ign, null);
    }

    /**
     * Execute ddl query via jdbc driver.
     */
    protected void executeDDl(String sql) throws SQLException {
        try (PreparedStatement upd = conn.prepareStatement(sql)) {
            upd.executeUpdate();
        }
    }

    /**
     * Get tables by name pattern using jdbc metadata request.
     *
     * @param tabNamePtrn Table name pattern.
     */
    protected List<String> getTableNames(String tabNamePtrn) throws SQLException {
        ArrayList<String> names = new ArrayList<>();

        try (ResultSet tabsRs =
                 conn.getMetaData().getTables(null, null, tabNamePtrn, new String[] {"TABLE"})) {
            while (tabsRs.next())
                names.add(tabsRs.getString("TABLE_NAME"));
        }

        // Actually metadata should be sorted by TABLE_NAME but it's broken.
        Collections.sort(names);

        return names;
    }

    /** Create tables. */
    @Before
    public void createTables() throws Exception {
        executeDDl("CREATE TABLE MY_FAV_TABLE (id INT PRIMARY KEY, val VARCHAR)");
        executeDDl("CREATE TABLE MY0FAV0TABLE (id INT PRIMARY KEY, val VARCHAR)");
        executeDDl("CREATE TABLE OTHER_TABLE (id INT PRIMARY KEY, val VARCHAR)");
    }

    /** Drop tables. */
    @After
    public void dropTables() throws Exception {
        // two tables that both matched by "TABLE MY_FAV_TABLE" sql pattern:
        executeDDl("DROP TABLE MY_FAV_TABLE");
        executeDDl("DROP TABLE MY0FAV0TABLE");

        // and another one that doesn't:
        executeDDl("DROP TABLE OTHER_TABLE");
    }

    /**
     * Test for escaping the "_" character in the table metadata request
     */
    @Test
    public void testTablesMatch() throws SQLException {
        assertEqualsCollections(asList("MY0FAV0TABLE", "MY_FAV_TABLE"), getTableNames("MY_FAV_TABLE"));
        assertEqualsCollections(singletonList("MY_FAV_TABLE"), getTableNames("MY\\_FAV\\_TABLE"));

        assertEqualsCollections(Collections.emptyList(), getTableNames("\\%"));
        assertEqualsCollections(asList("MY0FAV0TABLE", "MY_FAV_TABLE", "OTHER_TABLE"), getTableNames("%"));

        assertEqualsCollections(Collections.emptyList(), getTableNames(""));
        assertEqualsCollections(asList("MY0FAV0TABLE", "MY_FAV_TABLE", "OTHER_TABLE"), getTableNames(null));
    }

    /**
     * Assert that collections contains the same elements regardless their order. Each element from the second
     * collection should be met in the first one exact the same times. This method is required in this test because
     *
     * @param exp Expected.
     * @param actual Actual.
     */
    private void assertEqNoOrder(Collection<String> exp, Collection<String> actual) {
        ArrayList<String> expSorted = new ArrayList<>(exp);
        ArrayList<String> actSorted = new ArrayList<>(exp);

        Collections.sort(expSorted);
        Collections.sort(actSorted);

        assertEqualsCollections(expSorted, actSorted);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        try {
            conn.close();

            conn = null;

            stopAllGrids();
        }
        finally {
            super.afterTestsStopped();
        }
    }
}
