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

package org.apache.ignite.internal.processors.query.h2;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.QueryUtils.sysSchemaName;

/**
 * Test expose SPATIAL indexes through SQL system view INDEXES.
 */
public class H2IndexesSystemViewTest extends GridCommonAbstractTest {
    /** Client instance name. */
    private static final String CLIENT = "client";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        return super.getConfiguration().setCacheConfiguration(new CacheConfiguration().setName(DEFAULT_CACHE_NAME));
    }

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(getConfiguration());
        startGrid(getConfiguration().setClientMode(true).setIgniteInstanceName(CLIENT));
    }

    /**
     * Test indexes system view.
     */
    @Test
    public void testIndexesView() {
        execSql("CREATE TABLE PUBLIC.AFF_CACHE (ID1 INT, ID2 INT, GEOM GEOMETRY, PRIMARY KEY (ID1))");

        execSql("CREATE SPATIAL INDEX IDX_GEO_1 ON PUBLIC.AFF_CACHE(GEOM)");

        String idxSql = "SELECT " +
            " CACHE_ID," +
            " CACHE_NAME," +
            " SCHEMA_NAME," +
            " TABLE_NAME," +
            " INDEX_NAME," +
            " INDEX_TYPE," +
            " COLUMNS," +
            " IS_PK," +
            " IS_UNIQUE," +
            " INLINE_SIZE" +
            " FROM " + sysSchemaName() + ".INDEXES ORDER BY TABLE_NAME, INDEX_NAME";

        List<List<?>> srvNodeIndexes = execSql(idxSql);

        List<List<?>> clientNodeNodeIndexes = execSql(grid(CLIENT), idxSql);

        // TODO GG-43558 Client nodes currently return inline size = -1 for BPLUS indexes.
        // for (List<?> idx : clientNodeNodeIndexes)
        // assertTrue(srvNodeIndexes.contains(idx));

        Object[][] expectedResults = {
            {-825022849, "SQL_PUBLIC_AFF_CACHE", "PUBLIC", "AFF_CACHE", "IDX_GEO_1", "SPATIAL", "\"GEOM\" ASC", false, false, null},
            {-825022849, "SQL_PUBLIC_AFF_CACHE", "PUBLIC", "AFF_CACHE", "__SCAN_", "SCAN", null, false, false, null},
            {-825022849, "SQL_PUBLIC_AFF_CACHE", "PUBLIC", "AFF_CACHE", "_key_PK", "BTREE", "\"ID1\" ASC", true, true, 5},
            {-825022849, "SQL_PUBLIC_AFF_CACHE", "PUBLIC", "AFF_CACHE", "_key_PK_hash", "HASH", "\"ID1\" ASC", false, true, null}
        };

        // TODO GG-43558 Client nodes currently return inline size = -1 for BPLUS indexes.
        Object[][] clientExpectedResults = {
                {-825022849, "SQL_PUBLIC_AFF_CACHE", "PUBLIC", "AFF_CACHE", "IDX_GEO_1", "SPATIAL", "\"GEOM\" ASC", false, false, null},
                {-825022849, "SQL_PUBLIC_AFF_CACHE", "PUBLIC", "AFF_CACHE", "__SCAN_", "SCAN", null, false, false, null},
                {-825022849, "SQL_PUBLIC_AFF_CACHE", "PUBLIC", "AFF_CACHE", "_key_PK", "BTREE", "\"ID1\" ASC", true, true, -1},
                {-825022849, "SQL_PUBLIC_AFF_CACHE", "PUBLIC", "AFF_CACHE", "_key_PK_hash", "HASH", "\"ID1\" ASC", false, true, null}
        };

        checkResults(expectedResults, srvNodeIndexes);
        checkResults(clientExpectedResults, clientNodeNodeIndexes);

        // TODO
        Object[] expSrvInlineSizeRes = {null, null, 5, null};
        Object[] expClientInlineSizeRes = {null, null, -1, null};

        String inlineSizesSql = "SELECT INLINE_SIZE" +
                " FROM " + sysSchemaName() + ".INDEXES ORDER BY TABLE_NAME, INDEX_NAME";

        List<List<?>> srvNodeInlineSizes = execSql(inlineSizesSql);

        List<List<?>> clientNodeInlineSizes = execSql(grid(CLIENT), inlineSizesSql);

        for (int i = 0; i < expSrvInlineSizeRes.length; i++) {
            assertEquals(expSrvInlineSizeRes[i], srvNodeInlineSizes.get(i).get(0));
            assertEquals(expClientInlineSizeRes[i], clientNodeInlineSizes.get(i).get(0));
        }
    }

    private static void checkResults(Object[][] expectedResults, List<List<?>> nodeIndexes) {
        assertEquals(expectedResults.length, nodeIndexes.size());

        for (int i = 0; i < nodeIndexes.size(); i++) {
            List<?> resRow = nodeIndexes.get(i);

            Object[] expRow = expectedResults[i];

            assertEquals(expRow.length, resRow.size());

            for (int j = 0; j < expRow.length; j++)
                assertEquals(expRow[j], resRow.get(j));
        }
    }

    /**
     * Check precision and scale attributes for the {@code GEOMETRY} type in the {@code TABLE_COLUMNS} system view.
     */
    @Test
    public void testGeometryColumnPrecisionAndScale() {
        try {
            execSql("CREATE TABLE PUBLIC.TEST_GEOMETRY_COLUMN_META (ID INT PRIMARY KEY, GEOM GEOMETRY)");

            List<List<?>> rows = execSql("SELECT PRECISION, SCALE FROM " + sysSchemaName() +
                ".TABLE_COLUMNS WHERE TABLE_NAME='TEST_GEOMETRY_COLUMN_META' and COLUMN_NAME='GEOM'");

            assertEquals(1, rows.size());

            List<?> row = F.first(rows);

            assertEquals(H2Utils.GEOMETRY_DEFAULT_PRECISION, row.get(0));
            assertEquals(0, row.get(1));
        } finally {
            execSql("DROP TABLE IF EXISTS PUBLIC.TEST_GEOMETRY_COLUMN_META");
        }
    }

    /**
     * @param sql Sql.
     * @param args Args.
     */
    private List<List<?>> execSql(String sql, Object... args) {
        return execSql(grid(), sql, args);
    }

    /**
     * @param ignite Ignite.
     * @param sql Sql.
     * @param args Args.
     */
    @SuppressWarnings("unchecked")
    private List<List<?>> execSql(Ignite ignite, String sql, Object... args) {
        IgniteCache cache = ignite.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        if (args != null && args.length > 0)
            qry.setArgs(args);

        return cache.query(qry).getAll();
    }

}
