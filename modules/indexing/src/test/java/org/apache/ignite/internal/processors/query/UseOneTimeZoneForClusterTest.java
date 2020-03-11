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
import java.util.Collections;
import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/**
 * Tests for use one timezone for all nodes in cluster.
 */
public class UseOneTimeZoneForClusterTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEYS_CNT = 1024;

    /** Time zones to check. */
    private static final String[] TIME_ZONES = {"EST5EDT", "IST", "Europe/Moscow"};

    /** Time zone ID for other JVM to start remote grid. */
    private String tzId;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @return Additional JVM args for remote instances.
     */
    @Override protected List<String> additionalRemoteJvmArgs() {
        return Collections.singletonList("-Duser.timezone=" + tzId);
    }

    /**
     * @param name Remote node name.
     * @param tzId Default time zone for the node.
     */
    private void startRemoteGrid(String name, String tzId) throws Exception {
        this.tzId = tzId;

        startRemoteGrid(name, optimize(getConfiguration(name)), null);
    }

    /**
     */
    @Test
    public void test() throws Exception {
        startGrid(0);

        for (String tz : TIME_ZONES)
            startRemoteGrid("tz-" + tz, tz);

        startClientGrid("cli");

        sql("DROP TABLE IF EXISTS TZ_TEST", Collections.emptyList());

        sql("CREATE TABLE IF NOT EXISTS TZ_TEST (" +
            "id int, " +
            "dateVal DATE, " +
            "timeVal TIME, " +
            "tsVal TIMESTAMP, " +
            "PRIMARY KEY (id))", Collections.emptyList());

        // Use many row with different PK and the same date/time fields to be sure that
        // the each node will store at least one row.
        for (int i = 0; i < KEYS_CNT; ++i) {
            sql("INSERT INTO TZ_TEST (id, dateVal, timeVal, tsVal) " +
                    "VALUES (?, CAST(? AS DATE), CAST(? AS TIME), CAST(? AS TIMESTAMP))",
                Arrays.asList(
                    i,
                    "2019-09-09",
                    "09:09:09",
                    "2019-09-09 09:09:09.909"
                )
            );
        }


        List<List<?>> res = sql("SELECT " +
            "id, " +
            "CAST(dateVal AS VARCHAR), " +
            "CAST(timeVal AS VARCHAR), " +
            "CAST(tsVal AS VARCHAR) " +
            "FROM TZ_TEST ORDER BY id", Collections.emptyList());

        for (List<?> row : res) {
            assertEquals("2019-09-09", row.get(1));
            assertEquals("09:09:09", row.get(2));
            assertEquals("2019-09-09 09:09:09.909", row.get(3));
        }
    }

    /**
     * @param sql SQL query.
     * @return Results.
     */
    protected List<List<?>> sql(String sql, List<Object> params) throws Exception {
        GridQueryProcessor qryProc = grid("cli").context().query();

        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setSchema("PUBLIC").setArgs(params.toArray());

        return qryProc.querySqlFields(qry, true).getAll();
    }
}
