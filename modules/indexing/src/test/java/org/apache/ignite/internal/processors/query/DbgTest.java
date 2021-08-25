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

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 *
 */
//@WithSystemProperty(key = "DEFAULT_TOMBSTONE_TTL", value = "1000")
public class DbgTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEYS = 2000_000;

    /** Keys count. */
    private static final String ID0 = "const_identifier";

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startGrids(2);

        grid(0).cluster().state(ClusterState.ACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (String cacheName : grid(0).cacheNames())
            grid(0).cache(cacheName).destroy();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setMaxSize(100_000_000L)
                            .setPersistenceEnabled(true)
                    )
            );
    }

    /**
     * Check correct split sort expression that contains CAST function.
     *
     * Steps: - Creates test table and fill test data. - Executes query that contains sort expression with CAST
     * function. - The query must be executed successfully. - Checks query results.
     */
    @Test
    public void dbg() throws Exception {
        sql("CREATE TABLE IF NOT EXISTS TEST ( " +
            "ASSET_ID VARCHAR, " +
            "ATTRIBUTE_ID VARCHAR, " +
            "LANGUAGE VARCHAR, " +
            "ATTRIBUTE_VALUE VARCHAR, " +
            "ATTRIBUTES_STRING VARCHAR, " +
            "IS_MULTIPLE_VALUE VARCHAR, " +
            "CREATE_TIME TIMESTAMP, " +
            "MODIFY_TIME TIMESTAMP, " +
            "STATUS VARCHAR, " +
            "CONSTRAINT PK_PUBLIC_PIM_WCS_ASSET_DYNAMIC_ATTRIBUTES PRIMARY KEY (ASSET_ID,ATTRIBUTE_ID,LANGUAGE,ATTRIBUTE_VALUE)\n" +
            ") WITH \"" +
            "template=REPLICATED, " +
            "key_type=WCSKeyType10, " +
            "CACHE_NAME=WCSC10, " +
            "value_type=WCSValueType10" +
            "\"");

        sql("CREATE INDEX IF NOT EXISTS " +
            "TEST_IDX " +
            "ON TEST (ASSET_ID, LANGUAGE)"
        );

        awaitPartitionMapExchange();

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        GridTestUtils.runAsync(() -> {
                while (true) {
                    System.out.println("+++ " + grid(0).cache("WCSC10").size());
                    System.out.println("+++ TOMB " + grid(0).cachex("WCSC10").context().offheap().tombstonesCount());

                    U.sleep(10_000);
                }
            }
        );

//        GridTestUtils.runAsync(() -> {
//                while (true) {
//                    U.sleep(10_000);
//
//                    stopGrid(1);
//
//                    U.sleep(10_000);
//
//                    startGrid(1);
//                }
//            }
//        );

        GridTestUtils.runMultiThreaded(() -> {
            long cnt = 0;

            while (true) {
                try {
                    int keyIns = rnd.nextInt(KEYS);
                    sql("MERGE INTO TEST VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", row(keyIns));

                    if (cnt % 3 == 0) {
                        int keyRm = rnd.nextInt(KEYS);
                        sql("DELETE FROM TEST WHERE ASSET_ID=? ", row(keyRm)[0]);
                    }

                    if (cnt % 2 == 0) {
                        int keyUpd = rnd.nextInt(KEYS);
                        sql("UPDATE TEST SET MODIFY_TIME=? WHERE ASSET_ID=?", row(keyUpd)[7], row(keyUpd)[0]);
                    }

                    cnt++;
                }
                catch (Exception e) {
                    String msg = e.getMessage();

                    if (
                        !msg.contains("Ignite instance with provided name doesn't exist")
                            && !msg.contains("Failed to execute query (grid is stopping)")
                    )
                        log.error("+++ ", e);
                }
            }
        }, 10, "sql-upd");
    }

    /**
     *
     */
    Object[] row(int id) {
        return new Object[] {
            "ASSET_ID_" + id,
            "ATTRIBUTE_ID_" + id % 100,
            "LANGUAGE_" + id % 20,
            "ATTR_" + id % 10,
            "ARRT_STR_" + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID(),
            "IS_MULTIPLE_VALUE_N",
            new Timestamp(U.currentTimeMillis()),
            new Timestamp(U.currentTimeMillis()),
            null
        };
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return grid(1).context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setArgs(args), false);
    }

    /**
     * @param qry Query.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> execute(SqlFieldsQuery qry) {
        return grid(1).context().query().querySqlFields(qry, false);
    }
}
