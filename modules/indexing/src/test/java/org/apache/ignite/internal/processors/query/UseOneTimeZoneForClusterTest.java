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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.junit.Test;

/**
 * Tests for use one timezone for all nodes in cluster.
 */
public class UseOneTimeZoneForClusterTest extends AbstractIndexingCommonTest {
    /** Initial node name. */
    protected static final String INIT_NODE_NAME = "init-node";

    /** Keys count. */
    private static final int KEYS_CNT = 1024;

    /** Time zones to check. */
    private static final String[] TIME_ZONES = {"EST5EDT", "IST", "Europe/Moscow"};

    /** Time zone ID for other JVM to start remote grid. */
    private String tzId;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (INIT_NODE_NAME.equals(igniteInstanceName)) {
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);
            cfg.setClientMode(true);
        }

        return cfg
            .setClientMode(igniteInstanceName.startsWith("cli"))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                )
            );
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
    private Ignite startRemoteGrid(String name, String tzId) throws Exception {
        this.tzId = tzId;

        return startRemoteGrid(name, optimize(getConfiguration(name)), null);
    }

    /**
     */
    @Test
    public void testServerNodes() throws Exception {
        IgniteEx ignInit = startGrid(INIT_NODE_NAME);

        for (String tz : TIME_ZONES)
            startRemoteGrid("tz-" + tz, tz);

        ignInit.cluster().active(true);

        sql("DROP TABLE IF EXISTS TZ_TEST", Collections.emptyList());

        sql("CREATE TABLE IF NOT EXISTS TZ_TEST (" +
            "id int, " +
            "dateVal DATE, " +
            "timeVal TIME, " +
            "tsVal TIMESTAMP, " +
            "PRIMARY KEY (id))", Collections.emptyList());

        fillData();

        checkDates();
    }

    /**
     */
    @Test
    public void testClientsInDifferentTimeZones() throws Exception {
        IgniteEx ignInit = startGrid(INIT_NODE_NAME);

        for (String tz : TIME_ZONES)
            startRemoteGrid("tz-" + tz, tz);

        ignInit.cluster().active(true);

        sql("DROP TABLE IF EXISTS TZ_TEST", Collections.emptyList());

        sql("CREATE TABLE IF NOT EXISTS TZ_TEST (" +
            "id int, " +
            "dateVal DATE, " +
            "timeVal TIME, " +
            "tsVal TIMESTAMP, " +
            "PRIMARY KEY (id))", Collections.emptyList());

        List<ClusterGroup> clients = new ArrayList<>();

        for (String tz : TIME_ZONES) {
            Ignite c = startRemoteGrid("cli-tz-" + tz, tz);

            clients.add(ignInit.cluster().forNode(c.cluster().localNode()));
        }

        for (int i = 0; i < KEYS_CNT; ++i) {
            ClusterGroup rmtCli = clients.get(i % clients.size());

            final int id = i;
            ignInit.compute(rmtCli).run(new IgniteRunnable() {
                @IgniteInstanceResource
                Ignite ign;

                @Override public void run() {
                    try {
                        sql(ign, "INSERT INTO TZ_TEST (id, dateVal, timeVal, tsVal) " +
                                "VALUES (?, CAST(? AS DATE), CAST(? AS TIME), CAST(? AS TIMESTAMP))",
                            Arrays.asList(
                                id,
                                "2019-09-09",
                                "09:09:09",
                                "2019-09-09 09:09:09.909"
                            )
                        );
                    }
                    catch (Exception e) {
                        throw new IgniteException(e);
                    }
                }
            });
        }

        checkDates();
    }

    /**
     */
    @Test
    public void testPersistence() throws Exception {
        IgniteEx ignInit = startGrid(INIT_NODE_NAME);

        Ignite ignPrev = startRemoteGrid("tz-" + TIME_ZONES[0], TIME_ZONES[0]);

        ignInit.cluster().active(true);

        sql("DROP TABLE IF EXISTS TZ_TEST", Collections.emptyList());

        sql("CREATE TABLE IF NOT EXISTS TZ_TEST (" +
            "id int, " +
            "dateVal DATE, " +
            "timeVal TIME, " +
            "tsVal TIMESTAMP, " +
            "PRIMARY KEY (id)) WITH \"TEMPLATE=REPLICATED\"", Collections.emptyList());

        fillData();

        for (int i = 1; i < TIME_ZONES.length; ++i) {
            String tz = TIME_ZONES[i];

            startRemoteGrid("tz-" + tz, tz);

            ignInit.cluster().setBaselineTopology(ignInit.cluster().topologyVersion());

            awaitPartitionMapExchange(false, true,
                Collections.singleton(ignInit.localNode()), false);

            stopGrid(ignPrev.name());

            checkDates();
        }
    }

    /**
     */
    private void fillData() throws Exception {
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
    }

    /**
     */
    protected void checkDates() throws Exception {
        List<List<?>> res = sql("SELECT " +
            "id, " +
            "CAST(dateVal AS VARCHAR), " +
            "CAST(timeVal AS VARCHAR), " +
            "CAST(tsVal AS VARCHAR) " +
            "FROM TZ_TEST ORDER BY id", Collections.emptyList());

        assertEquals(KEYS_CNT, res.size());

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
        return sql(grid(INIT_NODE_NAME), sql, params);
    }

    /**
     * @param sql SQL query.
     * @return Results.
     */
    public static List<List<?>> sql(Ignite ign, String sql, List<Object> params) throws Exception {
        GridQueryProcessor qryProc = ((IgniteEx)ign).context().query();

        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setSchema("PUBLIC").setArgs(params.toArray());

        return qryProc.querySqlFields(qry, true).getAll();
    }
}
