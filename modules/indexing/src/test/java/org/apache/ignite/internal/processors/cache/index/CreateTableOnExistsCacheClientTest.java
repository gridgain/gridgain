/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.index;

import java.util.List;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests table availability on client node when CREATE TABLE command is called on existing cache.
 */
public class CreateTableOnExistsCacheClientTest extends AbstractIndexingCommonTest {
    /** */
    private static final int ROWS = 10;

    /** */
    private final String CACHE_NAME = "TEST";

    /** */
    private final String TYPE_NAME = "TEST_TYPE";

    /** */
    private IgniteEx srv;

    /** */
    private IgniteEx cli;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        srv = startGrid("crd");
        cli = startClientGrid("cli");

        srv.cluster().state(ClusterState.ACTIVE);

        srv.getOrCreateCache(new CacheConfiguration<>(CACHE_NAME)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        G.stopAll(false);

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setClusterStateOnStart(ClusterState.INACTIVE)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setMaxSize(128 * 1024 * 1024)))
            .setSqlConfiguration(new SqlConfiguration().setSqlSchemas("TEST"));
    }

    /** */
    @Test
    public void test() throws Exception {
        load(0, ROWS);

        createTable();

        GridTestUtils.waitForCondition(() -> {
                    try {
                        check(cli);
                        return true;
                    }
                    catch (IgniteSQLException ex) {
                        if (ex.getMessage().contains("Failed to parse query. Table \"TEST\" not found"))
                            return false;
                        else
                            throw ex;
                    }
                },
                10_000L);

        check(cli);
    }

    /** */
    private void createTable() {
        srv.context().query().querySqlFields(new SqlFieldsQuery(String.format("CREATE TABLE TEST.TEST " +
            "(ID INT, VAL0 int, VAL1 VARCHAR," +
            " PRIMARY KEY (ID)" +
            ") WITH " +
            " \"CACHE_NAME=%s,VALUE_TYPE=%s\"", CACHE_NAME, TYPE_NAME)), false);
    }

    /** */
    private void load(int beg, int end) {
        IgniteCache<Object, Object> c = srv.cache(CACHE_NAME);

        for (int i = beg; i < end; ++i) {
            BinaryObjectBuilder bob = srv.binary().builder(TYPE_NAME);
            bob.setField("VAL0", i);
            bob.setField("VAL1", "val" + i);

            c.put(i, bob.build());
        }
    }

    /** */
    private void check(IgniteEx ign) {
        List<List<?>> res = ign.context().query().querySqlFields(
            new SqlFieldsQuery("SELECT * FROM TEST.TEST"),
            true
        ).getAll();

        assertEquals(ROWS, res.size());
    }
}
