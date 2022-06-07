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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests basic functionality of enabling indexing.
 */
public class CreateTableOnExistsCacheClientTest extends AbstractIndexingCommonTest {
    private static final int ROWS = 10;
    private final String CACHE_NAME = "TEST";
    private final String TYPE_NAME = "TEST_TYPE";
    protected static final String ATTR_FILTERED = "FILTERED";

    private IgniteEx srv;
    private IgniteEx cli;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        srv = startGrid("crd");

        cli = clientWithCoordinator("cli", srv);

        srv.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<?, ?> ccfg = testCacheConfiguration(CACHE_NAME);

        srv.getOrCreateCache(ccfg);

        awaitPartitionMapExchange();
    }

    /** */
    private IgniteEx clientWithCoordinator(String name, IgniteEx ign) throws Exception {
        while (true) {
            IgniteEx cli = startClientGrid(name);

            if (((TcpDiscoveryNode)cli.localNode()).clientRouterNodeId().equals(ign.localNode().id()))
                return cli;
            else
                stopGrid(name);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        G.stopAll(false);

        super.afterTest();
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setClusterStateOnStart(ClusterState.INACTIVE)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setMaxSize(128 * 1024 * 1024)))
            .setSqlSchemas("TEST");
    }

    /** */
    protected CacheConfiguration<?, ?> testCacheConfiguration(
        String name
    ) {
        return new CacheConfiguration<>(name)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
    }

    /** */
    @Test
    public void test() throws Exception {
//        assertNotNull(cli.cache(CACHE_NAME));

        load(0, ROWS);

        createTable();

        check(srv);

        System.out.println("+++ CHECK CLI");
        cli.cache(CACHE_NAME);

        GridTestUtils.waitForCondition(() -> {
                    try {
                        check(cli);
                        return true;
                    }
                    catch (Throwable e) {
                        return false;
                    }
                },
                10_000L);

        check(cli);
    }

    /** */
    private void createTable() {
        System.out.println("+++ CREATE TABLE");
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

    /** */
    protected static class NodeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return node.attribute(ATTR_FILTERED) == null;
        }
    }
}
