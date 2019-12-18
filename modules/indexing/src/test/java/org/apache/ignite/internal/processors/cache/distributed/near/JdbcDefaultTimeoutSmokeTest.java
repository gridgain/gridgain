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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class JdbcDefaultTimeoutSmokeTest extends GridCommonAbstractTest {
    @QuerySqlFunction
    public static int longProcess(int i, long millis) {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return i;
    }

    @Override protected long getTestTimeout() {
        return 30_000;
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>("TEST")
            .setIndexedTypes(Integer.class, Integer.class)
            .setSqlFunctionClasses(JdbcDefaultTimeoutSmokeTest.class);

        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(ccfg)
            .setDefaultQueryTimeout(1_000);
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        IgniteEx ign = startGrid(0);

        ign.cache("TEST").putAll(IntStream.range(0, 1000).boxed().collect(Collectors.toMap(Function.identity(), Function.identity())));
    }

    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    @Test
    public void testThinJdbc() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://localhost:10800")) {
            Statement stmt = conn.createStatement();

            stmt.execute("select TEST.longProcess(_key, 5) from TEST.Integer");

            fail("Exception expected");
        }
        catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().toLowerCase().contains("cancel"));
        }
    }

    @Test
    public void testThinJdbcStatementTimeout() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://localhost:10800")) {
            Statement stmt = conn.createStatement();
            stmt.setQueryTimeout(0);

            stmt.execute("select TEST.longProcess(_key, 5) from TEST.Integer");

            // assert no exception
        }
    }

    @Test
    public void testThinJdbcConnectionPropertyTimeout() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://localhost:10800?queryTimeout=0")) {
            Statement stmt = conn.createStatement();

            stmt.execute("select TEST.longProcess(_key, 5) from TEST.Integer");

            // assert no exception
        }
    }

    @Test
    public void testThinJava() throws Exception {
        try (IgniteClient cli = G.startClient(new ClientConfiguration().setAddresses("localhost:10800"))) {
            SqlFieldsQuery qry = new SqlFieldsQuery("select TEST.longProcess(_key, 10) from TEST.Integer");

            cli.query(qry).getAll();

            fail("Exception expected");
        }
        catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().toLowerCase().contains("cancel"));
        }
    }
}
