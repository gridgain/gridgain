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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlInitialConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for {@link SqlInitialConfiguration#setDisabledSqlFunctions(String[])}.
 */
@RunWith(Parameterized.class)
public class DisabledSqlFunctionsTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEY_CNT = 10;

    /** Empty disabled functions. */
    public static final String[] EMPTY_DISABLED_FUNCTIONS = new String[0];

    /** Disabled SQL functions. */
    private static String[] disabledFuncs;

    /** Local mode . */
    @Parameterized.Parameter
    public boolean local;

    /** Executes query on client node. */
    @Parameterized.Parameter(1)
    public boolean client;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "local={0}, client={1}")
    public static Collection parameters() {
        Set<Object[]> paramsSet = new LinkedHashSet<>();

        for (int i = 0; i < 4; ++i) {
            Object[] params = new Object[2];

            params[0] = (i & 1) == 0;
            params[1] = (i & 2) == 0;

            paramsSet.add(params);
        }

        return paramsSet;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (disabledFuncs != null)
            cfg.setSqlInitialConfiguration(new SqlInitialConfiguration().setDisabledSqlFunctions(disabledFuncs));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     */
    private void init() throws Exception {
        startGrid("srv");
        startGrid("cli");

        IgniteCache<Long, Long> c = grid("srv").createCache(new CacheConfiguration<Long, Long>()
            .setName("test")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(Collections.singleton(new QueryEntity(Long.class, Long.class)
                .setTableName("test")
                .addQueryField("id", Long.class.getName(), null)
                .addQueryField("val", Long.class.getName(), null)
                .setKeyFieldName("id")
                .setValueFieldName("val")
            ))
            .setAffinity(new RendezvousAffinityFunction(false, 10)));

        for (long i = 0; i < KEY_CNT; ++i)
            c.put(i, i);

    }

    /**
     */
    @Test
    public void testDefaultSelect() throws Exception {
        disabledFuncs = null;

        init();

        checkSqlWithDisabledFunction("SELECT FILE_WRITE(0, 'test.dat')");
        checkSqlWithDisabledFunction("SELECT FILE_READ('test.dat')");
        checkSqlWithDisabledFunction("SELECT CSVWRITE('test.csv', 'select 1, 2')");
        checkSqlWithDisabledFunction("SELECT * FROM CSVREAD('test.csv')");
        checkSqlWithDisabledFunction("SELECT MEMORY_FREE()");
        checkSqlWithDisabledFunction("SELECT MEMORY_USED()");
        checkSqlWithDisabledFunction("SELECT LOCK_MODE()");
        checkSqlWithDisabledFunction("SELECT LINK_SCHEMA('TEST2', '', 'jdbc:h2:./test', 'sa', 'sa', 'PUBLIC')");
        checkSqlWithDisabledFunction("SELECT SESSION_ID()");
        checkSqlWithDisabledFunction("SELECT CANCEL_SESSION(1)");
    }

    /**
     */
    @Test
    public void testDefaultInsert() throws Exception {
        disabledFuncs = null;

        init();

        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, FILE_WRITE(0, 'test.dat')");
        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, FILE_READ('test.dat')");
        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, SELECT CSVWRITE('test.csv', 'select 1, 2')");
        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, count(*) FROM CSVREAD('test.csv')");
        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, MEMORY_FREE()");
        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, MEMORY_USED()");
        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, LOCK_MODE()");
        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, LINK_SCHEMA('TEST2', '', 'jdbc:h2:./test', 'sa', 'sa', 'PUBLIC')");
        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, SESSION_ID()");
        checkSqlWithDisabledFunction("INSERT INTO TEST (ID, VAL) SELECT 1, CANCEL_SESSION(1)");
    }

    /**
     */
    @Test
    public void testDefaultUpdate() throws Exception {
        disabledFuncs = null;

        init();

        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = FILE_WRITE(0, 'test.dat')");
        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = LENGTH(FILE_READ('test.dat'))");
        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = CSVWRITE('test.csv', 'select 1, 2')");
        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = SELECT count(*) FROM CSVREAD('test.csv')");
        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = MEMORY_FREE()");
        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = MEMORY_USED()");
        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = LOCK_MODE()");
        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = LINK_SCHEMA('TEST2', '', 'jdbc:h2:./test', 'sa', 'sa', 'PUBLIC')");
        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = SESSION_ID()");
        checkSqlWithDisabledFunction("UPDATE TEST SET VAL = CANCEL_SESSION(1)");
    }

    /**
     */
    @Test
    public void testDefaultDelete() throws Exception {
        disabledFuncs = null;

        init();

        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = FILE_WRITE(0, 'test.dat')");
        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = LENGTH(FILE_READ('test.dat'))");
        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = CSVWRITE('test.csv', 'select 1, 2')");
        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = SELECT count(*) FROM CSVREAD('test.csv')");
        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = MEMORY_FREE()");
        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = MEMORY_USED()");
        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = LOCK_MODE()");
        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = LINK_SCHEMA('TEST2', '', 'jdbc:h2:./test', 'sa', 'sa', 'PUBLIC')");
        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = SESSION_ID()");
        checkSqlWithDisabledFunction("DELETE FROM TEST WHERE VAL = CANCEL_SESSION(1)");
    }

    /**
     * Test local query execution.
     */
    @Test
    public void testAllowFunctionsDisabledByDefault() throws Exception {
        disabledFuncs = EMPTY_DISABLED_FUNCTIONS;

        init();

        sql("SELECT FILE_WRITE(0, 'test.dat')").getAll();
        sql("SELECT FILE_READ('test.dat')").getAll();
        sql("SELECT CSVWRITE('test.csv', 'select 1, 2')").getAll();
        sql("SELECT * FROM CSVREAD('test.csv')").getAll();
        sql("SELECT MEMORY_FREE()").getAll();
        sql("SELECT MEMORY_USED()").getAll();
        sql("SELECT LOCK_MODE()").getAll();
        sql("SELECT LINK_SCHEMA('TEST2', '', 'jdbc:h2:./test', 'sa', 'sa', 'PUBLIC')").getAll();
        sql("SELECT SESSION_ID()").getAll();
        sql("SELECT CANCEL_SESSION(1)").getAll();
    }

    /**
     * Test local query execution.
     */
    @Test
    public void testCustomDisabledFunctionsSet_Length() throws Exception {
        disabledFuncs = new String[] {"LENGTH"};

        init();

        sql("SELECT FILE_WRITE(0, 'test.dat')").getAll();
        sql("SELECT FILE_READ('test.dat')").getAll();
        sql("SELECT CSVWRITE('test.csv', 'select 1, 2')").getAll();
        sql("SELECT * FROM CSVREAD('test.csv')").getAll();
        sql("SELECT MEMORY_FREE()").getAll();
        sql("SELECT MEMORY_USED()").getAll();
        sql("SELECT LOCK_MODE()").getAll();
        sql("SELECT LINK_SCHEMA('TEST2', '', 'jdbc:h2:./test', 'sa', 'sa', 'PUBLIC')").getAll();
        sql("SELECT SESSION_ID()").getAll();
        sql("SELECT CANCEL_SESSION(1)").getAll();

        checkSqlWithDisabledFunction("SELECT LENGTH(?)", "test");
    }

    /**
     * Test local query execution.
     */
    @Test
    public void testCustomDisabledFunctionsSet_FileRead() throws Exception {
        disabledFuncs = new String[] {"FILE_READ"};

        init();

        sql("SELECT FILE_WRITE(0, 'test.dat')").getAll();
        checkSqlWithDisabledFunction("SELECT FILE_READ('test.dat')");
        sql("SELECT CSVWRITE('test.csv', 'select 1, 2')").getAll();
        sql("SELECT * FROM CSVREAD('test.csv')").getAll();
        sql("SELECT MEMORY_FREE()").getAll();
        sql("SELECT MEMORY_USED()").getAll();
        sql("SELECT LOCK_MODE()").getAll();
        sql("SELECT LINK_SCHEMA('TEST2', '', 'jdbc:h2:./test', 'sa', 'sa', 'PUBLIC')").getAll();
        sql("SELECT SESSION_ID()").getAll();
        sql("SELECT CANCEL_SESSION(1)").getAll();
    }

    /**
     * Test local query execution.
     */
    @Test
    public void testNullSqlConfigurations() throws Exception {
        IgniteConfiguration cfg = getConfiguration("test");

        GridTestUtils.assertThrows(log, () -> cfg.setSqlInitialConfiguration(null),
            IllegalArgumentException.class, "Ouch! Argument is invalid: SQL initial configuration cannot be null");
    }

    /**
     */
    private void checkSqlWithDisabledFunction(final String sql, final Object ... args) {
        GridTestUtils.assertThrows(log, () -> sql(sql, args).getAll(), IgniteSQLException.class, "The function is disabled");
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object ... args) {
        IgniteEx ign = client ? grid("cli") :grid("srv");

        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setLocal(local)
            .setArgs(args), false);
    }
}
