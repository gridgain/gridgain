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

package org.apache.ignite.internal.processors.cache.index;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.After;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.h2.database.H2Tree.IGNITE_THROTTLE_INLINE_SIZE_CALCULATION;

/**
 * A set of tests for caches with decimal index.
 */
public class DecimalIndexTest extends AbstractIndexingCommonTest {
    /** Server listening logger. */
    private ListeningTestLogger testLog = new ListeningTestLogger(false, log);

    /**
     *
     */
    @After
    public void tearDown() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration igniteCfg = super.getConfiguration(igniteInstanceName);

        igniteCfg.setConsistentId(igniteInstanceName);
        igniteCfg.setGridLogger(testLog);

        return igniteCfg;
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = IGNITE_THROTTLE_INLINE_SIZE_CALCULATION, value = "1")
    public void smallInlineSIze() throws Exception {
        final String TBL_NAME = "DECIMAL_TABLE";
        final String ERROR_MSG = "Indexed columns of a row cannot be fully inlined into index";

        LogListener lsnr = LogListener.matches(ERROR_MSG).build();

        testLog.registerListener(lsnr);

        Ignite node = startGrid(0);

        sql(node, "create table " + TBL_NAME + " (key_pk int, key1 decimal, key2 decimal, key3 decimal, primary key(key_pk), val int)");
        sql(node, "create index pk_id on " + TBL_NAME + " (key1, key2, key3) inline_size 19");
        sql(node, "insert into " + TBL_NAME + "(key_pk, key1, key2, key3, val) values (1, 1.0, 2.0, 3.0, 1)");
        sql(node, "insert into " + TBL_NAME + "(key_pk, key1, key2, key3, val) values (2, 1.0, 2.0, 3.0, 1)");

        assertTrue("Unexpected full inline of the index", lsnr.check());
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = IGNITE_THROTTLE_INLINE_SIZE_CALCULATION, value = "1")
    public void correctInlineSIze() throws Exception {
        final String TBL_NAME = "DECIMAL_TABLE";
        final String ERROR_MSG = "Indexed columns of a row cannot be fully inlined into index";

        LogListener lsnr = LogListener.matches(ERROR_MSG).build();

        testLog.registerListener(lsnr);

        Ignite node = startGrid(0);

        sql(node, "create table " + TBL_NAME + " (key_pk int, key1 decimal, key2 decimal, key3 decimal, primary key(key_pk), val int)");
        sql(node, "create index pk_id on " + TBL_NAME + " (key1, key2, key3) inline_size 20");
        sql(node, "insert into " + TBL_NAME + "(key_pk, key1, key2, key3, val) values (1, 1.0, 2.0, 3.0, 1)");
        sql(node, "insert into " + TBL_NAME + "(key_pk, key1, key2, key3, val) values (2, 1.0, 2.0, 3.0, 1)");

        assertFalse("Full inline of the index didn't happen", lsnr.check());
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = IGNITE_THROTTLE_INLINE_SIZE_CALCULATION, value = "1")
    public void nullAndOutOfRangeValues() throws Exception {
        final String TBL_NAME = "DECIMAL_TABLE";
        final String ERROR_MSG = "Indexed columns of a row cannot be fully inlined into index";

        LogListener lsnr = LogListener.matches(ERROR_MSG).build();

        testLog.registerListener(lsnr);

        Ignite node = startGrid(0);

        sql(node, "create table " + TBL_NAME + " (key_pk int, key1 decimal, key2 decimal, primary key(key_pk), val int)");
        sql(node, "create index pk_id on " + TBL_NAME + " (key1, key2)");
        sql(node, "insert into " + TBL_NAME + "(key_pk, key1, key2, val) values (1, 1.0, 2.0, 10)");
        sql(node, "insert into " + TBL_NAME + "(key_pk, key1, key2, val) values (2, null, 2.0, 20)");
        sql(node, "insert into " + TBL_NAME + "(key_pk, key1, key2, val) values (3, 1.0, null, 30)");
        sql(node, "insert into " + TBL_NAME + "(key_pk, key1, key2, val) values (4, 3.40282347E+99, 2.0, 40)");
        sql(node, "insert into " + TBL_NAME + "(key_pk, key1, key2, val) values (5, 1.0, 3.40282347E+99, 50)");
        sql(node, "insert into " + TBL_NAME + "(key_pk, key1, key2, val) values (6, 1.0, 2.0, 60)");

        List<List<?>> res = sql(node, "select val from " + TBL_NAME + " where key1 = 1.0 and key2 > 2.0");
        assertEquals(1, res.size());
        assertEquals(1, res.get(0).size());
        assertEquals(50, res.get(0).get(0));
    }

    /**
     * @param ig Ignite.
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results.
     */
    private List<List<?>> sql(Ignite ig, String sql, Object... args) {
        return ((IgniteEx)ig).context().query().querySqlFields(
            new SqlFieldsQuery(sql).setArgs(args), true).getAll();
    }
}
