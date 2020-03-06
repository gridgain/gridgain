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
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_INDEX_COST_FUNCTION;

/**
 * Tests for local query execution in lazy mode.
 */
public class ChooseIndexTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int OBJ_CNT = 1_000;

    /** Test logger. */
    private ListeningTestLogger testLog;

    /** Logger listener. */
    private LogListener logLsnr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(testLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        testLog = new ListeningTestLogger(false, log);

        logLsnr = LogListener
            .matches("Invalid cost function: INVALID_COST_FUNC")
            .build();

        testLog.registerListener(logLsnr);

        startGrid();

        sql(grid(), "CREATE TABLE TEST (" +
            "    ID INT PRIMARY KEY, " +
            "    V0 INT, " +
            "    V1 INT, " +
            "    V2 INT, " +
            "    V3 INT, " +
            "    VAL INT" +
            ") WITH\" TEMPLATE=REPLICATED,CACHE_NAME=inst,VALUE_TYPE=test_val\"");

        sql(grid(), "CREATE INDEX IDX_V1 ON TEST (V1)");
        sql(grid(), "CREATE INDEX IDX_V0_V1 ON TEST (V0, V1, V2, V3)");

        try (IgniteDataStreamer streamer = grid().dataStreamer("inst")) {
            for (int i = 0; i < OBJ_CNT; ++i) {
                BinaryObjectBuilder bobVal = grid().binary().builder("test_val");

                bobVal.setField("V0", i);
                bobVal.setField("V1", i);
                bobVal.setField("V2", i);
                bobVal.setField("V3", i);

                streamer.addData(i, bobVal.build());
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     */
    @Test
    public void testDefault() {
        String plan = (String)sql(grid(),
            "EXPLAIN SELECT * FROM TEST WHERE V0=0 AND V1=0").getAll().get(0).get(0);

        assertTrue("Invalid plan: " + plan, plan.contains("PUBLIC.IDX_V0_V1"));
    }

    /**
     */
    @WithSystemProperty(key = IGNITE_INDEX_COST_FUNCTION, value = "LAST")
    @Test
    public void testLast() {
        String plan = (String)sql(grid(),
            "EXPLAIN SELECT * FROM TEST WHERE V0=0 AND V1=0").getAll().get(0).get(0);

        assertTrue("Invalid plan: " + plan, plan.contains("PUBLIC.IDX_V0_V1"));
    }

    /**
     */
    @WithSystemProperty(key = IGNITE_INDEX_COST_FUNCTION, value = "COMPATIBLE_8_7_12")
    @Test
    public void testCompatible_8_7_12() {
        String plan = (String)sql(grid(),
            "EXPLAIN SELECT * FROM TEST WHERE V0=0 AND V1=0").getAll().get(0).get(0);

        assertTrue("Invalid plan: " + plan, plan.contains("PUBLIC.IDX_V1"));
    }

    /**
     */
    @WithSystemProperty(key = IGNITE_INDEX_COST_FUNCTION, value = "COMPATIBLE_8_7_6")
    @Test
    public void testCompatible_8_7_6() {
        String plan = (String)sql(grid(),
            "EXPLAIN SELECT * FROM TEST WHERE V0=0 AND V1=0").getAll().get(0).get(0);

        assertTrue("Invalid plan: " + plan, plan.contains("PUBLIC.IDX_V0_V1"));
    }

    /**
     */
    @WithSystemProperty(key = IGNITE_INDEX_COST_FUNCTION, value = "INVALID_COST_FUNC")
    @Test
    public void testInvalidCostFunctionName() {
        assertTrue(logLsnr.check());
    }

    /**
     * @param ign Node.
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(IgniteEx ign, String sql, Object... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setArgs(args), false);
    }
}
