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

import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.h2.result.LazyResult;
import org.h2.result.ResultInterface;
import org.junit.Test;

/**
 * Tests for local query execution in lazy mode.
 */
public class ChooseIndexTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int OBJ_CNT = 1_000;

    /** Keys count. */
    private static final int FIELDS_CNT = 20;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid();

        sql(grid(), "CREATE TABLE TEST (" +
            "    ID0 INT, " +
            "    ID1 INT, " +
            "    ID2 INT, " +
            "    VAL VARCHAR, " +
            "    PRIMARY KEY (ID1, ID2, ID0)" +
            ") WITH\"TEMPLATE=REPLICATED,CACHE_NAME=inst,KEY_TYPE=instKey\"");

        sql(grid(), "CREATE INDEX IDX_ID0_ID2 ON TEST (ID0, ID1, ID2, ID0, ID2, ID1)");
        sql(grid(), "CREATE INDEX IDX_ID2 ON TEST (ID1)");

        try (IgniteDataStreamer streamer = grid().dataStreamer("inst")) {
            for (int i = 0; i < OBJ_CNT; ++i) {
                for (int fld = 0; fld < FIELDS_CNT; ++fld) {
                    BinaryObjectBuilder bobKey = grid().binary().builder("instKey");
                    BinaryObjectBuilder bobVal = grid().binary().builder("instVal");

                    bobKey.setField("ID0", i);
                    bobKey.setField("ID1", 0);
                    bobKey.setField("ID2", fld);

                    bobVal.setField("VAL", "val_" + i + "_" + fld);

                    streamer.addData(bobKey.build(), "val_" + i + "_" + fld);
                }

                if (i % 1000 == 0)
                    System.out.println("Populate " + i + " of " + OBJ_CNT);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Test local query execution.
     */
    @Test
    public void test() {
        System.out.println("+++ PLAN: " + (String)sql(grid(),
            "EXPLAIN SELECT * FROM TEST WHERE ID0=0 AND ID1=0").getAll().get(0).get(0));
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object ... args) {
        return sql(grid(), sql, args);
    }

    /**
     * @param ign Node.
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(IgniteEx ign, String sql, Object ... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setArgs(args), false);
    }
}
