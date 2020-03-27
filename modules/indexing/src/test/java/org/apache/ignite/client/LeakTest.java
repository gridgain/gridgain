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

package org.apache.ignite.client;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.util.GridDebug;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 */
public class LeakTest {
    /** Keys. */
    int KEYS = 10000;

    /** Iterations. */
    int ITERS = 1_000;

    /** Per test timeout */
    @Rule
    public Timeout globalTimeout = new Timeout((int) GridTestUtils.DFLT_TEST_TIMEOUT, TimeUnit.MILLISECONDS);

    /**
     * Test queries in Ignite binary.
     */
    @Test
    public void testLeak() throws Exception {
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient cli = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))
        ) {
            sql(cli, "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL VARCHAR)");

            for (int i = 0; i < KEYS; ++i)
                sql(cli,"INSERT INTO TEST VALUES (?, ?)", i, "val_" + i);

            GridDebug.dumpHeap(String.format("mem%03d.hprof", 0), true);

            int iter = 1;

            while (true) {
                GridTestUtils.runMultiThreaded(() -> {
                        for (int i = 0; i < ITERS; ++i) {
                            int k = ThreadLocalRandom.current().nextInt(KEYS);
                            String val = "val_" + ThreadLocalRandom.current().nextInt(KEYS);

                            List<List<?>> res = sqlTest(cli, "UPDATE TEST SET val = ? WHERE ID = ?", val, k);

                            Assert.assertEquals(1L, res.get(0).get(0));

                            sqlTest(cli, "SELECT * FROM TEST WHERE ID < 2000");
                        }
                    },
                    10, "upd-pressure-thread");

                GridDebug.dumpHeap(String.format("mem%03d.hprof", iter++), true);

                System.out.println("Iter " + iter);
            }
        }
    }

    /**
     */
    private List<List<?>> sqlTest(IgniteClient cli, String sql, Object... params) {
        FieldsQueryCursor<List<?>> cur = cli.query(new SqlFieldsQuery(sql).setArgs(params));

        cur.iterator().hasNext();

        // Don't close cursor
         cur.close();

        // Returns fake update count
        return Collections.singletonList(Collections.singletonList(1L));
    }

    /**
     */
    private List<List<?>> sql(IgniteClient cli, String sql, Object... params) {
        return cli.query(new SqlFieldsQuery(sql).setArgs(params)).getAll();
    }
}
