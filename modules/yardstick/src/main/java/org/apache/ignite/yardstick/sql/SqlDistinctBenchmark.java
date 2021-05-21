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

package org.apache.ignite.yardstick.sql;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs query operations with joins.
 */
public class SqlDistinctBenchmark extends IgniteAbstractBenchmark {
    /** */
    private final String SQL_TMPL = "SELECT DISTINCT VAL_%s FROM TBL WHERE ID >=? AND ID < ?";

    /** Sql query for benchmark. */
    private String sql;

    /** */
    private long firstParamRange;

    /** Range. */
    private long range;

    /** Initialize step. */
    private boolean initStep;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        String typeName = args.getStringParameter("type", "LONG");

        sql = String.format(SQL_TMPL, typeName);

        range = args.getLongParameter("range", 0);
        initStep = args.getBooleanParameter("init", false);
        firstParamRange = args.range() - range;

        printParameters();

        IgniteSemaphore sem = ignite().semaphore("sql-setup", 1, true, true);

        try {
            if (sem.tryAcquire()) {
                println(cfg, "Create tables...");

                if (initStep)
                    init();
            }
            else {
                // Acquire (wait setup by other client) and immediately release/
                println(cfg, "Waits for setup...");

                sem.acquire();
            }
        }
        finally {
            sem.release();
        }

        printPlan();
    }

    /**
     *
     */
    private void init() {
        sql("CREATE TABLE TBL(" +
            "ID LONG PRIMARY KEY, " +
            "VAL_INT INT, " +
            "VAL_LONG LONG, " +
            "VAL_STR VARCHAR, " +
            "VAL_DECIMAL DECIMAL, " +
            "VAL_TIME TIME, " +
            "VAL_DATE TIME, " +
            "VAL_TIMESTAMP TIMESTAMP" +
            ") " +
            "WITH \"CACHE_NAME=TBL,VALUE_TYPE=DATA_VAL\"");

        println(cfg, "Populate TBL: " + args.range());

        fillCache("TBL", "DATA_VAL", (long)args.range(),
            (k, bob) -> {
                bob.setField("ID", k);
                bob.setField("VAL_INT", (int)(k / 10));
                bob.setField("VAL_LONG", k / 10);
                bob.setField("VAL_DECIMAL", new BigDecimal("" + (k / 10) + ".00"));
                bob.setField("VAL_STR","str_" + (k / 10));
                bob.setField("VAL_TIME", new Time(k / 10));
                bob.setField("VAL_DATE", new Date(k / 10));
                bob.setField("VAL_TIMESTAMP", new Timestamp(k / 10));
            });

        println(cfg, "Finished populating data");
    }

    /**
     * @param name Cache name.
     * @param typeName Type name.
     * @param range Key range.
     * @param fillDataConsumer Fill one object by key.
     */
    @SuppressWarnings("unchecked")
    private void fillCache(
        String name,
        String typeName,
        long range,
        BiConsumer<Long, BinaryObjectBuilder> fillDataConsumer) {
        println(cfg, "Populate cache: " + name + ", range: " + range);

        try (IgniteDataStreamer stream = ignite().dataStreamer(name)) {
            stream.allowOverwrite(false);

            for (long k = 0; k < range; ++k) {
                BinaryObjectBuilder bob = ignite().binary().builder(typeName);

                fillDataConsumer.accept(k, bob);

                stream.addData(k, bob.build());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        FieldsQueryCursor<List<?>> cur;

        long p0 = ThreadLocalRandom.current().nextLong(firstParamRange);

        cur = sql(sql, p0, p0 + range);

        Iterator it = cur.iterator();

        long cnt = 0;

        while (it.hasNext()) {
            it.next();
            ++cnt;
        }

        return true;
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return ((IgniteEx)ignite()).context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setSchema("PUBLIC")
            .setLazy(true)
            .setEnforceJoinOrder(true)
            .setArgs(args), false);
    }

    /**
     *
     */
    private void printParameters() {
        println("Benchmark parameter:");
        println("    SQL: " + sql);
        println("    init: " + initStep);
        println("    param range: " + range);
    }

    /**
     *
     */
    private void printPlan() {
        FieldsQueryCursor<List<?>> planCur;

        long p0 = ThreadLocalRandom.current().nextLong(firstParamRange);

        planCur = sql("EXPLAIN " + sql, p0, p0 + range);

        println("Plan: " + planCur.getAll().toString());
    }
}
