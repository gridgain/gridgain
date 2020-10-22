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

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.cache.IgniteScanQueryBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark for composite PK.
 */
public class IgniteCompositePkIndexBenchmark extends IgniteAbstractBenchmark {
    /**
     * Key class and query to create TEST table.
     */
    private static final Map<Class<?>, String> setupQrys = new HashMap<>();

    static {
        setupQrys.put(
            TestKey2Integers.class,
            "CREATE TABLE TEST (ID0 INT, ID1 INT, VALINT INT, VALSTR VARCHAR, " +
                "PRIMARY KEY (ID0, ID1)) " +
                "WITH \"CACHE_NAME=TEST,KEY_TYPE=" + TestKey2Integers.class.getName() + ",VALUE_TYPE=" + Value.class.getName() + "\""
        );
        setupQrys.put(
            TestKey8Integers.class,

            "CREATE TABLE TEST (ID0 INT, ID1 INT, ID2 INT, ID3 INT, ID4 INT, ID5 INT, ID6 INT, ID7 INT, VALINT INT, VALSTR VARCHAR, " +
                "PRIMARY KEY (ID0, ID1, ID2, ID3, ID4, ID5, ID6, ID7)) " +
                "WITH \"CACHE_NAME=TEST,KEY_TYPE=" + TestKey8Integers.class.getName() + ",VALUE_TYPE=" + Value.class.getName() + "\""
        );
        setupQrys.put(
            TestKeyHugeStringAndInteger.class,
            "CREATE TABLE TEST (ID0 VARCHAR, ID1 INT, VALINT INT, VALSTR VARCHAR, " +
                "PRIMARY KEY (ID0, ID1)) " +
                "WITH \"CACHE_NAME=TEST,KEY_TYPE=" + TestKeyHugeStringAndInteger.class.getName() + ",VALUE_TYPE=" + Value.class.getName() + "\""
        );
    }

    /** Cache name. */
    private static final String cacheName = "TEST";


    /** How many entries should be preloaded and within which range. */
    private int range;

    /** Key class. */
    private Class<?> keyCls;

    /** Value class. */
    private boolean addIndexes;

    /** */
    private Function<Integer, Object> keyCreator;

    /** */
    private TestAction testAct;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        range = args.range();
        String keyClsName = args.getStringParameter("keyClass", TestKey2Integers.class.getSimpleName());

        keyCls = Class.forName(IgniteCompositePkIndexBenchmark.class.getName() + "$" + keyClsName);
        final Constructor<?> keyConstructor = keyCls.getConstructor(int.class);

        keyCreator = (key) -> {
            try {
                return keyConstructor.newInstance(key);
            }
            catch (Exception e) {
                throw new IgniteException("Unexpected exception", e);
            }
        };

        addIndexes = args.getBooleanParameter("addIndexes", false);

        testAct = TestAction.valueOf(args.getStringParameter("action", "PUT").toUpperCase());

        printParameters();

        IgniteSemaphore sem = ignite().semaphore("setup", 1, true, true);

        try {
            if (sem.tryAcquire()) {
                println(cfg, "Create tables...");

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
    }

    /**
     *
     */
    private void init() {
        String createTblQry = setupQrys.get(keyCls);

        assert createTblQry != null : "Invalid key class: " + keyCls;

        sql(createTblQry);

        sql("CREATE INDEX IDX_ID1 ON TEST(ID1)");

        if (addIndexes) {
            sql("CREATE INDEX IDX_VAL_INT ON TEST(VALINT)");
            sql("CREATE INDEX IDX_VAL_STR ON TEST(VALSTR)");
        }

        println(cfg, "Populate cache, range: " + range);

        try (IgniteDataStreamer<Object, Object> stream = ignite().dataStreamer(cacheName)) {
            stream.allowOverwrite(false);

            for (int k = 0; k < range; ++k)
                stream.addData(keyCreator.apply(k), new Value(k));
        }

        println(cfg, "Cache populated. ");
        List<?> row = sql("SELECT * FROM TEST LIMIT 1").getAll().get(0);

        println("    partitions: " + ((IgniteEx)ignite()).cachex("TEST").affinity().partitions());
        println(cfg, "TEST table row: \n" + row);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        switch (testAct) {
            case PUT: {
                int k = ThreadLocalRandom.current().nextInt(range);
                int v = ThreadLocalRandom.current().nextInt(range);

                ignite().cache(cacheName).put(keyCreator.apply(k), new Value(v));

                return true;
            }

            case SCAN: {
                int k = ThreadLocalRandom.current().nextInt(range);

                List<List<?>> res = sql("SELECT ID1 FROM TEST WHERE VALINT=?", k).getAll();

                assert res.size() == 1;

                return true;
            }

            case CACHE_SCAN: {
                int k = ThreadLocalRandom.current().nextInt(range);

                ScanQuery<BinaryObject, BinaryObject> qry = new ScanQuery<>();

                qry.setFilter(new Filter(k));

                IgniteCache<BinaryObject, BinaryObject> cache = ignite().cache(cacheName).withKeepBinary();

                List<IgniteCache.Entry<BinaryObject, BinaryObject>> res = cache.query(qry).getAll();

                if (res.size() != 1)
                    throw new Exception("Invalid result size: " + res.size());

                if ((int)res.get(0).getValue().field("valInt") != k)
                    throw new Exception("Invalid entry found [key=" + k + ", entryKey=" + res.get(0).getKey() + ']');

                return true;
            }

            default:
                assert false : "Invalid action: " + testAct;

                return false;
        }
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
    /** */
    private void printParameters() {
        println("Benchmark parameter:");
        println("    range: " + range);
        println("    key: " + keyCls.getSimpleName());
        println("    idxs: " + addIndexes);
        println("    action: " + testAct);
    }

    /** */
    public static class TestKey2Integers {
        /** */
        @QuerySqlField
        private final int id0;

        @QuerySqlField
        private final int id1;

        /** */
        public TestKey2Integers(int key) {
            this.id0 = key / 1000;
            this.id1 = key;
        }
    }

    /** */
    public static class TestKeyHugeStringAndInteger {
        /** Prefix. */
        private static final String PREFIX = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

        /** */
        @QuerySqlField
        private final String id0;

        @QuerySqlField
        private final int id1;

        /** */
        public TestKeyHugeStringAndInteger(int key) {
            this.id0 = PREFIX + key;
            this.id1 = key;
        }
    }

    /** */
    public static class TestKey8Integers {
        /** */
        @QuerySqlField
        private final int id0;

        @QuerySqlField
        private final int id1;

        @QuerySqlField
        private final int id2;

        @QuerySqlField
        private final int id3;

        @QuerySqlField
        private final int id4;

        @QuerySqlField
        private final int id5;

        @QuerySqlField
        private final int id6;

        @QuerySqlField
        private final int id7;

        /** */
        public TestKey8Integers(int key) {
            this.id0 = 0;
            this.id1 = key / 100_000;
            this.id2 = key / 10_000;
            this.id3 = key / 1_000;
            this.id4 = key / 1000;
            this.id5 = key / 100;
            this.id6 = key / 10;
            this.id7 = key;
        }
    }

    /** */
    public static class Value {
        /** */
        @QuerySqlField
        private final int valInt;

        @QuerySqlField
        private final String valStr;

        /** */
        public Value(int key) {
            this.valInt = key;
            this.valStr = "val_str" + key;
        }
    }

    /**
     *
     */
    static class Filter implements IgniteBiPredicate<BinaryObject, BinaryObject> {
        /** */
        private final int val;

        /**
         * @param val Value to find.
         */
        public Filter(Integer val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(BinaryObject key, BinaryObject val) {
            return this.val == (int)val.field("valInt");
        }
    }

    /** */
    public enum TestAction {
        /** */
        PUT,

        /** */
        SCAN,

        /** */
        CACHE_SCAN
    }
}
