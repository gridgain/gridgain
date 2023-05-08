/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.sqltests;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Check that illegal joins of partitioned tables are warned.
 *
 * This top-level class is a wrapper to keep regular and parametrized tests in single place
 */
@RunWith(Enclosed.class)
public class CheckWarnJoinPartitionedTablesTest extends GridCommonAbstractTest {

    static final String FOO_TABLE = "\"foo\".Foo f";
    static final String BAR_TABLE = "\"bar\".Bar b";

    /** Utility class with basic stuff. */
    @Ignore
    static class BaseTest extends GridCommonAbstractTest {

        /** */
        private final ListeningTestLogger testLog = new ListeningTestLogger(log);

        /** */
        private IgniteEx crd;

        /** {@inheritDoc} */
        @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
            IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

            cfg.setGridLogger(testLog);

            return cfg;
        }

        /** {@inheritDoc} */
        @Override protected void beforeTest() throws Exception {
            crd = startGrid();
        }

        /** {@inheritDoc} */
        @Override protected void afterTest() throws Exception {
            stopAllGrids();
        }

        void createCachesWithPlainKey() {
            createCachesWithKeyType(PlainKey.class, () -> new PlainKey(0));
        }

        void createCachesWithAffinityKey() {
            createCachesWithKeyType(AffinityKey.class, () -> new AffinityKey(0));
        }

        <K> void createCachesWithKeyType(Class<K> keyCls, Supplier<K> keyProducer) {
            CacheConfiguration<K, Foo> fooCfg = new CacheConfiguration<K, Foo>("foo")
                .setIndexedTypes(keyCls, Foo.class);
            IgniteCache<K, Foo> fooCache = crd.getOrCreateCache(fooCfg);
            fooCache.put(keyProducer.get(), new Foo());

            CacheConfiguration<K, Bar> barCfg = new CacheConfiguration<K, Bar>("bar")
                .setIndexedTypes(keyCls, Bar.class);
            IgniteCache<K, Bar> barCache = crd.getOrCreateCache(barCfg);
            barCache.put(keyProducer.get(), new Bar());
        }

        /** Verify that expected message appeared in logs and vice versa */
        void checkLogListener(boolean shouldFindMsg, String sql, Object... args) {
            LogListener lsnr = LogListener.matches(
                "For join two partitioned tables join condition should contain the equality operation of affinity keys"
            ).build();

            testLog.registerListener(lsnr);

            execute(new SqlFieldsQuery(sql).setArgs(args));

            if (shouldFindMsg)
                assertTrue("Missing log message: " + sql, lsnr.check());
            else
                assertFalse("Unexpected log message: " + sql, lsnr.check());

            testLog.clearListeners();
        }

        /**
         * Execute query from node.
         *
         * @param qry query.
         */
        protected final void execute(SqlFieldsQuery qry) {
            FieldsQueryCursor<List<?>> cursor = crd.context().query().querySqlFields(qry, false);

            cursor.getAll();
        }
    }

    /** Non-parametrized test. */
    public static class SingleTest extends BaseTest {

        /** */
        @Test
        // This test should not pass as it contains wrong query. But current check pass it.
        public void testWrongQueryButAllAffinityKeysAreUsed() {
            execute(new SqlFieldsQuery(
                "CREATE TABLE A (ID INT, TITLE VARCHAR, PRICE INT, COMMENT VARCHAR, PRIMARY KEY (ID, TITLE, PRICE));"));

            // PRICE = ID
            checkLogListener(false,
                "SELECT a1.* FROM A a1 LEFT JOIN A a2 on a1.PRICE = a2.ID and a1.TITLE = a2.TITLE and a1.ID = a2.PRICE;");
        }

        /** Should not print warning since both tables have exactly same PKs and affinity keys and use them for join */
        @Test
        public void implicitInnerJoinOnIndexedTypesWithoutExplicitAffinityField() {
            createCachesWithPlainKey();

            checkJoinIndexedTypes();
        }

        /** Should not print warning since both tables have exactly same PKs and affinity keys and use them for join */
        @Test
        public void implicitInnerJoinOnIndexedTypesWithExplicitAffinityField() {
            createCachesWithAffinityKey();

            checkJoinIndexedTypes();
        }

        public void checkJoinIndexedTypes() {
            checkLogListener(false,
                "SELECT * FROM " + FOO_TABLE + ", " + BAR_TABLE + " WHERE f.id = b.id");

            checkLogListener(false,
                "SELECT * FROM " + FOO_TABLE + ", " + BAR_TABLE + " WHERE f.id = b.id and f.deposit = b.amount");
        }
    }

    /** Test that exploits join type parametrization. */
    @RunWith(Parameterized.class)
    public static class ParametrizedJoinTypesTest extends BaseTest {

        /** */
        @Parameterized.Parameter
        public String joinType;

        /** */
        @Parameterized.Parameters(name = "join={0}")
        public static List<Object> params() {
            return Arrays.asList("LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "JOIN");
        }

        /** */
        @Test
        public void joinSameTableWithPrimaryKey() {
            execute(new SqlFieldsQuery(
                "CREATE TABLE A (ID INT PRIMARY KEY, TITLE VARCHAR);"));

            checkSameTableWithJoinType();
        }

        /** */
        @Test
        public void joinSameTableWithPrimaryAffinityKey() {
            execute(new SqlFieldsQuery(
                "CREATE TABLE A (ID INT PRIMARY KEY, TITLE VARCHAR) with \"AFFINITY_KEY=ID\";"));

            checkSameTableWithJoinType();
        }

        /** */
        private void checkSameTableWithJoinType() {
            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a2.ID = a1.ID;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2._KEY;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1._KEY = a2.ID;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1._KEY = a2._KEY;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 where a1.ID = a2.ID;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 where a1.ID = a2._KEY;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID AND a1.TITLE = a2.TITLE;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID AND a1.TITLE != a2.TITLE;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID AND (a1.TITLE = a2.TITLE OR a1.ID = a2.ID);");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on (a1.TITLE = a2.TITLE OR a1.ID = a2.ID) AND a1.ID = a2.ID;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " (select a2.ID from A a2) a3 on a1.ID = a3.ID;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " (select ID from A) a2 where a1.ID = a2.ID;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 where a1.ID in (select a2.ID from A a2 " + joinType + " A a3 on a2.ID = a3.ID);");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " (select a2.ID from A a2 " + joinType + " A a3 on a2.ID = a3.ID) t" +
                    " on a1.ID = t.ID;");

            // Ignite doesn't support this.
            if (!joinType.contains("RIGHT"))
                checkLogListener(false,
                    "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID " + joinType + " A a3 on a1.ID = a3.ID;");

            // Some simple illegal joins.

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on 1 = 1;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID > a2.ID;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID != a2.ID;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.TITLE;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1._KEY = a2.TITLE;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.TITLE = a2.TITLE;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID OR a1.TITLE = a2.TITLE;");

            // Actually this is correct query. But this AST is too complex to analyze, and also it could be simplified.
            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID OR (a1.ID = a2.ID AND a1.TITLE = a2.TITLE);");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " (select ID from A) a2 on a1.ID > a2.ID;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " (select ID from A) a2 where a1.ID > a2.ID;");

            if (!joinType.contains("RIGHT"))
                checkLogListener(true,
                    "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.TITLE " + joinType + " A a3 on a1.ID = a3.ID;");

            // This query is invalid. But we don't have info about partitioning in nested joins.
            if (!joinType.contains("RIGHT")) {
                // For inner join we can check it because join conditions are parsed to WHERE clause.
                boolean shouldWarn = !joinType.contains("LEFT");

                checkLogListener(shouldWarn,
                    "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID " + joinType + " A a3 on a1.ID = a3.TITLE;");
            }

            checkLogListener(true,
                "SELECT a1.* FROM A a1 where a1.ID in (select a2.ID from A a2 " + joinType + " A a3 on a2.ID = a3.TITLE);");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 where a1.ID in (select a2.ID from A a2 " + joinType + " A a3 on a2.ID != a3.ID);");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " (select a2.ID from A a2 " + joinType + " A a3 on a2.ID = a3.TITLE) t" +
                    " on a1.ID = t.ID;");
        }

        /** */
        @Test
        public void joinSameTableWithComplexPrimaryKey() {
            execute(new SqlFieldsQuery(
                "CREATE TABLE A (ID INT, TITLE VARCHAR, PRICE INT, COMMENT VARCHAR, PRIMARY KEY (ID, TITLE, PRICE));"));

            checkSameTableWithComplexPrimaryKeyWithJoinType();
        }

        /** */
        private void checkSameTableWithComplexPrimaryKeyWithJoinType() {
            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1._KEY = a2._KEY;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 where a1._KEY = a2._KEY;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a2._KEY = a1._KEY;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID and a1.TITLE = a2.TITLE and a1.PRICE = a2.PRICE;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 where a1.ID = a2.ID and a1.TITLE = a2.TITLE and a1.PRICE = a2.PRICE;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on " +
                    "a1.ID = a2.ID and a1.TITLE = a2.TITLE and a1.PRICE = a2.PRICE and a1.COMMENT = a2.COMMENT;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on " +
                    "a1.ID = a2.ID and a1.TITLE = a2.TITLE and a1.PRICE = a2.PRICE and a1.COMMENT != a2.COMMENT;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.PRICE = a2.PRICE and a1.TITLE = a2.TITLE and a1.ID = a2.ID;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 where a1.PRICE = a2.PRICE and a1.TITLE = a2.TITLE and a1.ID = a2.ID;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID where a1.PRICE = a2.PRICE and a1.TITLE = a2.TITLE;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID where a1.PRICE = a2.PRICE and a1.TITLE = a2.TITLE;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 where a1.ID in (" +
                    "   select a2.ID from A a2 " + joinType + " A a3 on a2.ID = a3.ID and a2.TITLE = a3.TITLE and a2.PRICE = a3.PRICE);");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 where a1.ID in (" +
                    "   select a2.ID from A a2 " + joinType + " A a3 where a2.ID = a3.ID and a2.TITLE = a3.TITLE and a2.PRICE = a3.PRICE);");

            if (!joinType.contains("RIGHT"))
                checkLogListener(false,
                    "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1._KEY = a2._KEY " + joinType + " A a3 on a1._KEY = a3._KEY;");

            // Some simple illegal joins.

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a2._KEY != a1._KEY;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 where a2._KEY != a1._KEY;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID != a2.ID and a1.TITLE = a2.TITLE and a1.PRICE = a2.PRICE;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 where a1.ID != a2.ID and a1.TITLE = a2.TITLE and a1.PRICE = a2.PRICE;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID != a2.ID and a1.TITLE != a2.TITLE and a1.PRICE != a2.PRICE;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 where a1.ID != a2.ID and a1.TITLE != a2.TITLE and a1.PRICE != a2.PRICE;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID and a1.PRICE = a2.PRICE;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 where a1.ID = a2.ID and a1.PRICE = a2.PRICE;");

            if (!joinType.contains("RIGHT")) {
                // For inner join we can check it because join conditions are parsed to WHERE clause.
                boolean shouldWarn = !joinType.contains("LEFT");

                checkLogListener(false,
                    "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1._KEY = a2._KEY " + joinType + " A a3 on a1._KEY != a3._KEY;");

                checkLogListener(!shouldWarn,
                    "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1._KEY != a2._KEY " + joinType + " A a3 on a1._KEY = a3._KEY;");
            }
        }

        /** */
        @Test
        public void joinSameTableWithComplexPrimaryKeySingleAffKey() {
            execute(new SqlFieldsQuery(
                "CREATE TABLE A (ID INT, TITLE VARCHAR, PRICE INT, COMMENT VARCHAR, PRIMARY KEY (ID, TITLE, PRICE))" +
                    " with \"AFFINITY_KEY=ID\";"));

            checkSameTableWithComplexPrimaryKeySingleAffKeyWithJoinType();
        }

        /** */
        private void checkSameTableWithComplexPrimaryKeySingleAffKeyWithJoinType() {
            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 where a1.ID = a2.ID;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID and a1.TITLE != a2.TITLE;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 where a1.ID = a2.ID and a1.TITLE != a2.TITLE;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.TITLE != a2.TITLE and a1.ID = a2.ID;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 where a1.TITLE != a2.TITLE and a1.ID = a2.ID;");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 where a1.ID in (select a2.ID from A a2 " + joinType + " A a3 on a2.ID = a3.ID);");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " (select a2.ID from A a2 " + joinType + " A a3 on a2.ID = a3.ID) t" +
                    " on a1.ID = t.ID;");

            if (!joinType.contains("RIGHT"))
                checkLogListener(false,
                    "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID " + joinType + " A a3 on a1.ID = a3.ID;");

            // Some simple illegal joins.

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1._KEY = a2._KEY;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 where a1._KEY = a2._KEY;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID != a2.ID;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 where a1.ID != a2.ID;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID != a2.ID and a1.TITLE = a2.TITLE and a1.PRICE = a2.PRICE;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 where a1.ID != a2.ID and a1.TITLE = a2.TITLE and a1.PRICE = a2.PRICE;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.TITLE = a2.TITLE;");

            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 where a1.TITLE = a2.TITLE;");

            if (!joinType.contains("RIGHT")) {
                // For inner join we can check it because join conditions are parsed to WHERE clause.
                boolean shouldWarn = !joinType.contains("LEFT");

                checkLogListener(false,
                    "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID " + joinType + " A a3 on a1.ID != a3.ID;");

                checkLogListener(!shouldWarn,
                    "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID != a2.ID " + joinType + " A a3 on a1.ID = a3.ID;");
            }
        }

        /** */
        @Test
        public void joinWithPrimaryKey() {
            execute(new SqlFieldsQuery(
                "CREATE TABLE A (ID INT PRIMARY KEY, TITLE VARCHAR);"));

            execute(new SqlFieldsQuery(
                "CREATE TABLE B (ID INT PRIMARY KEY, PRICE INT);"));

            checkJoinPrimaryKeyWithJoinType();
        }

        /** */
        private void checkJoinPrimaryKeyWithJoinType() {
            checkLogListener(false,
                "SELECT a.* FROM A a " + joinType + " B b on a.ID = b.ID;");

            checkLogListener(false,
                "SELECT a.* FROM A a " + joinType + " B b on b.ID = a.ID;");

            checkLogListener(false,
                "SELECT a.* FROM A a " + joinType + " B b on b._KEY = a._KEY;");

            checkLogListener(false,
                "SELECT a.* FROM A a " + joinType + " B b on b.ID = a.ID and a.TITLE != 'Title';");

            checkLogListener(false,
                "SELECT a.* FROM A a " + joinType + " B b on b.ID = a.ID and b.PRICE > 100;");

            checkLogListener(false,
                "SELECT a.* FROM A a " + joinType + " B b on b.ID = a.ID and b.PRICE != a.ID;");

            // Some wrong queries.

            checkLogListener(true,
                "SELECT a.* FROM A a " + joinType + " B b on 1 = 1;");

            checkLogListener(true,
                "SELECT a.* FROM A a " + joinType + " B b where 1 = 1;");

            checkLogListener(true,
                "SELECT a.* FROM A a " + joinType + " B b on a.ID != b.ID;");

            checkLogListener(true,
                "SELECT a.* FROM A a " + joinType + " B b where a.ID != b.ID;");

            checkLogListener(true,
                "SELECT a.* FROM A a " + joinType + " B b on b.ID = 1;");

            checkLogListener(true,
                "SELECT a.* FROM A a " + joinType + " B b where b.ID = 1;");

            checkLogListener(true,
                "SELECT a.* FROM A a " + joinType + " B b on a.ID = b.PRICE;");

            checkLogListener(true,
                "SELECT a.* FROM A a " + joinType + " B b where a.ID = b.PRICE;");
        }

        /** */
        @Test
        public void joinPrimaryKeyAndAffinityKey() {
            execute(new SqlFieldsQuery(
                "CREATE TABLE A (k1 VARCHAR PRIMARY KEY, v2 VARCHAR);"));

            execute(new SqlFieldsQuery(
                "CREATE TABLE B (k1 VARCHAR, ak2 VARCHAR, v3 VARCHAR, PRIMARY KEY(k1, ak2)) with \"AFFINITY_KEY=ak2\";"));

            // Correct joins.
            checkLogListener(false,
                "SELECT * FROM A a " + joinType + " B b on a.k1 = b.ak2");

            checkLogListener(false,
                "SELECT * FROM A a " + joinType + " B b on a.k1 = b.ak2 WHERE b.k1 = ?", "1");

            checkLogListener(false,
                "SELECT * FROM A a " + joinType + " B b where a.k1 = b.ak2");

            checkLogListener(false,
                "SELECT * FROM A a " + joinType + " B b where b.ak2 = a.k1");

            checkLogListener(false,
                "SELECT * FROM A a " + joinType + " B b on a.k1 = b.ak2 WHERE a.k1 < b.ak2");

            // Wrong joins.

            checkLogListener(true,
                "SELECT * FROM A a " + joinType + " B b on a.k1 = b.k1");

            checkLogListener(true,
                "SELECT * FROM A a " + joinType + " B b on a.k1 = b.k1 and a.v2 = b.ak2");

            checkLogListener(true,
                "SELECT * FROM A a " + joinType + " B b on a.k1 > b.ak2");

            checkLogListener(true,
                "SELECT * FROM A a " + joinType + " B b where a.k1 = ? and b.ak2 = ?", "1", "1");
        }

        /** Should not print warning since both tables have exactly same PKs and affinity keys and use them for join */
        @Test
        public void joinOnIndexedTypesWithoutExplicitAffinityField() {
            createCachesWithPlainKey();

            checkJoinIndexedTypes();
        }

        /** Should not print warning since both tables have exactly same PKs and affinity keys and use them for join */
        @Test
        public void joinOnIndexedTypesWithExplicitAffinityField() {
            createCachesWithAffinityKey();

            checkJoinIndexedTypes();
        }

        /** */
        private void checkJoinIndexedTypes() {
            checkLogListener(false,
                "SELECT * FROM " + FOO_TABLE + " " + joinType + " " + BAR_TABLE + " on f.id = b.id");

            checkLogListener(false,
                "SELECT * FROM " + FOO_TABLE + " " + joinType + " " + BAR_TABLE + " on f.id = b.id WHERE f.deposit = b.amount");
        }
    }

    /** Key without explicit affinity declaration. */
    static class PlainKey {

        @QuerySqlField(index = true)
        private final long id;

        public PlainKey(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }
    }

    /** Key without explicit affinity declaration. */
    static class AffinityKey {

        @AffinityKeyMapped
        @QuerySqlField(index = true)
        private final long id;

        public AffinityKey(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

    }

    static class Foo {

        @QuerySqlField(index = true)
        private String name;

        @QuerySqlField(index = true)
        private long deposit;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getDeposit() {
            return deposit;
        }

        public void setDeposit(long deposit) {
            this.deposit = deposit;
        }
    }

    static class Bar {

        @QuerySqlField(index = true)
        private String name;

        @QuerySqlField(index = true)
        private long amount;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getAmount() {
            return amount;
        }

        public void setAmount(long amount) {
            this.amount = amount;
        }
    }

}
