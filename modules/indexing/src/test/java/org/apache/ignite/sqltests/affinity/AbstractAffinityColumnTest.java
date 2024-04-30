/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.sqltests.affinity;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public abstract class AbstractAffinityColumnTest<T extends AbstractAffinityColumnTest.Table> extends GridCommonAbstractTest {

    /** Whether node starts with persistence enabled. */
    protected boolean persistenceEnabled = false;

    protected int gridCnt;
    protected int backups;

    protected T fooTable;
    protected T barTable;

    public AbstractAffinityColumnTest() {
        super();
        gridCnt = 1;
        backups = 1;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("rawtypes")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration()
            .setPort(11211));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(persistenceEnabled)));

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setBackups(1));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(gridCnt);

        ignite(0).active(true);

        initTables();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(true);

        cleanPersistenceDir();
    }

    protected abstract void initTables();

    protected IgniteCache<?, ?> defaultCache() {
        return ignite(0).cache(DEFAULT_CACHE_NAME);
    }

    protected static final String FOO_TABLE = "foo_table";
    protected static final String FOO_CACHE = "foo_cache";

    protected static final String BAR_TABLE = "bar_table";
    protected static final String BAR_CACHE = "bar_cache";

    protected static final String ID_FIELD = "userId";
    protected static final String GROUP_ID_FIELD = "groupId";
    protected static final String NAME_FIELD = "name";
    protected static final String FACTOR_FIELD = "factor";

    protected abstract String getKeyType();

    protected void insert(long id) {
        fooTable.insert(id);
    }

    protected void putBinary(long id) {
        fooTable.putBinary(id);
    }

    protected void logAndAssertTable(int expectedCount) {
        fooTable.logAndAssertTable(expectedCount);
    }

    protected static void log(String msg) {
        System.out.println(msg);
    }

    protected static long random(long ceiling) {
        return (long)(Math.random() * ceiling);
    }

    protected static class Val {

        static final long FACTOR = 1000;

        private String name;
        private long factor;

        public static Val from(long id) {
            Val instance = new Val();
            instance.setName("val-" + id);
            instance.setFactor(id % FACTOR);
            return instance;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getFactor() {
            return factor;
        }

        public void setFactor(long factor) {
            this.factor = factor;
        }

        @Override public String toString() {
            return "Val [" + "name='" + name + '\'' + ", factor=" + factor + ']';
        }
    }

    public static class Table {

        private final IgniteEx ignite;

        private final String tableName;
        private final String cacheName;

        private final String idField;
        private final String groupField;

        private final String keyType;
        private final String valType;

        private final FieldValueGenerator fvGenerator;

        public Table(IgniteEx ignite,
            String tableName,
            String cacheName,
            String idField,
            String groupField,
            String keyType,
            String valType,
            FieldValueGenerator fvGenerator
        ) {
            this.ignite = ignite;
            this.tableName = tableName;
            this.cacheName = cacheName;
            this.idField = idField;
            this.groupField = groupField;
            this.keyType = keyType;
            this.valType = valType;
            this.fvGenerator = fvGenerator;
        }

        public Table(IgniteEx ignite, String tableName, String cacheName, String keyType) {
            this(ignite, tableName, cacheName, ID_FIELD, GROUP_ID_FIELD, keyType, Val.class.getName(), FieldValueGenerator.DEFAULT);
        }

        //public Table(IgniteEx ignite, String tableName, String cacheName, String keyType) {
        //    this(ignite, tableName, cacheName, ID_FIELD, GROUP_ID_FIELD, keyType, Val.class.getName(), FieldValueGenerator.DEFAULT);
        //}

        public IgniteCache<Object, Object> getCache() {
            return ignite.cache(cacheName);
        }

        public Table create(int backups) {
            String qry = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n" +
                "\t" + idField + " BIGINT,\n" +
                "\t" + groupField + " BIGINT,\n" +
                "\t" + NAME_FIELD + " VARCHAR,\n" +
                "\t" + FACTOR_FIELD + " BIGINT,\n" +
                "\tPRIMARY KEY (" + idField + ", " + groupField + ")\n" +
                ") WITH \"\n" +
                "\tcache_name=" + cacheName + ",\n" +
                "\tbackups=" + backups + ",\n" +
                "\tkey_type=" + keyType + ",\n" +
                "\tvalue_type=" + valType + ",\n" +
                "\taffinity_key=" + groupField + ",\n" +
                "\ttemplate=partitioned, PK_INLINE_SIZE=100, AFFINITY_INDEX_INLINE_SIZE=100\n" +
                "\";";

            log(qry);

            ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(qry));

            return this;
        }

        public SqlFieldsQuery insertQry(long id) {
            return new SqlFieldsQuery("INSERT INTO " + tableName + " (" +
                idField + ", " +
                groupField + ", " +
                NAME_FIELD + ", " +
                FACTOR_FIELD + "" +
                ") VALUES (?, ?, ? ,?)"
            ).setArgs(genId(id), genGroupId(id), "name-" + id, id);
        }

        public void executeQuery(SqlFieldsQuery qry) {
            //log(qry.toString());
            getCache().query(qry).getAll();
        }

        public long genId(long id) {
            return fvGenerator.gen(ID_FIELD, id);
        }

        public long genGroupId(long id) {
            return fvGenerator.gen(GROUP_ID_FIELD, id);
        }

        public BinaryObject genBinaryVal(long id) {
            return ignite.binary().builder(valType)
                .setField(NAME_FIELD, "val-" + id)
                .setField(FACTOR_FIELD, id % Val.FACTOR)
                .build();
        }

        public BinaryObject genBinaryKey(long id) {
            // use direct order vanilla field naming by default
            return genBinaryKey(id, idField, groupField);
        }

        public BinaryObject genBinaryKey(long id, String firstField, String secondField) {
            return ignite.binary().builder(keyType)
                .setField(firstField, fvGenerator.gen(firstField, id))
                .setField(secondField, fvGenerator.gen(secondField, id))
                .build();
        }

        // Data manipulations

        public void putBinary(long id, String firstField, String secondField) {
            getCache().withKeepBinary().put(genBinaryKey(id, firstField, secondField), genBinaryVal(id));
        }

        public void putBinary(long id) {
            getCache().withKeepBinary().put(genBinaryKey(id, ID_FIELD, GROUP_ID_FIELD), genBinaryVal(id));
        }

        public void insert(long id) {
            executeQuery(insertQry(id));
        }

        public void logAndAssertTable(int expectedCount) {
            AtomicInteger counter = new AtomicInteger(0);

            log("\n");
            getCache().withKeepBinary().query(new ScanQuery<BinaryObject, BinaryObject>()).getAll().forEach(l -> {
                IgniteBiTuple<?, ?> tuple = (IgniteBiTuple<?, ?>)l;
                log(">>> " + tuple.get1() + ",  " + tuple.get2());
                counter.incrementAndGet();
            });

            assertEquals("Entries count doesn't match", expectedCount, counter.get());
            counter.set(0);

            log("");
            getCache().withKeepBinary().query(new SqlFieldsQuery("SELECT * FROM " + tableName + " ORDER BY " + idField)).getAll().forEach(l -> {
                log(">>> Row: " + l);
                counter.incrementAndGet();
            });

            assertEquals("Rows count doesn't match", expectedCount, counter.get());
        }

        @Override public String toString() {
            // For SQL queries
            return tableName + " " + tableName.substring(0, 3);
        }
    }

    @FunctionalInterface
    public interface FieldValueGenerator {

        FieldValueGenerator DEFAULT = (fieldName, id) -> ID_FIELD.equalsIgnoreCase(fieldName) ? id : id % 100;

        long gen(String field, long id);

    }

}
