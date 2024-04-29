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
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public abstract class AbstractAffinityColumnTest extends GridCommonAbstractTest {

    /** Whether node starts with persistence enabled. */
    protected boolean persistenceEnabled = true;

    protected int gridCnt;
    protected int backups;

    public AbstractAffinityColumnTest() {
        super();
        gridCnt = 1;
        backups = 1;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("rawtypes")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(persistenceEnabled)));

        //cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
        //    .setBackups(1));

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

        createTables();
    }

    protected void createTables() {
        ignite(0).getOrCreateCache(DEFAULT_CACHE_NAME);
        //defaultCache().query(new SqlFieldsQuery(createQry(BAR_TABLE, BAR_CACHE, backups)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(true);

        cleanPersistenceDir();
    }

    protected static final String FOO_TABLE = "foo_table";
    protected static final String FOO_CACHE = "foo_cache";

    protected static final String BAR_TABLE = "bar_table";
    protected static final String BAR_CACHE = "bar_cache";

    protected static final String ID_FIELD = "id";
    protected static final String GROUP_ID_FIELD = "groupId";
    protected static final String NAME_FIELD = "name";
    protected static final String FACTOR_FIELD = "factor";

    protected String getAffinityField() {
        return GROUP_ID_FIELD;
    }

    protected String createQry(String tableName, String tableCache, int backups) {
        return "CREATE TABLE " + tableName + "(\n" +
            "\t" + ID_FIELD + " BIGINT,\n" +
            "\t" + GROUP_ID_FIELD + " BIGINT,\n" +
            "\t" + NAME_FIELD + " VARCHAR,\n" +
            "\t" + FACTOR_FIELD + " BIGINT,\n" +
            "\tPRIMARY KEY (" + ID_FIELD + ", " + GROUP_ID_FIELD + ")\n" +
            ") WITH \"\n" +
            "\tcache_name=" + tableCache + ",\n" +
            "\tbackups=" + backups + ",\n" +
            "\tKEY_TYPE=" + getKeyType() + ",\n" +
            "\tVALUE_TYPE=" + getValType() + ",\n" +
            "\taffinity_key=" + getAffinityField() + ",\n" +
            "\ttemplate=partitioned, PK_INLINE_SIZE=100, AFFINITY_INDEX_INLINE_SIZE=100\n" +
            "\";";
    }

    protected static SqlFieldsQuery insertQry(String table, long id) {
        //return new SqlFieldsQuery("INSERT INTO " + table + " (id, name, groupId, factor) VALUES (" + id + ", '" + ("val-" + id) + "', " + id % 100 + ", " + id + ")")
        return new SqlFieldsQuery("INSERT INTO " + table + "(ID, GROuPID, name, factor) VALUES (?, ?, ?, ?);")
            .setArgs(id, id % 100, "name-" + id, id)
            ;
    }

    protected IgniteCache<Object, Object> defaultCache() {
        return ignite(0).cache(DEFAULT_CACHE_NAME);
    }

    protected IgniteCache<Object, Object> tableCache(String cache) {
        return ignite(0).cache(cache);
    }

    protected abstract String getKeyType();

    protected String getValType() {
        return Val.class.getName();
    }

    protected abstract Object genKey(long id);

    protected BinaryObject genBinaryKey(long id) {
        return ignite(0).binary().builder(getKeyType())
            .setField("id", id)
            .setField("groupID", id % 100)
            .build();
    }

    protected BinaryObject genBinaryVal(long id) {
        return ignite(0).binary().builder(getValType())
            .setField(NAME_FIELD, "val-" + id)
            .setField(FACTOR_FIELD, id % Val.FACTOR)
            .build();
    }

    // Data insertion

    protected void put(String cache, long id) {
        tableCache(cache).put(genKey(id), Val.from(id));
    }

    protected void putBinary(String cache, long id) {
        tableCache(cache).withKeepBinary().put(genBinaryKey(id), genBinaryVal(id));
    }

    protected void insert(String table, long id) {
        defaultCache().query(insertQry(table, id)).getAll();
    }

    protected void logAndAssertTable(String cache, int expectedCount) {
        AtomicInteger counter = new AtomicInteger(0);

        log("\n");
        tableCache(cache).withKeepBinary().query(new ScanQuery()).getAll().forEach(l -> {
            IgniteBiTuple<?, ?> tuple = (IgniteBiTuple<?, ?>)l;
            log(">>> " + tuple.get1() + ",  " + tuple.get2());
            counter.incrementAndGet();
        });

        assertEquals("Entries count doesn't match", expectedCount, counter.get());
        counter.set(0);

        log("\n");
        tableCache(cache).withKeepBinary().query(new SqlFieldsQuery("SELECT * FROM " + FOO_TABLE + " ORDER BY id")).getAll().forEach(l -> {
            log(">>> Row: " + l);
            counter.incrementAndGet();
        });

        assertEquals("Rows count doesn't match", expectedCount, counter.get());
    }

    // Shortcuts

    protected void insert(long id) {
        insert(FOO_TABLE, id);
    }

    protected void put(long id) {
        put(FOO_CACHE, id);
    }

    protected void putBinary(long id) {
        putBinary(FOO_CACHE, id);
    }

    protected void logAndAssertTable(int expectedCount) {
        logAndAssertTable(FOO_CACHE, expectedCount);
    }

    protected static void log(String msg) {
        System.out.println(msg);
    }

    protected static class Val {

        static final long FACTOR = 100;

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

}
