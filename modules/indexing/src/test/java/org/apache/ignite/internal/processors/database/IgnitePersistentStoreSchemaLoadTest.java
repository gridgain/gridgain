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

package org.apache.ignite.internal.processors.database;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;

/**
 *
 */
@WithSystemProperty(key = IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK, value = "true")
public class IgnitePersistentStoreSchemaLoadTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String TMPL_NAME = "test_cache*";

    /** Table name. */
    private static final String TBL_NAME = Person.class.getSimpleName();

    /** Name of the cache created with {@code CREATE TABLE}. */
    private static final String SQL_CACHE_NAME = QueryUtils.createTableCacheName(QueryUtils.DFLT_SCHEMA, TBL_NAME);

    /** Name of the cache created upon cluster start. */
    private static final String STATIC_CACHE_NAME = TBL_NAME;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheCfg(TMPL_NAME));

        DataStorageConfiguration pCfg = new DataStorageConfiguration();

        pCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(100L * 1024 * 1024));

        pCfg.setCheckpointFrequency(1000);

        cfg.setDataStorageConfiguration(pCfg);

        return cfg;
    }

    /**
     * Create node configuration with a cache pre-configured.
     * @param gridName Node name.
     * @return Node configuration with a cache pre-configured.
     * @throws Exception if failed.
     */
    @SuppressWarnings("unchecked")
    private IgniteConfiguration getConfigurationWithStaticCache(String gridName) throws Exception {
        IgniteConfiguration cfg = getConfiguration(gridName);

        CacheConfiguration ccfg = cacheCfg(STATIC_CACHE_NAME);

        ccfg.setIndexedTypes(Integer.class, Person.class);
        ccfg.setSqlEscapeAll(true);

        cfg.setCacheConfiguration(ccfg);

        return optimize(cfg);
    }

    /** */
    private CacheConfiguration cacheCfg(String name) {
        CacheConfiguration<?, ?> cfg = new CacheConfiguration<>();

        cfg.setName(name);

        cfg.setRebalanceMode(CacheRebalanceMode.NONE);

        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testDynamicSchemaChangesPersistence() throws Exception {
        checkSchemaStateAfterNodeRestart(false);
    }

    /** */
    @Test
    public void testDynamicSchemaChangesPersistenceWithAliveCluster() throws Exception {
        checkSchemaStateAfterNodeRestart(true);
    }

    /** */
    @Test
    public void testDynamicSchemaChangesPersistenceWithStaticCache() throws Exception {
        IgniteEx node = startGrid(getConfigurationWithStaticCache(getTestIgniteInstanceName(0)));

        node.active(true);

        IgniteCache cache = node.cache(STATIC_CACHE_NAME);

        assertNotNull(cache);

        CountDownLatch cnt = checkpointLatch(node);

        checkOriginalSchema(node, STATIC_CACHE_NAME, Collections.emptyList());

        makeDynamicSchemaChanges(node, STATIC_CACHE_NAME);

        checkModifiedSchema(node, STATIC_CACHE_NAME);

        cnt.await();

        stopGrid(0);

        // Restarting with no-cache configuration - otherwise stored configurations
        // will be ignored due to cache names duplication.
        node = startGrid(0);

        node.active(true);

        checkModifiedSchema(node, STATIC_CACHE_NAME);
    }

    /**
     * Perform test with cache created with {@code CREATE TABLE}.
     * @param aliveCluster Whether there should remain an alive node when tested node is restarted.
     * @throws Exception if failed.
     */
    private void checkSchemaStateAfterNodeRestart(boolean aliveCluster) throws Exception {
        IgniteEx node = startGrid(0);

        node.active(true);

        if (aliveCluster)
            startGrid(1);

        CountDownLatch cnt = checkpointLatch(node);

        runSql("create table \"Person\" (" +
            "\"id\" int primary key," +
            "\"name\" varchar," +
            "\"city\" int," +
            "\"str1\" varchar," +
            "\"str2\" char(10) not null default '1'," +
            "\"num1\" decimal," +
            "\"num2\" decimal(10, 2) not null default 1," +
            "\"time1\" timestamp," +
            "\"time2\" timestamp(10, 2) not null," +
            "\"float1\" float," +
            "\"float2\" float(10) not null)", node, QueryUtils.DFLT_SCHEMA);

        checkOriginalSchema(node, SQL_CACHE_NAME, F.asList("str2", "num2"));

        makeDynamicSchemaChanges(node, QueryUtils.DFLT_SCHEMA);

        checkModifiedSchema(node, SQL_CACHE_NAME);

        cnt.await();

        stopGrid(0);

        node = startGrid(0);

        node.active(true);

        checkModifiedSchema(node, SQL_CACHE_NAME);

        node.context().query().querySqlFields(new SqlFieldsQuery("drop table \"Person\""), false).getAll();
    }

    /**
     * @param node Node whose checkpoint to wait for.
     * @return Latch released when checkpoint happens.
     */
    private CountDownLatch checkpointLatch(IgniteEx node) {
        final CountDownLatch cnt = new CountDownLatch(1);

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)node.context().cache().context().database();

        db.addCheckpointListener(new CheckpointListener() {
            @Override public void onMarkCheckpointBegin(Context ctx) {
                cnt.countDown();
            }

            @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {

            }

            @Override public void onCheckpointBegin(Context ctx) {
                /* No-op. */
            }
        });

        return cnt;
    }

    /**
     * Create dynamic index and column.
     * @param node Node.
     * @param schema Schema name.
     */
    private void makeDynamicSchemaChanges(IgniteEx node, String schema) {
        runSql("create index \"my_idx\" on \"Person\" (\"id\", \"name\")", node, schema);

        runSql("alter table \"Person\" drop column \"city\", \"str1\", \"str2\", \"num1\", " +
            "\"num2\", \"time1\", \"time2\", \"float1\", \"float2\"", node, schema);

        runSql("alter table \"Person\" add column (" +
            "\"rate\" decimal(10, 2) not null," +
            "\"str1\" char(10) not null," +
            "\"str2\" varchar," +
            "\"num1\" decimal(10, 2) not null," +
            "\"num2\" decimal," +
            "\"time1\" timestamp(10, 2) not null," +
            "\"time2\" timestamp," +
            "\"float1\" float(10) not null," +
            "\"float2\" float)", node, schema);
    }

    /**
     * Executes SQL query.
     *
     * @param sql SQL query.
     * @param node Ignite node.
     * @param schema Target schema.
     * @return Query execution result.
     */
    private List<List<?>> runSql(String sql, IgniteEx node, String schema) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(sql).setSchema(schema), false).getAll();
    }

    /**
     * Check original schema.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param expDfltFields List of fields that have a default value.
     */
    private void checkOriginalSchema(IgniteEx node, String cacheName, List<String> expDfltFields) {
        Collection<QueryEntity> entities = node.context().cache().cacheDescriptor(cacheName).schema().entities();
        assertEquals(1, entities.size());

        QueryEntity e = F.first(entities);

        assertEquals(0, e.getIndexes().size());

        assertEqualsUnordered(F.asList("id", "name", "city", "str1", "str2",
            "num1", "num2", "time1", "time2", "float1", "float2"), e.getFields().keySet());
        assertEqualsUnordered(F.asList("str2", "num2", "time2", "float2"), e.getNotNullFields());
        assertEqualsUnordered(F.asList("num2"), e.getFieldsScale().keySet());
        assertEqualsUnordered(F.asList("str2", "num2"), e.getFieldsPrecision().keySet());
        assertEqualsUnordered(expDfltFields, e.getDefaultFieldValues().keySet());
    }

    /**
     * Check that dynamically created schema objects are in place.
     * @param node Node.
     * @param cacheName Cache name.
     */
    private void checkModifiedSchema(IgniteEx node, String cacheName) {
        Collection<QueryEntity> entities = node.context().cache().cacheDescriptor(cacheName).schema().entities();
        assertEquals(1, entities.size());

        QueryEntity e = F.first(entities);

        assertEquals(1, e.getIndexes().size());
        assertEquals(0, e.getDefaultFieldValues().size());

        assertEqualsUnordered(F.asList("id", "name", "rate", "str1", "str2",
            "num1", "num2", "time1", "time2", "float1", "float2"), e.getFields().keySet());
        assertEqualsUnordered(F.asList("rate", "str1", "num1", "time1", "float1"), e.getNotNullFields());
        assertEqualsUnordered(F.asList("rate", "num1"), e.getFieldsScale().keySet());
        assertEqualsUnordered(F.asList("rate", "str1", "num1"), e.getFieldsPrecision().keySet());
    }

    /**
     * Compares collections without considering the order of the elements.
     */
    private void assertEqualsUnordered(List<String> expected, Set<String> actual) {
        String msg = "expected=" + expected + ", actual=" + actual;

        assertEquals(msg, expected.size(), actual.size());
        assertTrue(msg, actual.containsAll(expected));
    }

    /**
     *
     */
    protected static class Person implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @SuppressWarnings("unused")
        private Person() {
            // No-op.
        }

        /** */
        public Person(int id) {
            this.id = id;
        }

        /** */
        @QuerySqlField
        protected int id;

        /** */
        @QuerySqlField
        protected String name;

        /** */
        @QuerySqlField
        protected int city;

        /** */
        @QuerySqlField
        protected String str1;

        /** */
        @QuerySqlField(precision = 10, notNull = true)
        protected String str2;

        /** */
        @QuerySqlField
        protected BigDecimal num1;

        /** */
        @QuerySqlField(precision = 10, scale = 2, notNull = true)
        protected BigDecimal num2;

        /** */
        @QuerySqlField
        protected Timestamp time1;

        /** */
        @QuerySqlField(notNull = true)
        protected Timestamp time2;

        /** */
        @QuerySqlField
        protected Float float1;

        /** */
        @QuerySqlField(notNull = true)
        protected Float float2;
    }
}
