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

package org.apache.ignite.internal.processors.rest;

import java.io.IOException;
import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlIndexMetadata;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlMetadata;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandler;
import org.apache.ignite.internal.processors.rest.model.VisorGatewayArgument;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.cache.VisorCacheConfigurationCollectorTask;
import org.apache.ignite.internal.visor.cache.VisorCacheConfigurationCollectorTaskArg;
import org.apache.ignite.internal.visor.cache.VisorCacheStopTask;
import org.apache.ignite.internal.visor.cache.VisorCacheStopTaskArg;
import org.apache.ignite.internal.visor.compute.VisorGatewayTask;
import org.apache.ignite.jdbc.thin.JdbcThinStatementCancelSelfTest;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.configuration.WALMode.NONE;
import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;
import static org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor.DATA_LOST_ON_DEACTIVATION_WARNING;
import static org.apache.ignite.internal.processors.query.QueryUtils.TEMPLATE_PARTITIONED;
import static org.apache.ignite.internal.processors.query.QueryUtils.TEMPLATE_REPLICATED;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CACHE_METADATA;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_ACTIVATE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_ACTIVE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_DEACTIVATE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_INACTIVE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_SET_STATE;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_FAILED;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;

/**
 * Tests for Jetty REST protocol.
 */
@SuppressWarnings("unchecked")
public abstract class JettyRestProcessorAbstractSelfTest extends JettyRestProcessorCommonSelfTest {
    /** Used to sent request charset. */
    private static final String CHARSET = StandardCharsets.UTF_8.name();

    /** */
    private static boolean memoryMetricsEnabled;

    /** */
    private static final AtomicInteger KEY_GEN = new AtomicInteger(0);

    /** Expected minimal query duration of query execution with sleep. */
    private static final int EXPECTED_MIN_DURATION = 100;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        initCache();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        grid(0).cluster().state(ACTIVE);

        grid(0).cache(DEFAULT_CACHE_NAME).removeAll();

        if (memoryMetricsEnabled) {
            memoryMetricsEnabled = false;

            restartGrid();
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
    }

    /**
     * @param content Content to check.
     * @param err Error message.
     */
    protected void assertResponseContainsError(String content, String err) throws IOException {
        assertFalse(F.isEmpty(content));
        assertNotNull(err);

        JsonNode node = JSON_MAPPER.readTree(content);

        assertTrue(node.get("successStatus").asInt() != STATUS_SUCCESS);
        assertTrue(node.get("response").isNull());
        assertTrue(node.get("error").asText().contains(err));
    }

    /**
     * @param content Content to check.
     * @return JSON node with actual response.
     */
    protected JsonNode assertResponseSucceeded(String content, boolean bulk) throws IOException {
        assertNotNull(content);
        assertFalse(content.isEmpty());

        JsonNode node = JSON_MAPPER.readTree(content);

        JsonNode affNode = node.get("affinityNodeId");

        if (affNode != null)
            assertEquals(bulk, affNode.isNull());

        assertEquals(STATUS_SUCCESS, node.get("successStatus").asInt());
        assertTrue(node.get("error").isNull());

        assertNotSame(securityEnabled(), node.get("sessionToken").isNull());

        return node.get("response");
    }

    /**
     * @param content Content to check.
     * @param res Response.
     */
    private void assertCacheOperation(String content, Object res) throws IOException {
        JsonNode ret = assertResponseSucceeded(content, false);

        assertEquals(String.valueOf(res), ret.isObject() ? ret.toString() : ret.asText());
    }

    /**
     * @param content Content to check.
     * @param res Response.
     */
    private void assertCacheBulkOperation(String content, Object res) throws IOException {
        JsonNode ret = assertResponseSucceeded(content, true);

        assertEquals(String.valueOf(res), ret.asText());
    }

    /**
     * @param content Content to check.
     */
    private void assertCacheMetrics(String content) throws IOException {
        JsonNode ret = assertResponseSucceeded(content, true);

        assertTrue(ret.isObject());
    }

    /**
     * Validates JSON response.
     *
     * @param content Content to check.
     * @return REST result.
     * @throws IOException If parsing failed.
     */
    protected JsonNode validateJsonResponse(String content) throws IOException {
        return validateJsonResponse(content, false);
    }

    /**
     * Validates JSON response.
     *
     * @param content Content to check.
     * @param errorExpected is error expected.
     * @return REST result if {@code errorExpected} is {@code false}. Error instead.
     * @throws IOException If parsing failed.
     */
    protected JsonNode validateJsonResponse(String content, boolean errorExpected) throws IOException {
        return validateJsonResponse(content, errorExpected, false);
    }

    /**
     * Validates JSON response.
     *
     * @param content Content to check.
     * @param errorExpected is error expected.
     * @param canBeUnauthenticated can a request be unauthenticated
     * @return REST result if {@code errorExpected} is {@code false}. Error instead.
     * @throws IOException If parsing failed.
     */
    protected JsonNode validateJsonResponse(String content, boolean errorExpected, boolean canBeUnauthenticated) throws IOException {
        assertNotNull(content);
        assertFalse(content.isEmpty());

        JsonNode node = JSON_MAPPER.readTree(content);

        JsonNode errNode = node.get("error");

        if (errorExpected)
            assertTrue("Expected an error.", !errNode.isNull());
        else
            assertTrue("Unexpected error: " + errNode.asText(), errNode.isNull());

        assertEquals(STATUS_SUCCESS, node.get("successStatus").asInt());

        if (!canBeUnauthenticated)
            assertNotSame(securityEnabled(), node.get("sessionToken").isNull());

        return node.get(errorExpected ? "error" : "response");
    }

    /**
     * @param content Content to check.
     * @return Task result.
     */
    protected JsonNode jsonTaskResult(String content) throws IOException {
        assertNotNull(content);
        assertFalse(content.isEmpty());

        JsonNode node = JSON_MAPPER.readTree(content);

        assertEquals(STATUS_SUCCESS, node.get("successStatus").asInt());
        assertTrue(node.get("error").isNull());
        assertFalse(node.get("response").isNull());

        assertEquals(securityEnabled(), !node.get("sessionToken").isNull());

        JsonNode res = node.get("response");

        assertTrue(res.isObject());

        assertFalse(res.get("id").isNull());
        assertTrue(res.get("finished").asBoolean());
        assertTrue(res.get("error").isNull());

        return res.get("result");
    }

    /**
     * Check task result with expected failure.
     *
     * @param content Content to check.
     * @return Node with failure result.
     */
    protected JsonNode jsonTaskErrorResult(String content) throws IOException {
        assertNotNull(content);
        assertFalse(content.isEmpty());

        JsonNode node = JSON_MAPPER.readTree(content);

        assertEquals(STATUS_FAILED, node.get("successStatus").asInt());
        assertFalse(node.get("error").isNull());
        assertTrue(node.get("response").isNull());

        JsonNode error = node.get("error");

        assertTrue(error.isTextual());

        return error;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGet() throws Exception {
        jcache().put("getKey", "getVal");

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET, "key", "getKey");

        info("Get command result: " + ret);

        assertCacheOperation(ret, "getVal");
    }

    /**
     * @param json JSON to check.
     * @param p Person to compare with.
     * @throws IOException If failed.
     */
    private void checkJson(String json, Person p) throws IOException {
        JsonNode res = assertResponseSucceeded(json, false);

        assertEquals(p.id.intValue(), res.get("id").asInt());
        assertEquals(p.getOrganizationId().intValue(), res.get("orgId").asInt());
        assertEquals(p.getFirstName(), res.get("firstName").asText());
        assertEquals(p.getLastName(), res.get("lastName").asText());
        assertEquals(p.getSalary(), res.get("salary").asDouble());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetBinaryObjects() throws Exception {
        Person p = new Person(1, "John", "Doe", 300);

        jcache().put(300, p);

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET,
            "keyType", "int",
            "key", "300"
        );

        info("Get command result: " + ret);

        checkJson(ret, p);

        // Test with remote node.
        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET,
            "keyType", "int",
            "key", "300",
            "destId", grid(1).localNode().id().toString()
        );

        info("Get command result: " + ret);

        checkJson(ret, p);

        // Test with SQL.
        SqlFieldsQuery qry = new SqlFieldsQuery(
            "create table employee(id integer primary key, name varchar(100), salary integer);" +
                "insert into employee(id, name, salary) values (1, 'Alex', 300);"
        );

        grid(0).context().query().querySqlFields(qry, true, false);

        ret = content("SQL_PUBLIC_EMPLOYEE", GridRestCommand.CACHE_GET,
            "keyType", "int",
            "key", "1"
        );

        info("Get command result: " + ret);

        JsonNode res = assertResponseSucceeded(ret, false);

        assertEquals("Alex", res.get("NAME").asText());
        assertEquals(300, res.get("SALARY").asInt());

        // Test with circular reference.
        CircularRef ref1 = new CircularRef(1, "Alex");
        CircularRef ref2 = new CircularRef(2, "300");
        CircularRef ref3 = new CircularRef(3, "220");

        ref1.ref(ref2);

        jcache().put(220, ref1);

        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET,
            "keyType", "int",
            "key", "220"
        );

        info("Get command result: " + ret);

        JsonNode json = assertResponseSucceeded(ret, false);
        assertEquals(ref1.name, json.get("name").asText());
        assertEquals(ref1.ref.toString(), json.get("ref").toString());

        ref2.ref(ref1);

        jcache().put(222, ref1);

        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET,
            "keyType", "int",
            "key", "222"
        );

        info("Get command result: " + ret);

        assertResponseContainsError(ret, "Failed convert to JSON object for circular references");

        ref1.ref(ref2);
        ref2.ref(ref3);
        ref3.ref(ref1);
        jcache().put(223, ref1);

        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET,
            "keyType", "int",
            "key", "223"
        );

        info("Get command result: " + ret);

        assertResponseContainsError(ret, "Failed convert to JSON object for circular references");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNullMapKeyAndValue() throws Exception {
        Map<String, String> map1 = new HashMap<>();
        map1.put(null, null);
        map1.put("key", "value");

        jcache().put("mapKey1", map1);

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET, "key", "mapKey1");

        info("Get command result: " + ret);

        JsonNode res = validateJsonResponse(ret);

        assertEquals(F.asMap("", null, "key", "value"), JSON_MAPPER.treeToValue(res, HashMap.class));

        Map<String, String> map2 = new HashMap<>();
        map2.put(null, "value");
        map2.put("key", null);

        jcache().put("mapKey2", map2);

        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET, "key", "mapKey2");

        info("Get command result: " + ret);

        res = validateJsonResponse(ret);

        assertEquals(F.asMap("", "value", "key", null), JSON_MAPPER.treeToValue(res, HashMap.class));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleObject() throws Exception {
        SimplePerson p = new SimplePerson(1, "Test", java.sql.Date.valueOf("1977-01-26"), 1000.55, 39, "CIO", 25);

        jcache().put("simplePersonKey", p);

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET, "key", "simplePersonKey");

        info("Get command result: " + ret);

        JsonNode res = assertResponseSucceeded(ret, false);

        assertEquals(p.id, res.get("id").asInt());
        assertEquals(p.name, res.get("name").asText());
        assertEquals(p.birthday.toString(), res.get("birthday").asText());
        assertEquals(p.salary, res.get("salary").asDouble());
        assertNull(res.get("age"));
        assertEquals(p.post, res.get("post").asText());
        assertEquals(25, res.get("bonus").asInt());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDate() throws Exception {
        java.util.Date utilDate = new java.util.Date();

        DateFormat formatter = DateFormat.getDateTimeInstance(DateFormat.DEFAULT, DateFormat.DEFAULT, Locale.US);

        String date = formatter.format(utilDate);

        jcache().put("utilDateKey", utilDate);

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET, "key", "utilDateKey");

        info("Get command result: " + ret);

        assertCacheOperation(ret, date);

        java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());

        jcache().put("sqlDateKey", sqlDate);

        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET, "key", "sqlDateKey");

        info("Get SQL result: " + ret);

        assertCacheOperation(ret, sqlDate.toString());

        jcache().put("timestampKey", new java.sql.Timestamp(utilDate.getTime()));

        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET, "key", "timestampKey");

        info("Get timestamp: " + ret);

        assertCacheOperation(ret, date);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUUID() throws Exception {
        UUID uuid = UUID.randomUUID();

        jcache().put("uuidKey", uuid);

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET, "key", "uuidKey");

        info("Get command result: " + ret);

        assertCacheOperation(ret, uuid.toString());

        IgniteUuid igniteUuid = IgniteUuid.fromUuid(uuid);

        jcache().put("igniteUuidKey", igniteUuid);

        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET, "key", "igniteUuidKey");

        info("Get command result: " + ret);

        assertCacheOperation(ret, igniteUuid.toString());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTuple() throws Exception {
        T2 t = new T2("key", "value");

        jcache().put("tupleKey", t);

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET, "key", "tupleKey");

        info("Get command result: " + ret);

        JsonNode res = assertResponseSucceeded(ret, false);

        assertEquals(t.getKey(), res.get("key").asText());
        assertEquals(t.getValue(), res.get("value").asText());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheSize() throws Exception {
        jcache().removeAll();

        jcache().put("getKey", "getVal");

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_SIZE);

        info("Size command result: " + ret);

        assertCacheBulkOperation(ret, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIgniteName() throws Exception {
        String ret = content(null, GridRestCommand.NAME);

        info("Name command result: " + ret);

        assertEquals(getTestIgniteInstanceName(0), validateJsonResponse(ret).asText());
    }

    /**
     * @param cacheName Cache name to create.
     * @param mode Expected cache mode.
     * @param backups Expected number of backups.
     * @param wrtSync Expected cache write synchronization mode.
     * @param params Optional cache params.
     */
    private void checkGetOrCreateAndDestroy(
        String cacheName,
        CacheMode mode,
        int backups,
        CacheWriteSynchronizationMode wrtSync,
        String cacheGrp,
        String dataRegion,
        String... params
    ) throws Exception {
        String ret = content(cacheName, GridRestCommand.GET_OR_CREATE_CACHE, params);

        info("GetOrCreateCache command result: " + ret);

        validateJsonResponse(ret);

        IgniteCache<String, String> cache = grid(0).cache(cacheName);

        cache.put("1", "1");

        CacheConfiguration ccfg = cache.getConfiguration(CacheConfiguration.class);

        assertEquals(backups, ccfg.getBackups());
        assertEquals(mode, ccfg.getCacheMode());
        assertEquals(wrtSync, ccfg.getWriteSynchronizationMode());

        if (!F.isEmpty(cacheGrp))
            assertEquals(cacheGrp, ccfg.getGroupName());

        if (!F.isEmpty(dataRegion))
            assertEquals(dataRegion, ccfg.getDataRegionName());

        ret = content(cacheName, GridRestCommand.DESTROY_CACHE);

        assertTrue(validateJsonResponse(ret).isNull());
        assertNull(grid(0).cache(cacheName));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetOrCreateCache() throws Exception {
        checkGetOrCreateAndDestroy("testCache", PARTITIONED, 0, FULL_SYNC, null, null);

        checkGetOrCreateAndDestroy("testCache", PARTITIONED, 3, FULL_SYNC, null, null,
            "backups", "3"
        );

        checkGetOrCreateAndDestroy("testCache", PARTITIONED, 2, FULL_ASYNC, null, null,
            "backups", "2",
            "writeSynchronizationMode", FULL_ASYNC.name()
        );

        checkGetOrCreateAndDestroy("testCache", REPLICATED, Integer.MAX_VALUE, FULL_ASYNC, null, null,
            "templateName", TEMPLATE_REPLICATED,
            "writeSynchronizationMode", FULL_ASYNC.name()
        );

        checkGetOrCreateAndDestroy("testCache", REPLICATED, Integer.MAX_VALUE, FULL_ASYNC, null, null,
            "templateName", TEMPLATE_REPLICATED,
            "backups", "0",
            "writeSynchronizationMode", FULL_ASYNC.name()
        );

        checkGetOrCreateAndDestroy("testCache", PARTITIONED, 1, FULL_ASYNC, "testGroup", null,
            "templateName", TEMPLATE_PARTITIONED,
            "backups", "1",
            "writeSynchronizationMode", FULL_ASYNC.name(),
            "cacheGroup", "testGroup"
        );

        checkGetOrCreateAndDestroy("testCache", PARTITIONED, 2, FULL_ASYNC, null, "testDataRegion",
            "templateName", TEMPLATE_PARTITIONED,
            "backups", "2",
            "writeSynchronizationMode", FULL_ASYNC.name(),
            "dataRegion", "testDataRegion"
        );

        checkGetOrCreateAndDestroy("testCache", PARTITIONED, 3, FULL_ASYNC, "testGroup", "testDataRegion",
            "templateName", TEMPLATE_PARTITIONED,
            "backups", "3",
            "writeSynchronizationMode", FULL_ASYNC.name(),
            "cacheGroup", "testGroup",
            "dataRegion", "testDataRegion"
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAll() throws Exception {
        final Map<String, String> entries = F.asMap("getKey1", "getVal1", "getKey2", "getVal2");

        jcache().putAll(entries);

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET_ALL,
            "k1", "getKey1",
            "k2", "getKey2"
        );

        info("Get all command result: " + ret);

        JsonNode res = assertResponseSucceeded(ret, true);

        assertTrue(res.isObject());

        assertEquals(entries, JSON_MAPPER.treeToValue(res, Map.class));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectPut() throws Exception {
        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_PUT, "key", "key0");

        assertResponseContainsError(ret,
            "Failed to find mandatory parameter in request: val");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContainsKey() throws Exception {
        grid(0).cache(DEFAULT_CACHE_NAME).put("key0", "val0");

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_CONTAINS_KEY, "key", "key0");

        assertCacheOperation(ret, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContainsKeys() throws Exception {
        grid(0).cache(DEFAULT_CACHE_NAME).put("key0", "val0");
        grid(0).cache(DEFAULT_CACHE_NAME).put("key1", "val1");

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_CONTAINS_KEYS,
            "k1", "key0",
            "k2", "key1"
        );

        assertCacheBulkOperation(ret, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndPut() throws Exception {
        grid(0).cache(DEFAULT_CACHE_NAME).put("key0", "val0");

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET_AND_PUT,
            "key", "key0",
            "val", "val1"
        );

        assertCacheOperation(ret, "val0");

        assertEquals("val1", grid(0).cache(DEFAULT_CACHE_NAME).get("key0"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndPutIfAbsent() throws Exception {
        grid(0).cache(DEFAULT_CACHE_NAME).put("key0", "val0");

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET_AND_PUT_IF_ABSENT,
            "key", "key0",
            "val", "val1"
        );

        assertCacheOperation(ret, "val0");

        assertEquals("val0", grid(0).cache(DEFAULT_CACHE_NAME).get("key0"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutIfAbsent2() throws Exception {
        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_PUT_IF_ABSENT,
            "key", "key0",
            "val", "val1"
        );

        assertCacheOperation(ret, true);

        assertEquals("val1", grid(0).cache(DEFAULT_CACHE_NAME).get("key0"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveValue() throws Exception {
        grid(0).cache(DEFAULT_CACHE_NAME).put("key0", "val0");

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_REMOVE_VALUE,
            "key", "key0",
            "val", "val1"
        );

        assertCacheOperation(ret, false);

        assertEquals("val0", grid(0).cache(DEFAULT_CACHE_NAME).get("key0"));

        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_REMOVE_VALUE,
            "key", "key0",
            "val", "val0"
        );

        assertCacheOperation(ret, true);

        assertNull(grid(0).cache(DEFAULT_CACHE_NAME).get("key0"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndRemove() throws Exception {
        grid(0).cache(DEFAULT_CACHE_NAME).put("key0", "val0");

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET_AND_REMOVE, "key", "key0");

        assertCacheOperation(ret, "val0");

        assertNull(grid(0).cache(DEFAULT_CACHE_NAME).get("key0"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplaceValue() throws Exception {
        grid(0).cache(DEFAULT_CACHE_NAME).put("key0", "val0");

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_REPLACE_VALUE,
            "key", "key0",
            "val", "val1",
            "val2", "val2"
        );

        assertCacheOperation(ret, false);

        assertEquals("val0", grid(0).cache(DEFAULT_CACHE_NAME).get("key0"));

        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_REPLACE_VALUE,
            "key", "key0",
            "val", "val1",
            "val2", "val0"
        );

        assertCacheOperation(ret, true);

        assertEquals("val1", grid(0).cache(DEFAULT_CACHE_NAME).get("key0"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndReplace() throws Exception {
        grid(0).cache(DEFAULT_CACHE_NAME).put("key0", "val0");

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET_AND_REPLACE,
            "key", "key0",
            "val", "val1"
        );

        assertCacheOperation(ret, "val0");

        assertEquals("val1", grid(0).cache(DEFAULT_CACHE_NAME).get("key0"));
    }

    /**
     * Performs all possible cluster state transitions via REST commands.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClusterStateChange() throws Exception {
        try {
            changeClusterState(CLUSTER_SET_STATE, ACTIVE, ACTIVE_READ_ONLY, false, true);

            changeClusterState(CLUSTER_SET_STATE, ACTIVE_READ_ONLY, INACTIVE, false, false);
            changeClusterState(CLUSTER_SET_STATE, ACTIVE_READ_ONLY, INACTIVE, true, true);

            changeClusterState(CLUSTER_SET_STATE, INACTIVE, ACTIVE, false, true);

            changeClusterState(CLUSTER_SET_STATE, ACTIVE, ACTIVE, false, true);

            changeClusterState(CLUSTER_SET_STATE, ACTIVE, INACTIVE, false, false);
            changeClusterState(CLUSTER_SET_STATE, ACTIVE, INACTIVE, true, true);

            changeClusterState(CLUSTER_SET_STATE, INACTIVE, INACTIVE, false, true);
            changeClusterState(CLUSTER_SET_STATE, INACTIVE, INACTIVE, true, true);

            changeClusterState(CLUSTER_SET_STATE, INACTIVE, ACTIVE_READ_ONLY, false, true);

            changeClusterState(CLUSTER_SET_STATE, ACTIVE_READ_ONLY, ACTIVE_READ_ONLY, false, true);

            changeClusterState(CLUSTER_SET_STATE, ACTIVE_READ_ONLY, ACTIVE, false, true);

            changeClusterStateByDepricatedCommands(CLUSTER_ACTIVATE, CLUSTER_DEACTIVATE);
            changeClusterStateByDepricatedCommands(CLUSTER_ACTIVE, CLUSTER_INACTIVE);
        }
        finally {
            grid(0).cluster().state(ACTIVE);

            initCache();
        }
    }

    /**
     * Performs all cluster state change transitions by depricated commands.
     *
     * @param activateCmd Cluster activate command.
     * @param deactivateCmd Cluster deactive command.
     * @throws Exception If failed.
     */
    private void changeClusterStateByDepricatedCommands(
        GridRestCommand activateCmd,
        GridRestCommand deactivateCmd
    ) throws Exception {
        assertTrue(activateCmd.name(), activateCmd == CLUSTER_ACTIVE || activateCmd == CLUSTER_ACTIVATE);
        assertTrue(deactivateCmd.name(), deactivateCmd == CLUSTER_INACTIVE || deactivateCmd == CLUSTER_DEACTIVATE);

        grid(0).cluster().state(ACTIVE);

        changeClusterState(deactivateCmd, ACTIVE, INACTIVE, false, false);
        changeClusterState(deactivateCmd, ACTIVE, INACTIVE, true, true);

        changeClusterState(activateCmd, INACTIVE, ACTIVE, false, true);

        grid(0).cluster().state(ACTIVE_READ_ONLY);

        changeClusterState(deactivateCmd, ACTIVE_READ_ONLY, INACTIVE, false, false);
        changeClusterState(deactivateCmd, ACTIVE_READ_ONLY, INACTIVE, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPut() throws Exception {
        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_PUT,
            "key", "putKey",
            "val", "putVal"
        );

        info("Put command result: " + ret);

        assertEquals("putVal", jcache().localPeek("putKey"));

        assertCacheOperation(ret, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutWithExpiration() throws Exception {
        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_PUT,
            "key", "putKey",
            "val", "putVal",
            "exp", "2000"
        );

        assertCacheOperation(ret, true);

        assertEquals("putVal", jcache().get("putKey"));

        Thread.sleep(2100);

        assertNull(jcache().get("putKey"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAdd() throws Exception {
        jcache().put("addKey1", "addVal1");

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_ADD,
            "key", "addKey2",
            "val", "addVal2"
        );

        assertCacheOperation(ret, true);

        assertEquals("addVal1", jcache().localPeek("addKey1"));
        assertEquals("addVal2", jcache().localPeek("addKey2"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAddWithExpiration() throws Exception {
        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_ADD,
            "key", "addKey",
            "val", "addVal",
            "exp", "2000"
        );

        assertCacheOperation(ret, true);

        assertEquals("addVal", jcache().get("addKey"));

        Thread.sleep(2100);

        assertNull(jcache().get("addKey"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAll() throws Exception {
        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_PUT_ALL,
            "k1", "putKey1",
            "k2", "putKey2",
            "v1", "putVal1",
            "v2", "putVal2"
        );

        info("Put all command result: " + ret);

        assertEquals("putVal1", jcache().localPeek("putKey1"));
        assertEquals("putVal2", jcache().localPeek("putKey2"));

        assertCacheBulkOperation(ret, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemove() throws Exception {
        jcache().put("rmvKey", "rmvVal");

        assertEquals("rmvVal", jcache().localPeek("rmvKey"));

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_REMOVE, "key", "rmvKey");

        info("Remove command result: " + ret);

        assertNull(jcache().localPeek("rmvKey"));

        assertCacheOperation(ret, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAll() throws Exception {
        jcache().put("rmvKey1", "rmvVal1");
        jcache().put("rmvKey2", "rmvVal2");
        jcache().put("rmvKey3", "rmvVal3");
        jcache().put("rmvKey4", "rmvVal4");

        assertEquals("rmvVal1", jcache().localPeek("rmvKey1"));
        assertEquals("rmvVal2", jcache().localPeek("rmvKey2"));
        assertEquals("rmvVal3", jcache().localPeek("rmvKey3"));
        assertEquals("rmvVal4", jcache().localPeek("rmvKey4"));

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_REMOVE_ALL,
            "k1", "rmvKey1",
            "k2", "rmvKey2"
        );

        info("Remove all command result: " + ret);

        assertNull(jcache().localPeek("rmvKey1"));
        assertNull(jcache().localPeek("rmvKey2"));
        assertEquals("rmvVal3", jcache().localPeek("rmvKey3"));
        assertEquals("rmvVal4", jcache().localPeek("rmvKey4"));

        assertCacheBulkOperation(ret, true);

        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_REMOVE_ALL);

        info("Remove all command result: " + ret);

        assertNull(jcache().localPeek("rmvKey1"));
        assertNull(jcache().localPeek("rmvKey2"));
        assertNull(jcache().localPeek("rmvKey3"));
        assertNull(jcache().localPeek("rmvKey4"));
        assertEquals(0, jcache().localSize());

        assertCacheBulkOperation(ret, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCas() throws Exception {
        jcache().put("casKey", "casOldVal");

        assertEquals("casOldVal", jcache().localPeek("casKey"));

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_CAS,
            "key", "casKey",
            "val2", "casOldVal",
            "val1", "casNewVal"
        );

        info("CAS command result: " + ret);

        assertEquals("casNewVal", jcache().localPeek("casKey"));

        assertCacheOperation(ret, true);

        jcache().remove("casKey");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplace() throws Exception {
        jcache().put("repKey", "repOldVal");

        assertEquals("repOldVal", jcache().localPeek("repKey"));

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_REPLACE,
            "key", "repKey",
            "val", "repVal"
        );

        info("Replace command result: " + ret);

        assertEquals("repVal", jcache().localPeek("repKey"));

        assertCacheOperation(ret, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplaceWithExpiration() throws Exception {
        jcache().put("replaceKey", "replaceVal");

        assertEquals("replaceVal", jcache().get("replaceKey"));

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_REPLACE,
            "key", "replaceKey",
            "val", "replaceValNew",
            "exp", "2000"
        );

        assertCacheOperation(ret, true);

        assertEquals("replaceValNew", jcache().get("replaceKey"));

        // Use larger value to avoid false positives.
        Thread.sleep(2100);

        assertNull(jcache().get("replaceKey"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAppend() throws Exception {
        jcache().put("appendKey", "appendVal");

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_APPEND,
            "key", "appendKey",
            "val", "_suffix"
        );

        assertCacheOperation(ret, true);

        assertEquals("appendVal_suffix", jcache().get("appendKey"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPrepend() throws Exception {
        jcache().put("prependKey", "prependVal");

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_PREPEND,
            "key", "prependKey",
            "val", "prefix_"
        );

        assertCacheOperation(ret, true);

        assertEquals("prefix_prependVal", jcache().get("prependKey"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIncrement() throws Exception {
        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.ATOMIC_INCREMENT,
            "key", "incrKey",
            "init", "2",
            "delta", "3"
        );

        JsonNode res = validateJsonResponse(ret);

        assertEquals(5, res.asInt());
        assertEquals(5, grid(0).atomicLong("incrKey", 0, true).get());

        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.ATOMIC_INCREMENT,
            "key", "incrKey",
            "delta", "10"
        );

        res = validateJsonResponse(ret);

        assertEquals(15, res.asInt());
        assertEquals(15, grid(0).atomicLong("incrKey", 0, true).get());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDecrement() throws Exception {
        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.ATOMIC_DECREMENT,
            "key", "decrKey",
            "init", "15",
            "delta", "10"
        );

        JsonNode res = validateJsonResponse(ret);

        assertEquals(5, res.asInt());
        assertEquals(5, grid(0).atomicLong("decrKey", 0, true).get());

        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.ATOMIC_DECREMENT,
            "key", "decrKey",
            "delta", "3"
        );

        res = validateJsonResponse(ret);

        assertEquals(2, res.asInt());
        assertEquals(2, grid(0).atomicLong("decrKey", 0, true).get());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCar() throws Exception {
        jcache().put("casKey", "casOldVal");

        assertEquals("casOldVal", jcache().localPeek("casKey"));

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_CAS,
            "key", "casKey",
            "val2", "casOldVal"
        );

        info("CAR command result: " + ret);

        assertNull(jcache().localPeek("casKey"));

        assertCacheOperation(ret, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutIfAbsent() throws Exception {
        assertNull(jcache().localPeek("casKey"));

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_CAS,
            "key", "casKey",
            "val1", "casNewVal"
        );

        info("PutIfAbsent command result: " + ret);

        assertEquals("casNewVal", jcache().localPeek("casKey"));

        assertCacheOperation(ret, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCasRemove() throws Exception {
        jcache().put("casKey", "casVal");

        assertEquals("casVal", jcache().localPeek("casKey"));

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_CAS, "key", "casKey");

        info("CAS Remove command result: " + ret);

        assertNull(jcache().localPeek("casKey"));

        assertCacheOperation(ret, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMetrics() throws Exception {
        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_METRICS);

        info("Cache metrics command result: " + ret);

        assertCacheMetrics(ret);
    }

    /**
     * Tests that PROBE command is handled successfully.
     */
    @Test
    public void testProbeCommand() throws Exception {
        String ret = content(null, GridRestCommand.PROBE);

        validateJsonResponse(ret, false, true);
    }

    /**
     * @param metas Metadata for Ignite caches.
     * @throws Exception If failed.
     */
    private void testMetadata(Collection<GridCacheSqlMetadata> metas, JsonNode arr) throws Exception {
        assertTrue(arr.isArray());

        for (JsonNode item : arr) {
            JsonNode cacheNameNode = item.get("cacheName");
            final String cacheName = (cacheNameNode == null || "null".equals(cacheNameNode.asText())) ? null : cacheNameNode.asText();

            GridCacheSqlMetadata meta = F.find(metas, null, new P1<GridCacheSqlMetadata>() {
                @Override public boolean apply(GridCacheSqlMetadata meta) {
                    return F.eq(meta.cacheName(), cacheName);
                }
            });

            assertNotNull("REST return metadata for unexpected cache: " + cacheName, meta);

            JsonNode types = item.get("types");

            assertNotNull(types);
            assertFalse(types.isNull());

            assertEqualsCollections(meta.types(), JSON_MAPPER.treeToValue(types, Collection.class));

            JsonNode keyClasses = item.get("keyClasses");

            assertNotNull(keyClasses);
            assertFalse(keyClasses.isNull());

            assertEquals(meta.keyClasses(), JSON_MAPPER.treeToValue(keyClasses, Map.class));

            JsonNode valClasses = item.get("valClasses");

            assertNotNull(valClasses);
            assertFalse(valClasses.isNull());

            assertEquals(meta.valClasses(), JSON_MAPPER.treeToValue(valClasses, Map.class));

            JsonNode fields = item.get("fields");

            assertNotNull(fields);
            assertFalse(fields.isNull());
            assertEquals(meta.fields(), JSON_MAPPER.treeToValue(fields, Map.class));

            JsonNode indexesByType = item.get("indexes");

            assertNotNull(indexesByType);
            assertFalse(indexesByType.isNull());
            assertEquals(meta.indexes().size(), indexesByType.size());

            for (Map.Entry<String, Collection<GridCacheSqlIndexMetadata>> metaIndexes : meta.indexes().entrySet()) {
                JsonNode indexes = indexesByType.get(metaIndexes.getKey());

                assertNotNull(indexes);
                assertFalse(indexes.isNull());
                assertEquals(metaIndexes.getValue().size(), indexes.size());

                for (final GridCacheSqlIndexMetadata metaIdx : metaIndexes.getValue()) {
                    JsonNode idx = F.find(indexes, null, new P1<JsonNode>() {
                        @Override public boolean apply(JsonNode idx) {
                            return metaIdx.name().equals(idx.get("name").asText());
                        }
                    });

                    assertNotNull(idx);

                    assertEqualsCollections(metaIdx.fields(),
                        JSON_MAPPER.treeToValue(idx.get("fields"), Collection.class));
                    assertEqualsCollections(metaIdx.descendings(),
                        JSON_MAPPER.treeToValue(idx.get("descendings"), Collection.class));
                    assertEquals(metaIdx.unique(), idx.get("unique").asBoolean());
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMetadataLocal() throws Exception {
        IgniteCacheProxy<?, ?> cache = F.first(grid(0).context().cache().publicCaches());

        assertNotNull("Should have configured public cache!", cache);

        Collection<GridCacheSqlMetadata> metas = cache.context().queries().sqlMetadata();

        // TODO: IGNITE-7740 uncomment after IGNITE-7740 will be fixed.
        // int cachesCnt = grid(0).cacheNames().size();
        // assertEquals(cachesCnt, metas.size());

        String ret = content("", CACHE_METADATA);

        info("Cache metadata: " + ret);

        JsonNode arrRes = validateJsonResponse(ret);

        // TODO: IGNITE-7740 uncomment after IGNITE-7740 will be fixed.
        // assertEquals(cachesCnt, arrRes.size());

        testMetadata(metas, arrRes);

        Collection<GridCacheSqlMetadata> dfltCacheMeta = cache.context().queries().sqlMetadata();

        ret = content(DEFAULT_CACHE_NAME, CACHE_METADATA);

        info("Cache metadata: " + ret);

        arrRes = validateJsonResponse(ret);

        assertEquals(1, arrRes.size());

        testMetadata(dfltCacheMeta, arrRes);

        assertResponseContainsError(content("nonExistingCacheName", CACHE_METADATA),
            "Failed to request meta data. nonExistingCacheName is not found");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMetadataRemote() throws Exception {
        CacheConfiguration<Integer, String> partialCacheCfg = new CacheConfiguration<>("partial");

        partialCacheCfg.setIndexedTypes(Integer.class, String.class);
        partialCacheCfg.setNodeFilter(new NodeConsistentIdFilter(grid(1).localNode().consistentId()));

        IgniteCacheProxy<Integer, String> c = (IgniteCacheProxy<Integer, String>)grid(1).createCache(partialCacheCfg);

        Collection<GridCacheSqlMetadata> metas = c.context().queries().sqlMetadata();

        String ret = content("", CACHE_METADATA);

        info("Cache metadata: " + ret);

        JsonNode arrRes = validateJsonResponse(ret);

        // TODO: IGNITE-7740 uncomment after IGNITE-7740 will be fixed.
        // int cachesCnt = grid(1).cacheNames().size();
        // assertEquals(cachesCnt, arrRes.size());

        testMetadata(metas, arrRes);

        ret = content("person", CACHE_METADATA);

        info("Cache metadata with cacheName parameter: " + ret);

        arrRes = validateJsonResponse(ret);

        assertEquals(1, arrRes.size());

        testMetadata(metas, arrRes);

        assertResponseContainsError(content("nonExistingCacheName", CACHE_METADATA),
            "Failed to request meta data. nonExistingCacheName is not found");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTopology() throws Exception {
        String ret = content(null, GridRestCommand.TOPOLOGY,
            "attr", "false",
            "mtr", "false"
        );

        info("Topology command result: " + ret);

        JsonNode res = validateJsonResponse(ret);

        assertEquals(gridCount(), res.size());

        for (JsonNode node : res) {
            assertTrue(node.get("attributes").isNull());
            assertTrue(node.get("metrics").isNull());

            JsonNode caches = node.get("caches");

            assertFalse(caches.isNull());

            Collection<IgniteCacheProxy<?, ?>> publicCaches = grid(0).context().cache().publicCaches();

            assertEquals(publicCaches.size(), caches.size());

            for (JsonNode cache : caches) {
                String cacheName0 = cache.get("name").asText();

                final String cacheName = cacheName0.isEmpty() ? null : cacheName0;

                IgniteCacheProxy<?, ?> publicCache = F.find(publicCaches, null, new P1<IgniteCacheProxy<?, ?>>() {
                    @Override public boolean apply(IgniteCacheProxy<?, ?> c) {
                        return F.eq(c.getName(), cacheName);
                    }
                });

                assertNotNull(publicCache);

                CacheMode cacheMode = CacheMode.valueOf(cache.get("mode").asText());

                assertEquals(publicCache.getConfiguration(CacheConfiguration.class).getCacheMode(), cacheMode);
            }
        }

        // Test that caches not included.
        ret = content(null, GridRestCommand.TOPOLOGY,
            "attr", "false",
            "mtr", "false",
            "caches", "false"
        );

        info("Topology command result: " + ret);

        res = validateJsonResponse(ret);

        assertEquals(gridCount(), res.size());

        for (JsonNode node : res) {
            assertTrue(node.get("attributes").isNull());
            assertTrue(node.get("metrics").isNull());
            assertTrue(node.get("caches").isNull());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNode() throws Exception {
        IgniteEx ignite = grid(0);
        String nid = ignite.localNode().id().toString();

        String ret = content(null, GridRestCommand.NODE,
            "attr", "true",
            "mtr", "true",
            "id", nid
        );

        info("Topology command result: " + ret);

        JsonNode res = validateJsonResponse(ret);

        assertTrue(res.get("attributes").isObject());
        assertTrue(res.get("metrics").isObject());

//        JsonNode attrs = res.get("attributes");
//        IgniteClusterEx cluster = ignite.cluster();
//
//        assertEquals(cluster.id().toString(), attrs.get(IGNITE_CLUSTER_ID).asText());
//        assertEquals(cluster.tag(), attrs.get(IGNITE_CLUSTER_NAME).asText());

        JsonNode caches = res.get("caches");

        assertTrue(caches.isArray());
        assertFalse(caches.isNull());
        assertEquals(ignite.context().cache().publicCaches().size(), caches.size());

        ret = content(null, GridRestCommand.NODE,
            "attr", "false",
            "mtr", "false",
            "ip", LOC_HOST
        );

        info("Topology command result: " + ret);

        res = validateJsonResponse(ret);

        assertTrue(res.get("attributes").isNull());
        assertTrue(res.get("metrics").isNull());

        ret = content(null, GridRestCommand.NODE,
            "attr", "false",
            "mtr", "false",
            "ip", LOC_HOST,
            "id", UUID.randomUUID().toString()
        );

        info("Topology command result: " + ret);

        res = validateJsonResponse(ret);

        assertTrue(res.isNull());

        // Check that caches not included.
        ret = content(null, GridRestCommand.NODE,
            "id", nid,
            "attr", "false",
            "mtr", "false",
            "caches", "false"
        );

        info("Topology command result: " + ret);

        res = validateJsonResponse(ret);

        assertTrue(res.get("attributes").isNull());
        assertTrue(res.get("metrics").isNull());
        assertTrue(res.get("caches").isNull());
    }

    /**
     * Tests {@code exe} command.
     * <p>
     * Note that attempt to execute unknown task (UNKNOWN_TASK) will result in exception on server.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExe() throws Exception {
        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.EXE);

        info("Exe command result: " + ret);

        assertResponseContainsError(ret, "Failed to find mandatory parameter in request: name");

        // Attempt to execute unknown task (UNKNOWN_TASK) will result in exception on server.
        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.EXE, "name", "UNKNOWN_TASK");

        info("Exe command result: " + ret);

        assertResponseContainsError(ret, "Unknown task name or failed to auto-deploy task (was task (re|un)deployed?)");

        grid(0).compute().localDeployTask(TestTask1.class, TestTask1.class.getClassLoader());
        grid(0).compute().localDeployTask(TestTask2.class, TestTask2.class.getClassLoader());

        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.EXE, "name", TestTask1.class.getName());

        info("Exe command result: " + ret);

        JsonNode res = jsonTaskResult(ret);

        assertTrue(res.isNull());

        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.EXE, "name", TestTask2.class.getName());

        info("Exe command result: " + ret);

        res = jsonTaskResult(ret);

        assertEquals(TestTask2.RES, res.asText());

        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.RESULT);

        info("Exe command result: " + ret);

        assertResponseContainsError(ret, "Failed to find mandatory parameter in request: id");
    }

    /**
     * Tests execution of Visor tasks via {@link VisorGatewayTask}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testVisorGatewayCacheConfigurationCollectorTask() throws Exception {
        ClusterNode locNode = grid(1).localNode();

        String ret = content(new VisorGatewayArgument(VisorCacheConfigurationCollectorTask.class)
            .setNode(locNode)
            .setTaskArgument(VisorCacheConfigurationCollectorTaskArg.class)
            .addCollectionArgument(String.class, "person"));

        info("VisorCacheConfigurationCollectorTask result: " + ret);

        jsonTaskResult(ret);

        assertTrue(ret.contains(String.format("\"memoryPolicyName\":\"%s\"",
            grid(1).configuration().getDataStorageConfiguration().getDefaultDataRegionConfiguration().getName()
        )));
    }

        /**
         * Tests execution of Visor tasks via {@link VisorGatewayTask}.
         *
         * @throws Exception If failed.
         */
    @Test
    public void testVisorGateway() throws Exception {
        ClusterNode locNode = grid(1).localNode();

        String ret = content(new VisorGatewayArgument(VisorCacheStopTask.class)
            .setNode(locNode)
            .setTaskArgument(VisorCacheStopTaskArg.class, "c"));

        info("VisorCacheStopTask result: " + ret);

        jsonTaskResult(ret);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDataRegionMetrics() throws Exception {
        String ret = content(F.asMap("cmd", GridRestCommand.DATA_REGION_METRICS.key()));

        JsonNode res = validateJsonResponse(ret);

        assertTrue(res.size() > 0);

        info(GridRestCommand.DATA_REGION_METRICS.key().toUpperCase() + " command result: " + ret);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDataStorageMetricsDisabled() throws Exception {
        if (memoryMetricsEnabled) {
            memoryMetricsEnabled = false;

            restartGrid();
        }

        String ret = content(F.asMap("cmd", GridRestCommand.DATA_STORAGE_METRICS.key()));

        JsonNode res = validateJsonResponse(ret);

        assertTrue(res.asText().equalsIgnoreCase("Storage metrics are not enabled"));

        info(GridRestCommand.DATA_STORAGE_METRICS.key().toUpperCase() + " command result: " + ret);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDataStorageMetricsEnabled() throws Exception {
        if (!memoryMetricsEnabled) {
            memoryMetricsEnabled = true;

            restartGrid();
        }

        String ret = content(F.asMap("cmd", GridRestCommand.DATA_STORAGE_METRICS.key()));

        assertNotNull(validateJsonResponse(ret));

        info(GridRestCommand.DATA_STORAGE_METRICS.key().toUpperCase() + " command result: " + ret);
    }

    /**
     * Restart grid.
     *
     * @throws Exception If failed.
     */
    protected void restartGrid() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        startGrids(gridCount());

        grid(0).cluster().active(true);

        initCache();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersion() throws Exception {
        String ret = content(null, GridRestCommand.VERSION);

        JsonNode res = validateJsonResponse(ret, false, true);

        assertEquals(VER_STR, res.asText());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryArgs() throws Exception {
        String qry = "salary > ? and salary <= ?";

        String ret = content("person", GridRestCommand.EXECUTE_SQL_QUERY,
            "type", "Person",
            "pageSize", "10",
            "qry", URLEncoder.encode(qry, CHARSET),
            "arg1", "1000",
            "arg2", "2000"
        );

        JsonNode items = validateJsonResponse(ret).get("items");

        assertEquals(2, items.size());

        assertFalse(queryCursorFound());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryScan() throws Exception {
        String ret = content("person", GridRestCommand.EXECUTE_SCAN_QUERY,
            "pageSize", "10",
            "cacheName", "person"
        );

        JsonNode items = validateJsonResponse(ret).get("items");

        assertEquals(4, items.size());

        assertFalse(queryCursorFound());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFilterQueryScan() throws Exception {
        String ret = content("person", GridRestCommand.EXECUTE_SCAN_QUERY,
            "pageSize", "10",
            "className", ScanFilter.class.getName()
        );

        JsonNode items = validateJsonResponse(ret).get("items");

        assertEquals(2, items.size());

        assertFalse(queryCursorFound());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectFilterQueryScan() throws Exception {
        String clsName = ScanFilter.class.getName() + 1;

        String ret = content("person", GridRestCommand.EXECUTE_SCAN_QUERY,
            "pageSize", "10",
            "className", clsName
        );

        assertResponseContainsError(ret, "Failed to find target class: " + clsName);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQuery() throws Exception {
        grid(0).cache(DEFAULT_CACHE_NAME).put("1", "1");
        grid(0).cache(DEFAULT_CACHE_NAME).put("2", "2");
        grid(0).cache(DEFAULT_CACHE_NAME).put("3", "3");

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.EXECUTE_SQL_QUERY,
            "type", "String",
            "pageSize", "1",
            "qry", URLEncoder.encode("select * from String", CHARSET)
        );

        JsonNode qryId = validateJsonResponse(ret).get("queryId");

        assertFalse(validateJsonResponse(ret).get("queryId").isNull());

        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.FETCH_SQL_QUERY,
            "pageSize", "1",
            "qryId", qryId.asText()
        );

        JsonNode res = validateJsonResponse(ret);

        JsonNode qryId0 = validateJsonResponse(ret).get("queryId");

        assertEquals(qryId0, qryId);
        assertFalse(res.get("last").asBoolean());

        ret = content(DEFAULT_CACHE_NAME, GridRestCommand.FETCH_SQL_QUERY,
            "pageSize", "1",
            "qryId", qryId.asText()
        );

        res = validateJsonResponse(ret);

        qryId0 = validateJsonResponse(ret).get("queryId");

        assertEquals(qryId0, qryId);
        assertTrue(res.get("last").asBoolean());

        assertFalse(queryCursorFound());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDistributedJoinsQuery() throws Exception {
        String qry = "select * from Person, \"organization\".Organization " +
            "where \"organization\".Organization.id = Person.orgId " +
            "and \"organization\".Organization.name = ?";

        String ret = content("person", GridRestCommand.EXECUTE_SQL_QUERY,
            "type", "Person",
            "distributedJoins", "true",
            "pageSize", "10",
            "qry", URLEncoder.encode(qry, CHARSET),
            "arg1", "o1"
        );

        JsonNode items = validateJsonResponse(ret).get("items");

        assertEquals(2, items.size());

        assertFalse(queryCursorFound());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSqlFieldsQuery() throws Exception {
        String qry = "select concat(firstName, ' ', lastName) from Person";

        String ret = content("person", GridRestCommand.EXECUTE_SQL_FIELDS_QUERY,
            "pageSize", "10",
            "qry", URLEncoder.encode(qry, CHARSET)
        );

        JsonNode items = validateJsonResponse(ret).get("items");

        assertEquals(4, items.size());

        assertFalse(queryCursorFound());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDistributedJoinsSqlFieldsQuery() throws Exception {
        String qry = "select * from \"person\".Person p, \"organization\".Organization o where o.id = p.orgId";

        String ret = content("person", GridRestCommand.EXECUTE_SQL_FIELDS_QUERY,
            "distributedJoins", "true",
            "pageSize", "10",
            "qry", URLEncoder.encode(qry, CHARSET)
        );

        JsonNode items = validateJsonResponse(ret).get("items");

        assertEquals(4, items.size());

        assertFalse(queryCursorFound());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSqlFieldsMetadataQuery() throws Exception {
        String qry = "select firstName, lastName from Person";

        String ret = content("person", GridRestCommand.EXECUTE_SQL_FIELDS_QUERY,
            "pageSize", "10",
            "qry", URLEncoder.encode(qry, CHARSET)
        );

        JsonNode res = validateJsonResponse(ret);

        JsonNode items = res.get("items");

        JsonNode meta = res.get("fieldsMetadata");

        assertEquals(4, items.size());
        assertEquals(2, meta.size());

        JsonNode o = meta.get(0);

        assertEquals("FIRSTNAME", o.get("fieldName").asText());
        assertEquals("java.lang.String", o.get("fieldTypeName").asText());
        assertEquals("person", o.get("schemaName").asText());
        assertEquals("PERSON", o.get("typeName").asText());

        assertFalse(queryCursorFound());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryClose() throws Exception {
        String qry = "salary > ? and salary <= ?";

        String ret = content("person", GridRestCommand.EXECUTE_SQL_QUERY,
            "type", "Person",
            "pageSize", "1",
            "qry", URLEncoder.encode(qry, CHARSET),
            "arg1", "1000",
            "arg2", "2000"
        );

        JsonNode res = validateJsonResponse(ret);

        assertEquals(1, res.get("items").size());

        assertTrue(queryCursorFound());

        assertFalse(res.get("queryId").isNull());

        String qryId = res.get("queryId").asText();

        content("person", GridRestCommand.CLOSE_SQL_QUERY, "qryId", qryId);

        assertFalse(queryCursorFound());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryDelay() throws Exception {
        String qry = "salary > ? and salary <= ?";

        String ret = null;

        for (int i = 0; i < 10; ++i)
            ret = content("person", GridRestCommand.EXECUTE_SQL_QUERY,
                "type", "Person",
                "pageSize", "1",
                "qry", URLEncoder.encode(qry, CHARSET),
                "arg1", "1000",
                "arg2", "2000"
            );

        JsonNode items = validateJsonResponse(ret).get("items");

        assertEquals(1, items.size());

        assertTrue(queryCursorFound());

        U.sleep(10000);

        assertFalse(queryCursorFound());
    }

    /**
     * @return Cache.
     */
    protected <K, V> IgniteCache<K, V> typedCache() {
        return grid(0).cache("test_typed_access");
    }

    /**
     * @param type Key and value type.
     * @param k Key to put.
     * @param v Value to put.
     * @param status Expected operation status to check.
     * @throws Exception If failed.
     */
    private void putTypedValue(String type, String k, String v, int status) throws Exception {
        String ret = content("test_typed_access", GridRestCommand.CACHE_PUT,
            "keyType", type,
            "valueType", type,
            "key", k,
            "val", v
        );

        info("Command result: " + ret);

        JsonNode json = JSON_MAPPER.readTree(ret);

        assertEquals(status, json.get("successStatus").asInt());

        if (status == STATUS_SUCCESS)
            assertTrue(json.get("error").isNull());
        else
            assertTrue(json.get("error").asText().startsWith("Failed to convert value to specified type [type="));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTypedPut() throws Exception {
        // Test boolean type.
        putTypedValue("boolean", "true", "false", STATUS_SUCCESS);
        putTypedValue("java.lang.Boolean", "false", "true", STATUS_SUCCESS);

        IgniteCache<Boolean, Boolean> cBool = typedCache();

        assertEquals(Boolean.FALSE, cBool.get(true));
        assertEquals(Boolean.TRUE, cBool.get(false));

        // Test byte type.
        putTypedValue("byte", "64", "100", STATUS_SUCCESS);
        putTypedValue("java.lang.Byte", "-25", "-127", STATUS_SUCCESS);
        putTypedValue("byte", "65", "aaa", STATUS_FAILED);
        putTypedValue("byte", "aaa", "64", STATUS_FAILED);
        putTypedValue("byte", "aaa", "aaa", STATUS_FAILED);

        IgniteCache<Byte, Byte> cByte = typedCache();

        assertEquals(Byte.valueOf("100"), cByte.get(Byte.valueOf("64")));
        assertEquals(Byte.valueOf("-127"), cByte.get(Byte.valueOf("-25")));

        // Test short type.
        putTypedValue("short", "1024", "4096", STATUS_SUCCESS);
        putTypedValue("java.lang.Short", "-15000", "-16000", STATUS_SUCCESS);
        putTypedValue("short", "1025", "bbb", STATUS_FAILED);
        putTypedValue("short", "bbb", "5", STATUS_FAILED);
        putTypedValue("short", "bbb", "bbb", STATUS_FAILED);

        IgniteCache<Short, Short> cShort = typedCache();

        assertEquals(Short.valueOf("4096"), cShort.get(Short.valueOf("1024")));
        assertEquals(Short.valueOf("-16000"), cShort.get(Short.valueOf("-15000")));

        // Test integer type.
        putTypedValue("int", "65555", "128256", STATUS_SUCCESS);
        putTypedValue("Integer", "74555", "200000", STATUS_SUCCESS);
        putTypedValue("java.lang.Integer", "-200", "-100000", STATUS_SUCCESS);
        putTypedValue("int", "0", "ccc", STATUS_FAILED);
        putTypedValue("int", "ccc", "0", STATUS_FAILED);
        putTypedValue("int", "ccc", "ccc", STATUS_FAILED);

        IgniteCache<Integer, Integer> cInt = typedCache();

        assertEquals(Integer.valueOf(128256), cInt.get(65555));
        assertEquals(Integer.valueOf(200000), cInt.get(74555));
        assertEquals(Integer.valueOf(-100000), cInt.get(-200));

        // Test long type.
        putTypedValue("long", "3000000", "400000", STATUS_SUCCESS);
        putTypedValue("java.lang.Long", "-3000000", "-400000", STATUS_SUCCESS);
        putTypedValue("long", "777", "ddd", STATUS_FAILED);
        putTypedValue("long", "ddd", "777", STATUS_FAILED);
        putTypedValue("long", "ddd", "ddd", STATUS_FAILED);

        IgniteCache<Long, Long> cLong = typedCache();

        assertEquals(Long.valueOf(400000), cLong.get(3000000L));
        assertEquals(Long.valueOf(-400000), cLong.get(-3000000L));

        // Test float type.
        putTypedValue("float", "1.5", "2.5", STATUS_SUCCESS);
        putTypedValue("java.lang.Float", "-7.5", "-8.5", STATUS_SUCCESS);
        putTypedValue("float", "1.5", "hhh", STATUS_FAILED);
        putTypedValue("float", "hhh", "1.5", STATUS_FAILED);
        putTypedValue("float", "hhh", "hhh", STATUS_FAILED);

        IgniteCache<Float, Float> cFloat = typedCache();

        assertEquals(2.5f, cFloat.get(1.5f));
        assertEquals(-8.5f, cFloat.get(-7.5f));

        // Test double type.
        putTypedValue("double", "5.5", "75.5", STATUS_SUCCESS);
        putTypedValue("java.lang.Double", "-155.5", "-255.5", STATUS_SUCCESS);
        putTypedValue("double", "jjj", "75.5", STATUS_FAILED);
        putTypedValue("double", "6.5", "jjj", STATUS_FAILED);
        putTypedValue("double", "jjj", "jjj", STATUS_FAILED);

        IgniteCache<Double, Double> cDouble = typedCache();

        assertEquals(75.5d, cDouble.get(5.5d));
        assertEquals(-255.5d, cDouble.get(-155.5d));

        // Test date type.
        putTypedValue("date", "2018-02-18", "2017-01-01", STATUS_SUCCESS);
        putTypedValue("java.sql.Date", "2018-01-01", "2017-02-02", STATUS_SUCCESS);
        putTypedValue("date", "xxxx-yy-mm", "2017-01-01", STATUS_FAILED);
        putTypedValue("date", "2018-03-18", "xxxx-yy-mm", STATUS_FAILED);
        putTypedValue("date", "xxxx-yy-mm", "xxxx-yy-mm", STATUS_FAILED);

        IgniteCache<Date, Date> cDate = typedCache();

        assertEquals(Date.valueOf("2017-01-01"), cDate.get(Date.valueOf("2018-02-18")));
        assertEquals(Date.valueOf("2017-02-02"), cDate.get(Date.valueOf("2018-01-01")));

        // Test time type.
        putTypedValue("Time", "01:01:01", "02:02:02", STATUS_SUCCESS);
        putTypedValue("java.sql.Time", "03:03:03", "04:04:04", STATUS_SUCCESS);
        putTypedValue("Time", "aa:bb:dd", "02:02:02", STATUS_FAILED);
        putTypedValue("Time", "01:01:01", "zz:vv:pp", STATUS_FAILED);
        putTypedValue("Time", "zz:zz:zz", "zz:zz:zz", STATUS_FAILED);

        IgniteCache<Time, Time> cTime = typedCache();

        assertEquals(Time.valueOf("02:02:02"), cTime.get(Time.valueOf("01:01:01")));
        assertEquals(Time.valueOf("04:04:04"), cTime.get(Time.valueOf("03:03:03")));

        // Test timestamp type.
        putTypedValue("Timestamp", "2018-02-18%2001:01:01", "2017-01-01%2002:02:02", STATUS_SUCCESS);
        putTypedValue("java.sql.timestamp", "2018-01-01%2001:01:01", "2018-05-05%2005:05:05", STATUS_SUCCESS);
        putTypedValue("timestamp", "error", "2018-03-18%2001:01:01", STATUS_FAILED);
        putTypedValue("timestamp", "2018-03-18%2001:01:01", "error", STATUS_FAILED);
        putTypedValue("timestamp", "error", "error", STATUS_FAILED);

        IgniteCache<Timestamp, Timestamp> cTs = typedCache();

        assertEquals(Timestamp.valueOf("2017-01-01 02:02:02"), cTs.get(Timestamp.valueOf("2018-02-18 01:01:01")));
        assertEquals(Timestamp.valueOf("2018-05-05 05:05:05"), cTs.get(Timestamp.valueOf("2018-01-01 01:01:01")));

        // Test UUID type.
        UUID k1 = UUID.fromString("121f5ae8-148d-11e8-b642-0ed5f89f718b");
        UUID v1 = UUID.fromString("64c6c225-b31c-4000-b136-ef14562ac785");
        putTypedValue("UUID", k1.toString(), v1.toString(), STATUS_SUCCESS);
        putTypedValue("UUID", "error", v1.toString(), STATUS_FAILED);
        putTypedValue("UUID", k1.toString(), "error", STATUS_FAILED);
        putTypedValue("UUID", "error", "error", STATUS_FAILED);

        UUID k2 = UUID.randomUUID();
        UUID v2 = UUID.randomUUID();
        putTypedValue("java.util.UUID", k2.toString(), v2.toString(), STATUS_SUCCESS);

        IgniteCache<UUID, UUID> cUUID = typedCache();

        assertEquals(v1, cUUID.get(k1));
        assertEquals(v2, cUUID.get(k2));

        // Test IgniteUuid type.
        IgniteUuid ik1 = IgniteUuid.randomUuid();
        IgniteUuid iv1 = IgniteUuid.randomUuid();
        putTypedValue("IgniteUuid", ik1.toString(), iv1.toString(), STATUS_SUCCESS);
        putTypedValue("IgniteUuid", "error", iv1.toString(), STATUS_FAILED);
        putTypedValue("IgniteUuid", ik1.toString(), "error", STATUS_FAILED);
        putTypedValue("IgniteUuid", "error", "error", STATUS_FAILED);

        IgniteUuid ik2 = IgniteUuid.randomUuid();
        IgniteUuid iv2 = IgniteUuid.randomUuid();
        putTypedValue("org.apache.ignite.lang.IgniteUuid", ik2.toString(), iv2.toString(), STATUS_SUCCESS);

        IgniteCache<IgniteUuid, IgniteUuid> cIgniteUUID = typedCache();

        assertEquals(iv1, cIgniteUUID.get(ik1));
        assertEquals(iv2, cIgniteUUID.get(ik2));
    }

    /**
     * @param keyType Key type.
     * @param k Key value.
     * @param exp Expected value to test.
     * @throws Exception If failed.
     */
    private void getTypedValue(String keyType, String k, String exp) throws Exception {
        String ret = content("test_typed_access", GridRestCommand.CACHE_GET,
            "keyType", keyType,
            "key", k
        );

        info("Command result: " + ret);

        JsonNode json = validateJsonResponse(ret);

        assertEquals(exp, json.isObject() ? json.toString() : json.asText());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTypedGet() throws Exception {
        // Test boolean type.
        IgniteCache<Boolean, Boolean> cBool = typedCache();

        cBool.put(true, false);
        cBool.put(false, true);

        getTypedValue("boolean", "true", "false");
        getTypedValue("java.lang.Boolean", "false", "true");

        // Test byte type.
        IgniteCache<Byte, Byte> cByte = typedCache();

        cByte.put((byte)77, (byte)55);
        cByte.put((byte)-88, (byte)-10);

        getTypedValue("byte", "77", "55");
        getTypedValue("java.lang.Byte", "-88", "-10");

        // Test short type.
        IgniteCache<Short, Short> cShort = typedCache();

        cShort.put((short)2222, (short)3333);
        cShort.put((short)-11111, (short)-12222);

        getTypedValue("short", "2222", "3333");
        getTypedValue("java.lang.Short", "-11111", "-12222");

        // Test integer type.
        IgniteCache<Integer, Integer> cInt = typedCache();
        cInt.put(65555, 128256);
        cInt.put(74555, 200000);
        cInt.put(-200, -100000);

        getTypedValue("int", "65555", "128256");
        getTypedValue("Integer", "74555", "200000");
        getTypedValue("java.lang.Integer", "-200", "-100000");

        // Test long type.
        IgniteCache<Long, Long> cLong = typedCache();

        cLong.put(3333333L, 4444444L);
        cLong.put(-3333333L, -4444444L);

        getTypedValue("long", "3333333", "4444444");
        getTypedValue("java.lang.Long", "-3333333", "-4444444");

        // Test float type.
        IgniteCache<Float, Float> cFloat = typedCache();

        cFloat.put(11.5f, 21.5f);
        cFloat.put(-71.5f, -81.5f);

        getTypedValue("float", "11.5", "21.5");
        getTypedValue("java.lang.Float", "-71.5", "-81.5");

        // Test double type.
        IgniteCache<Double, Double> cDouble = typedCache();

        cDouble.put(58.5d, 758.5d);
        cDouble.put(-1558.5d, -2558.5d);

        getTypedValue("double", "58.5", "758.5");
        getTypedValue("java.lang.Double", "-1558.5", "-2558.5");

        // Test date type.
        IgniteCache<Date, Date> cDate = typedCache();

        cDate.put(Date.valueOf("2018-02-18"), Date.valueOf("2017-01-01"));
        cDate.put(Date.valueOf("2018-01-01"), Date.valueOf("2017-02-02"));

        getTypedValue("Date", "2018-02-18", "2017-01-01");
        getTypedValue("java.sql.Date", "2018-01-01", "2017-02-02");

        // Test time type.
        IgniteCache<Time, Time> cTime = typedCache();

        cTime.put(Time.valueOf("01:01:01"), Time.valueOf("02:02:02"));
        cTime.put(Time.valueOf("03:03:03"), Time.valueOf("04:04:04"));

        getTypedValue("Time", "01:01:01", "02:02:02");
        getTypedValue("java.sql.Time", "03:03:03", "04:04:04");

        // Test timestamp type.
        IgniteCache<Timestamp, String> cTimestamp = typedCache();

        cTimestamp.put(Timestamp.valueOf("2018-02-18 01:01:01"), "test1");
        cTimestamp.put(Timestamp.valueOf("2018-01-01 01:01:01"), "test2");

        getTypedValue("Timestamp", "2018-02-18%2001:01:01", "test1");
        getTypedValue("java.sql.timestamp", "2018-01-01%2001:01:01", "test2");

        // Test UUID type.
        IgniteCache<UUID, UUID> cUUID = typedCache();

        UUID k1 = UUID.fromString("121f5ae8-148d-11e8-b642-0ed5f89f718b");
        UUID v1 = UUID.fromString("64c6c225-b31c-4000-b136-ef14562ac785");
        cUUID.put(k1, v1);

        UUID k2 = UUID.randomUUID();
        UUID v2 = UUID.randomUUID();
        cUUID.put(k2, v2);

        getTypedValue("UUID", k1.toString(), v1.toString());
        getTypedValue("java.util.UUID", k2.toString(), v2.toString());

        // Test IgniteUuid type.
        IgniteCache<IgniteUuid, IgniteUuid> cIgniteUUID = typedCache();

        IgniteUuid ik1 = IgniteUuid.randomUuid();
        IgniteUuid iv1 = IgniteUuid.randomUuid();
        cIgniteUUID.put(ik1, iv1);

        IgniteUuid ik2 = IgniteUuid.randomUuid();
        IgniteUuid iv2 = IgniteUuid.randomUuid();
        cIgniteUUID.put(ik2, iv2);

        getTypedValue("IgniteUuid", ik1.toString(), iv1.toString());
        getTypedValue("org.apache.ignite.lang.IgniteUuid", ik2.toString(), iv2.toString());

        // Test tuple.
        IgniteCache<Integer, T2<Integer, String>> cTuple = typedCache();

        T2<Integer, String> tup = new T2<>(1, "test");

        cTuple.put(555, tup);

        getTypedValue("int", "555", JSON_MAPPER.writeValueAsString(tup));

        // Test enum.
        IgniteCache<Integer, CacheMode> cEnum = typedCache();

        cEnum.put(888, PARTITIONED);

        getTypedValue("int", "888", PARTITIONED.toString());
    }

    /**
     * Test to check work of Cache REST commands without cache name
     *
     * Steps:
     * 1) Start test node
     * 2) From REST client call all Cache commands
     * 3) All requests except request for CACHE_METADATA should fail with
     * "Failed to find mandatory parameter in request: cacheName" exception
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheCommandsWithoutCacheName() throws Exception {
        final String ERROR_MSG = "Failed to find mandatory parameter in request: cacheName";

        EnumSet<GridRestCommand> cacheCommands = EnumSet.of(GridRestCommand.DESTROY_CACHE,
            GridRestCommand.GET_OR_CREATE_CACHE,
            GridRestCommand.CACHE_CONTAINS_KEYS,
            GridRestCommand.CACHE_CONTAINS_KEY,
            GridRestCommand.CACHE_GET,
            GridRestCommand.CACHE_GET_AND_PUT,
            GridRestCommand.CACHE_GET_AND_REPLACE,
            GridRestCommand.CACHE_GET_AND_PUT_IF_ABSENT,
            GridRestCommand.CACHE_PUT_IF_ABSENT,
            GridRestCommand.CACHE_GET_ALL,
            GridRestCommand.CACHE_PUT,
            GridRestCommand.CACHE_ADD,
            GridRestCommand.CACHE_PUT_ALL,
            GridRestCommand.CACHE_REMOVE,
            GridRestCommand.CACHE_REMOVE_VALUE,
            GridRestCommand.CACHE_REPLACE_VALUE,
            GridRestCommand.CACHE_GET_AND_REMOVE,
            GridRestCommand.CACHE_REMOVE_ALL,
            GridRestCommand.CACHE_REPLACE,
            GridRestCommand.CACHE_CAS,
            GridRestCommand.CACHE_APPEND,
            GridRestCommand.CACHE_PREPEND,
            GridRestCommand.CACHE_METRICS,
            GridRestCommand.CACHE_SIZE,
            CACHE_METADATA);

        for (GridRestCommand command : cacheCommands) {
            String ret = content(null, command);

            if (command == CACHE_METADATA)
                validateJsonResponse(ret);
            else {
                JsonNode err = jsonTaskErrorResult(ret);
                assertFalse(err.isNull());
                assertTrue(err.textValue().contains(ERROR_MSG));
            }
        }
    }

    /**
     * Test to check work of Query REST commands without cache name
     *
     * Steps:
     * 1) Start test node
     * 2) From REST client call all Query commands
     * 3) All requests except requests for FETCH_SQL_QUERY and CLOSE_SQL_QUERY should fail with
     * "Failed to find mandatory parameter in request: cacheName" exception
     *
     * @throws Exception If failed.
     */
    @Test
    public void testQueryCommandsWithoutCacheName() throws Exception {
        final String ERROR_MSG = "Failed to find mandatory parameter in request: cacheName";

        EnumSet<GridRestCommand> qryCommands = EnumSet.of(GridRestCommand.EXECUTE_SQL_QUERY,
            GridRestCommand.EXECUTE_SQL_FIELDS_QUERY,
            GridRestCommand.EXECUTE_SCAN_QUERY,
            GridRestCommand.FETCH_SQL_QUERY,
            GridRestCommand.CLOSE_SQL_QUERY);

        for (GridRestCommand command : qryCommands) {
            String ret = content(null, command,
                "pageSize", "1",
                "qry", URLEncoder.encode("SELECT * FROM table", CHARSET));

            JsonNode err = jsonTaskErrorResult(ret);
            assertFalse(err.isNull());

            if (command == GridRestCommand.EXECUTE_SQL_QUERY ||
                command == GridRestCommand.EXECUTE_SCAN_QUERY ||
                command == GridRestCommand.EXECUTE_SQL_FIELDS_QUERY)
                assertTrue(err.textValue().contains(ERROR_MSG));
            else
                assertFalse(err.textValue().contains(ERROR_MSG));
        }
    }

    /**
     * @return True if any query cursor is available.
     */
    private boolean queryCursorFound() {
        boolean found = false;

        for (int i = 0; i < gridCount(); ++i) {
            Map<GridRestCommand, GridRestCommandHandler> handlers =
                GridTestUtils.getFieldValue(grid(i).context().rest(), "handlers");

            GridRestCommandHandler qryHnd = handlers.get(GridRestCommand.CLOSE_SQL_QUERY);

            ConcurrentHashMap<Long, Iterator> its = GridTestUtils.getFieldValue(qryHnd, "qryCurs");

            found |= !its.isEmpty();
        }

        return found;
    }

    /**
     * Init cache.
     */
    protected void initCache() {
        CacheConfiguration typedCache = new CacheConfiguration<>("test_typed_access");
        ignite(0).getOrCreateCache(typedCache);

        CacheConfiguration<Integer, Organization> orgCacheCfg = new CacheConfiguration<>("organization");

        orgCacheCfg.setIndexedTypes(Integer.class, Organization.class);

        IgniteCache<Integer, Organization> orgCache = ignite(0).getOrCreateCache(orgCacheCfg);

        orgCache.clear();

        Organization o1 = new Organization(1, "o1");
        Organization o2 = new Organization(2, "o2");

        orgCache.put(1, o1);
        orgCache.put(2, o2);

        CacheConfiguration<Integer, Person> personCacheCfg = new CacheConfiguration<>("person");

        personCacheCfg.setIndexedTypes(Integer.class, Person.class);
        personCacheCfg.setSqlFunctionClasses(JdbcThinStatementCancelSelfTest.TestSQLFunctions.class);

        IgniteCache<Integer, Person> personCache = grid(0).getOrCreateCache(personCacheCfg);

        personCache.clear();

        Person p1 = new Person(1, "John", "Doe", 2000);
        Person p2 = new Person(1, "Jane", "Doe", 1000);
        Person p3 = new Person(2, "John", "Smith", 1000);
        Person p4 = new Person(2, "Jane", "Smith", 2000);

        personCache.put(p1.getId(), p1);
        personCache.put(p2.getId(), p2);
        personCache.put(p3.getId(), p3);
        personCache.put(p4.getId(), p4);

        SqlQuery<Integer, Person> qry = new SqlQuery<>(Person.class, "salary > ? and salary <= ?");

        qry.setArgs(1000, 2000);

        assertEquals(2, personCache.query(qry).getAll().size());
    }

    /**
     * Organization class.
     */
    public static class Organization implements Serializable {
        /** Organization ID (indexed). */
        @QuerySqlField(index = true)
        private Integer id;

        /** First name (not-indexed). */
        @QuerySqlField(index = true)
        private String name;

        /**
         * @param id Id.
         * @param name Name.
         */
        Organization(Integer id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * @return Id.
         */
        public Integer getId() {
            return id;
        }

        /**
         * @return Name.
         */
        public String getName() {
            return name;
        }
    }

    /**
     * Test class that could have circular references.
     */
    public static class CircularRef implements Serializable {
        /** */
        private int id;

        /** */
        private String name;

        /** */
        private CircularRef ref;

        /**
         * @param id ID.
         * @param name Name.
         */
        CircularRef(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * @return ID.
         */
        public int id() {
            return id;
        }

        /**
         * @return Name.
         */
        public String name() {
            return name;
        }

        /**
         * @return Reference to other object.
         */
        public CircularRef ref() {
            return ref;
        }

        /**
         * @param ref Reference to other object.
         */
        public void ref(CircularRef ref) {
            this.ref = ref;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            SB sb = new SB();

            sb.a('{')
                .a('"').a("id").a('"').a(':').a(id).a(',')
                .a('"').a("name").a('"').a(':').a('"').a(name).a('"').a(',')
                .a('"').a("ref").a('"').a(':').a(ref)
                .a('}');

            return sb.toString();
        }
    }

    /**
     * Person class.
     */
    public static class Person implements Serializable {
        /** Person ID (indexed). */
        @QuerySqlField(index = true)
        private Integer id;

        /** Organization id. */
        @QuerySqlField(index = true)
        private Integer orgId;

        /** First name (not-indexed). */
        @QuerySqlField
        private String firstName;

        /** Last name (not indexed). */
        @QuerySqlField
        private String lastName;

        /** Salary (indexed). */
        @QuerySqlField(index = true)
        private double salary;

        /**
         * @param firstName First name.
         * @param lastName Last name.
         * @param salary Salary.
         */
        Person(Integer orgId, String firstName, String lastName, double salary) {
            id = KEY_GEN.getAndIncrement();

            this.orgId = orgId;
            this.firstName = firstName;
            this.lastName = lastName;
            this.salary = salary;
        }

        /**
         * @return Organization ID.
         */
        public Integer getOrganizationId() {
            return orgId;
        }

        /**
         * @return First name.
         */
        public String getFirstName() {
            return firstName;
        }

        /**
         * @return Last name.
         */
        public String getLastName() {
            return lastName;
        }

        /**
         * @return Salary.
         */
        public double getSalary() {
            return salary;
        }

        /**
         * @return Id.
         */
        public Integer getId() {
            return id;
        }
    }

    /**
     * Class of internal object for conposed key type.
     */
    public static class CompositeKeyInternal implements Serializable {
        /** Object ID (indexed). */
        @QuerySqlField(index = true)
        private Integer id;

        /**  */
        CompositeKeyInternal() {
            id = KEY_GEN.getAndIncrement();
        }

        /**
         * @return Id.
         */
        public Integer getId() {
            return id;
        }
    }

    /**
     * Class of extermal object for composite key type.
     */
    public static class CompositeKeyExternal implements Serializable {
        /** Object ID (indexed). */
        @QuerySqlField(index = true)
        private Integer id;

        /** Internal object. */
        private CompositeKeyInternal internal;

        /**  */
        CompositeKeyExternal(CompositeKeyInternal internal) {
            id = KEY_GEN.getAndIncrement();

            this.internal = internal;
        }

        /**
         * @return Id.
         */
        public Integer getId() {
            return id;
        }

        /**
         * @return Internal object.
         */
        public CompositeKeyInternal getInternal() {
            return internal;
        }
    }

    /**
     * Test filter for scan query.
     */
    public static class ScanFilter implements IgniteBiPredicate<Integer, Person> {
        /** {@inheritDoc} */
        @Override public boolean apply(Integer integer, Person person) {
            return person.salary > 1000;
        }
    }

    /** Filter by consistent id. */
    private static class NodeConsistentIdFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private final Object consistentId;

        /**
         * @param consistentId Consistent id where cache should be started.
         */
        NodeConsistentIdFilter(Object consistentId) {
            this.consistentId = consistentId;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode n) {
            return n.consistentId().equals(consistentId);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        DataRegionConfiguration drCfg = new DataRegionConfiguration();
        drCfg.setName("testDataRegion");
        drCfg.setMaxSize(100L * 1024 * 1024);

        if (memoryMetricsEnabled)
            drCfg.setPersistenceEnabled(true);

        dsCfg.setDefaultDataRegionConfiguration(drCfg);

        if (memoryMetricsEnabled)
            dsCfg.setMetricsEnabled(true).setWalMode(NONE);
        else
            dsCfg.setMetricsEnabled(false);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /**
     * Test if current cluster state equals expected. Checks both REST commands: {@link GridRestCommand#CLUSTER_STATE}
     * and {@link GridRestCommand#CLUSTER_CURRENT_STATE}.
     *
     * @param expState Expected state.
     * @throws Exception If failed.
     */
    private void checkState(ClusterState expState) throws Exception {
        assertClusterState(expState);
        assertClusterState(ClusterState.active(expState));
    }

    /**
     * Test if current cluster state equals expected.
     *
     * @param expState Expected state.
     * @throws Exception If failed.
     */
    private void assertClusterState(ClusterState expState) throws Exception {
        String ret = content(null, GridRestCommand.CLUSTER_STATE);

        info("Cluster state: " + ret);
        JsonNode res = validateJsonResponse(ret);

        assertEquals(ret, expState.toString(), res.asText());
        assertEquals(ret, expState, grid(0).cluster().state());
    }

    /**
     * Test if current cluster state equals expected.
     *
     * @param exp Expected state.
     * @throws Exception If failed.
     */
    private void assertClusterState(boolean exp) throws Exception {
        String ret = content(null, GridRestCommand.CLUSTER_CURRENT_STATE);

        info("Cluster state: " + ret);
        JsonNode res = validateJsonResponse(ret);

        assertEquals(ret, exp, res.asBoolean());
        assertEquals(ret, exp, grid(0).cluster().active());
    }

    /**
     * Checks that cluster has state {@code curState}, tries to change state to {@code newState} by {@code cmd} command
     * and checks state after change.
     *
     * @param cmd State change command.
     * @param curState Expected cluster state before change.
     * @param newState New cluster state after change.
     * @param force Add force flag to the command parameters.
     * @param success Excepted result of cluster state change.
     * @throws Exception If failed.
     */
    private void changeClusterState(
        GridRestCommand cmd,
        ClusterState curState,
        ClusterState newState,
        boolean force,
        boolean success
    ) throws Exception {
        checkState(curState);

        Map<String, String> params = new LinkedHashMap<>();

        params.put("cmd", cmd.key());

        if (force)
            params.put("force", "true");

        if (cmd == CLUSTER_SET_STATE)
            params.put("state", newState.name());

        String ret = content(params);

        JsonNode res = validateJsonResponse(ret, !success);

        assertFalse(res.isNull());

        if (success)
            assertTrue(res.asText().startsWith(cmd.key()));
        else
            assertTrue(res.asText().contains(DATA_LOST_ON_DEACTIVATION_WARNING));

        checkState(success ? newState : curState);
    }
}
