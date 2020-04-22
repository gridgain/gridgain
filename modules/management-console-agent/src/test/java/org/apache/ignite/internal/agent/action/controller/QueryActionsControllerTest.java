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

package org.apache.ignite.internal.agent.action.controller;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.agent.dto.action.JobResponse;
import org.apache.ignite.internal.agent.dto.action.Request;
import org.apache.ignite.internal.agent.dto.action.TaskResponse;
import org.apache.ignite.internal.agent.dto.action.query.NextPageQueryArgument;
import org.apache.ignite.internal.agent.dto.action.query.QueryArgument;
import org.apache.ignite.internal.agent.dto.action.query.ScanQueryArgument;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Before;
import org.junit.Test;

import static java.util.Collections.singleton;
import static org.apache.ignite.internal.agent.dto.action.Status.COMPLETED;
import static org.apache.ignite.internal.agent.dto.action.Status.FAILED;
import static org.apache.ignite.internal.agent.dto.action.Status.RUNNING;

/**
 * Query actions controller test.
 */
public class QueryActionsControllerTest extends AbstractActionControllerTest {
    /** {@inheritDoc} */
    @Before
    @Override public void startup() throws Exception {
        startup0(3);
    }

    /**
     * Should execute query.
     */
    @Test
    public void shouldExecuteQuery() {
        Request req = new Request()
            .setAction("QueryActions.executeSqlQuery")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setId(UUID.randomUUID())
            .setArgument(
                new QueryArgument()
                    .setQueryId("qry")
                    .setQueryText(getCreateQuery() + getInsertQuery(1, 2) + getSelectQuery())
                    .setPageSize(10)
            );

        executeAction(req, (res) -> {
            JobResponse r = F.first(res);

            if (r != null && r.getStatus() == COMPLETED) {
                DocumentContext ctx = parse(r.getResult());

                JSONArray resArr = ctx.read("$[*]]");

                int id = ctx.read("$[2].rows[0][0]");

                int val = ctx.read("$[2].rows[0][1]");

                return resArr.size() == 3 && id == 1 && val == 2;
            }

            return false;
        });
    }

    /**
     * Should execute query with parameters.
     */
    @Test
    public void shouldExecuteQueryWithParameters() {
        Request req = new Request()
            .setAction("QueryActions.executeSqlQuery")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setId(UUID.randomUUID())
            .setArgument(
                new QueryArgument()
                    .setQueryId("qry")
                    .setQueryText(getCreateQuery() + getInsertQuery(1, 2) + getInsertQuery(2, 3) + getSelectQueryWithParameter())
                    .setPageSize(10)
                    .setParameters(new Object[]{1})
            );

        executeAction(req, (res) -> {
            JobResponse r = F.first(res);

            if (r != null && r.getStatus() == COMPLETED) {
                DocumentContext ctx = parse(r.getResult());

                JSONArray resArr = ctx.read("$[*]]");

                JSONArray arr = ctx.read("$[3].rows[*]");

                int id = ctx.read("$[3].rows[0][0]");

                int val = ctx.read("$[3].rows[0][1]");

                return resArr.size() == 4 && arr.size() == 1 && id == 1 && val == 2;
            }

            return false;
        });
    }

    /**
     * Should execute query with specified default schema.
     */
    @Test
    public void shouldExecuteQueryWithDefaultSchema() {
        final String testSchema = "MC_AGENT_TEST_SCHEMA";
        final String testTable = "MC_AGENT_TEST_TABLE";
        final String fullTestTable = String.format("%s.%s", testSchema, testTable);
        final String testClassName = "test.data." + testTable;

        CacheConfiguration<Integer, Object> ccfg = defaultCacheConfiguration();
        ccfg.setSqlSchema(testSchema);

        ArrayList<QueryEntity> qryEntities = new ArrayList<>();

        QueryEntity qryEntity = new QueryEntity();

        qryEntity.setKeyType("java.lang.Integer");
        qryEntity.setValueType(testClassName);
        qryEntity.setTableName(testTable);
        qryEntity.setKeyFieldName("id");

        HashSet<String> keyFields = new HashSet<>();

        keyFields.add("id");

        qryEntity.setKeyFields(keyFields);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("id", "java.lang.Integer");
        fields.put("Name", "java.lang.String");

        qryEntity.setFields(fields);

        qryEntities.add(qryEntity);

        ccfg.setQueryEntities(qryEntities);

        IgniteCache<Integer, Object> cache = cluster.ignite().createCache(ccfg);
        Integer testId = 1;

        BinaryObjectBuilder b = ignite(0).binary().builder(testClassName);
        b.setField("id", testId);
        b.setField("name", "name");

        cache.put(testId, b.build());

        Request req = new Request()
            .setAction("QueryActions.executeSqlQuery")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setId(UUID.randomUUID())
            .setArgument(
                new QueryArgument()
                    .setQueryId("qry")
                    .setDefaultSchema(testSchema)
                    .setQueryText(getSelectQuery(fullTestTable) +
                        getSelectQuery(testTable))
                    .setPageSize(10)
            );

        executeAction(req, (res) -> {
            JobResponse r = F.first(res);

            if (r != null && r.getStatus() == COMPLETED) {
                DocumentContext ctx = parse(r.getResult());

                JSONArray resArr = ctx.read("$[*]]");

                Integer id1 = ctx.read("$[0].rows[0][0]");
                String val1 = ctx.read("$[0].rows[0][1]");

                Integer id2 = ctx.read("$[1].rows[0][0]");
                String val2 = ctx.read("$[1].rows[0][1]");

                return resArr.size() == 2 && id1 == 1 && "name".equals(val1)
                    && id1.equals(id2) && val1.equals(val2);
            }

            return false;
        });
    }

    /**
     * Should get next page.
     */
    @Test
    public void shouldGetNextPage() {
        final AtomicReference<String> cursorId = new AtomicReference<>();

        Request req = new Request()
            .setAction("QueryActions.executeSqlQuery")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setId(UUID.randomUUID())
            .setArgument(
                new QueryArgument()
                    .setQueryId("qry")
                    .setQueryText(getCreateQuery() + getInsertQuery(1, 2) + getInsertQuery(2, 3) + getSelectQuery())
                    .setPageSize(1)
            );

        executeAction(req, (res) -> {
            JobResponse r = F.first(res);

            if (r != null && r.getStatus() == COMPLETED) {
                DocumentContext ctx = parse(r.getResult());

                JSONArray resArr = ctx.read("$[*]]");

                JSONArray arr = ctx.read("$[3].rows[*]");

                boolean hasMore = ctx.read("$[3].hasMore");

                cursorId.set(ctx.read("$[3].cursorId"));

                return resArr.size() == 4 && hasMore && arr.size() == 1;
            }

            return false;
        });

        Request nextPageReq = new Request()
            .setAction("QueryActions.nextPage")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setId(UUID.randomUUID())
            .setArgument(
                new NextPageQueryArgument().setQueryId("qry").setCursorId(cursorId.get()).setPageSize(1)
            );

        executeAction(nextPageReq, (res) -> {
            JobResponse r = F.first(res);

            if (r != null && r.getStatus() == COMPLETED) {
                DocumentContext ctx = parse(r.getResult());

                JSONArray arr = ctx.read("$.rows[*]");

                boolean hasMore = ctx.read("$.hasMore");

                int id = ctx.read("$.rows[0][0]");

                int val = ctx.read("$.rows[0][1]");

                return arr.size() == 1 && !hasMore && id == 2 && val == 3;
            }

            return false;
        });
    }

    /**
     * Should cancel query before getting next page.
     */
    @Test
    public void shouldCancelQueryAndCleanup() {
        final AtomicReference<String> cursorId = new AtomicReference<>();
        Request req = new Request()
            .setAction("QueryActions.executeSqlQuery")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setId(UUID.randomUUID())
            .setArgument(
                new QueryArgument()
                    .setQueryId("qry")
                    .setQueryText(getCreateQuery() + getInsertQuery(1, 2) + getInsertQuery(2, 3) + getSelectQuery())
                    .setPageSize(1)
            );

        executeAction(req, (res) -> {
            JobResponse r = F.first(res);

            if (r != null && r.getStatus() == COMPLETED) {
                DocumentContext ctx = parse(r.getResult());

                cursorId.set(ctx.read("$[3].cursorId"));

                return true;
            }

            return false;
        });

        Request cancelReq = new Request()
            .setAction("QueryActions.cancel")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setId(UUID.randomUUID())
            .setArgument("qry");

        executeAction(cancelReq, (res) -> {
            JobResponse r = F.first(res);

            return r != null && r.getStatus() == COMPLETED;
        });

        Request nextPageReq = new Request()
            .setAction("QueryActions.nextPage")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setId(UUID.randomUUID())
            .setArgument(
                new NextPageQueryArgument().setQueryId("qry").setCursorId(cursorId.get()).setPageSize(1)
            );

        executeAction(nextPageReq, (res) -> {
            JobResponse r = F.first(res);

            return r != null && r.getStatus() == FAILED;
        });
    }

    /**
     * Should cancel long running query.
     */
    @Test
    public void shouldCancelLongQuery() {
        StringBuilder qryText = new StringBuilder(getCreateQuery());

        for (int i = 0; i <= 1_000; i++)
            qryText.append(getInsertQuery(i, i + 1));

        qryText.append(getSelectQuery());

        Request req = new Request()
            .setAction("QueryActions.executeSqlQuery")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setId(UUID.randomUUID())
            .setArgument(
                new QueryArgument()
                    .setQueryId("qry")
                    .setQueryText(qryText.toString())
                    .setPageSize(1_000)
            );

        executeAction(req, (res) -> {
            TaskResponse taskRes = taskResult(req.getId());

            return taskRes != null && taskRes.getStatus() == RUNNING;
        });

        Request cancelReq = new Request()
            .setAction("QueryActions.cancel")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setId(UUID.randomUUID())
            .setArgument("qry");

        executeAction(cancelReq, (res) -> {
            JobResponse r = F.first(res);

            return r != null && r.getStatus() == COMPLETED;
        });

        assertWithPoll(
            () -> {
                JobResponse res = jobResult(req.getId());
                return res != null && res.getStatus() == FAILED;
            }
        );
    }

    /**
     * 1. Execute a query action with sleep function.
     * 2. Send kill query action with global query id for a query from 1.
     * 3. Assert that action was completed and no one running queries exists in a cluster.
     */
    @Test
    public void shouldKillRunningQuery() {
        GridKernalContext ctx = ((IgniteEx) cluster.ignite()).context();

        cluster.ignite().createCache(
            new CacheConfiguration<>("TestCache")
                .setIndexedTypes(Integer.class, String.class)
                .setSqlFunctionClasses(GridTestUtils.SqlTestFunctions.class)
        );

        GridTestUtils.SqlTestFunctions.sleepMs = 20_000;

        Request req = new Request()
            .setAction("QueryActions.executeSqlQuery")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setId(UUID.randomUUID())
            .setArgument(
                new QueryArgument()
                    .setQueryId(UUID.randomUUID().toString())
                    .setQueryText("SELECT count(*), sleep() AS SLEEP FROM \"TestCache\".STRING")
                    .setPageSize(1)
                    .setDefaultSchema("TestCache")
            );

        executeAction(req, (res) -> {
            TaskResponse taskRes = taskResult(req.getId());

            return taskRes != null && taskRes.getStatus() == RUNNING;
        });

        GridRunningQueryInfo runQry = F.first(ctx.query().runningQueries(-1));

        Request killReq = new Request()
            .setAction("QueryActions.kill")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setId(UUID.randomUUID())
            .setArgument(runQry.globalQueryId());

        executeAction(killReq, (res) -> {
            TaskResponse taskRes = taskResult(killReq.getId());

            if (taskRes != null && taskRes.getStatus() == COMPLETED) {
                Collection<GridRunningQueryInfo> infos = ctx.query().runningQueries(-1);

                return infos.isEmpty();
            }

            return false;
        });
    }

    /**
     * 1. Execute a query with sleep function in another thread.
     * 2. Send kill query action with global query id for a query from 1.
     * 3. Assert that action was completed and no one running queries exists in a cluster.
     */
    @Test
    public void shouldKillRunningQueryWhichRunByDirectApi() {
        GridKernalContext ctx = ((IgniteEx) cluster.ignite()).context();

        cluster.ignite().createCache(
            new CacheConfiguration<>("TestCache")
                .setIndexedTypes(Integer.class, String.class)
                .setSqlFunctionClasses(GridTestUtils.SqlTestFunctions.class)
        );

        GridTestUtils.SqlTestFunctions.sleepMs = 20_000;

        CompletableFuture.runAsync(() -> {
            SqlFieldsQuery qry = new SqlFieldsQuery("SELECT count(*), sleep() AS SLEEP FROM \"TestCache\".STRING");
            qry.setSchema("TestCache");

            try (FieldsQueryCursor<List<?>> cursor = ctx.query().querySqlFields(qry, true)) {
                cursor.iterator().next();
            }
        });

        assertWithPoll(() -> !ctx.query().runningQueries(-1).isEmpty());

        GridRunningQueryInfo runQry = F.first(ctx.query().runningQueries(-1));

        Request killReq = new Request()
            .setAction("QueryActions.kill")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setId(UUID.randomUUID())
            .setArgument(runQry.globalQueryId());

        executeAction(killReq, (res) -> {
            TaskResponse taskRes = taskResult(killReq.getId());

            if (taskRes != null && taskRes.getStatus() == COMPLETED) {
                Collection<GridRunningQueryInfo> infos = ctx.query().runningQueries(-1);

                return infos.isEmpty();
            }

            return false;
        });
    }

    /**
     * Should execute scan query.
     */
    @Test
    public void shouldExecuteScanQuery() {
        IgniteCache<Object, Object> cache = cluster.ignite().createCache("test_cache");
        cache.put("key_1", "value_1");

        Request req = new Request()
            .setAction("QueryActions.executeScanQuery")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setId(UUID.randomUUID())
            .setArgument(
                new ScanQueryArgument()
                    .setCacheName("test_cache")
                    .setQueryId("qry")
                    .setPageSize(1)
            );

        executeAction(req, (res) -> {
            JobResponse r = F.first(res);

            if (r != null && r.getStatus() == COMPLETED) {
                DocumentContext ctx = parse(r.getResult());

                JSONArray resArr = ctx.read("$[*]]");

                JSONArray arr = ctx.read("$[0].rows[*]");

                boolean hasMore = ctx.read("$[0].hasMore");

                String id = ctx.read("$[0].rows[0][1]");

                String val = ctx.read("$[0].rows[0][3]");

                return resArr.size() == 1 && arr.size() == 1 && !hasMore && "key_1".equals(id) && "value_1".equals(val);
            }

            return false;
        });
    }

    /**
     * Add testcase description after test case approve.
     */
    @Test
    public void shouldReturnRunningQueriesFromAllNodes() {
        cluster.ignite().createCache(
            new CacheConfiguration<>("TestCache")
                .setIndexedTypes(Integer.class, String.class)
                .setSqlFunctionClasses(GridTestUtils.SqlTestFunctions.class)
        );

        GridTestUtils.SqlTestFunctions.sleepMs = 5_000;

        for (UUID nid : allNodeIds) {
            Request req = new Request()
                .setAction("QueryActions.executeSqlQuery")
                .setNodeIds(singleton(nid))
                .setId(UUID.randomUUID())
                .setArgument(
                    new QueryArgument()
                        .setQueryId(UUID.randomUUID().toString())
                        .setQueryText("SELECT count(*), sleep() AS \"" + nid + "\" FROM \"TestCache\".STRING")
                        .setPageSize(1)
                        .setDefaultSchema("TestCache")
                );

            executeAction(req, (res) -> {
                TaskResponse taskRes = taskResult(req.getId());

                return taskRes != null && taskRes.getStatus() == RUNNING;
            });
        }

        Request req = new Request()
            .setAction("QueryActions.runningQueries")
            .setId(UUID.randomUUID())
            .setArgument(1);

        executeAction(req, (res) -> {
            TaskResponse taskRes = taskResult(req.getId());

            if (taskRes != null && taskRes.getStatus() == COMPLETED && taskRes.getJobCount() == 3) {
                boolean isAllResponsesNotEmpty = res.stream()
                    .noneMatch(r -> ((Collection<Map<String, String>>)r.getResult()).isEmpty());

                long queriesCnt = res.stream()
                    .flatMap(r -> ((Collection<Map<String, String>>) r.getResult()).stream())
                    .map(m -> m.get("query"))
                    .distinct()
                    .count();

                return isAllResponsesNotEmpty && queriesCnt == 3;
            }

            return false;
        });
    }

    /**
     * Add testcase description after test case approve.
     */
    @Test
    public void shouldReturnRunningQueriesFromCoordinatorNode() {
        cluster.ignite().createCache(
            new CacheConfiguration<>("TestCache")
                .setIndexedTypes(Integer.class, String.class)
                .setSqlFunctionClasses(GridTestUtils.SqlTestFunctions.class)
        );

        GridTestUtils.SqlTestFunctions.sleepMs = 5_000;

        Request qryReq = new Request()
            .setAction("QueryActions.executeSqlQuery")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setId(UUID.randomUUID())
            .setArgument(
                new QueryArgument()
                    .setQueryId(UUID.randomUUID().toString())
                    .setQueryText("SELECT count(*), sleep() FROM \"TestCache\".STRING")
                    .setPageSize(1)
                    .setDefaultSchema("TestCache")
            );

        executeAction(qryReq, (res) -> {
            TaskResponse taskRes = taskResult(qryReq.getId());

            return taskRes != null && taskRes.getStatus() == RUNNING;
        });

        Request req = new Request()
            .setAction("QueryActions.runningQueries")
            .setId(UUID.randomUUID())
            .setArgument(1);

        executeAction(req, (res) -> {
            TaskResponse taskRes = taskResult(req.getId());

            if (taskRes != null && taskRes.getStatus() == COMPLETED && taskRes.getJobCount() == 3) {

                boolean hasCorrectRes = res.stream()
                    .filter(r -> r.getNodeConsistentId().equals(cluster.localNode().consistentId().toString()))
                    .anyMatch(r -> !((Collection<Map<String, String>>)r.getResult()).isEmpty());

                boolean isOtherResponsesEmpty = res.stream()
                    .filter(r -> !r.getNodeConsistentId().equals(cluster.localNode().consistentId().toString()))
                    .allMatch(r -> ((Collection<Map<String, String>>)r.getResult()).isEmpty());

                return hasCorrectRes && isOtherResponsesEmpty;
            }

            return false;
        });
    }

    /**
     * 1. Create and fill the cache with Integer key and String value.
     * 2. Execute sql query action: SELECT count(*), sleep() FROM "TestCache".STRING and wait completion.
     * 3. Execute scan query action for TestCache and wait completion.
     * 4. Send the "QueryActions.history" action with "since" argument equals to 1.
     * 5. Assert that query history contains 2 queries.
     * 6. Assert that SCAN and SQL_FIELDS query present in query history.
     * 7. Assert that history queries contains a correct queries text presentation.
     */
    @Test
    public void shouldReturnQueryHistory() {
        IgniteCache<Object, Object> cache = cluster.ignite().createCache(
            new CacheConfiguration<>("TestCache")
                .setQueryDetailMetricsSize(1)
                .setIndexedTypes(Integer.class, String.class)
                .setSqlFunctionClasses(GridTestUtils.SqlTestFunctions.class)
        );

        for (Integer i = 0; i < 1_000; i++)
            cache.put(i, i.toString());

        GridTestUtils.SqlTestFunctions.sleepMs = 5_000;

        Request sqlQryReq = new Request()
            .setAction("QueryActions.executeSqlQuery")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setId(UUID.randomUUID())
            .setArgument(
                new QueryArgument()
                    .setQueryId(UUID.randomUUID().toString())
                    .setQueryText("SELECT count(*), sleep() FROM \"TestCache\".STRING")
                    .setPageSize(1)
                    .setDefaultSchema("TestCache")
            );

        executeAction(sqlQryReq, (res) -> {
            TaskResponse taskRes = taskResult(sqlQryReq.getId());

            return taskRes != null && taskRes.getStatus() == COMPLETED;
        });

        Request scanQryReq = new Request()
            .setAction("QueryActions.executeScanQuery")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setId(UUID.randomUUID())
            .setArgument(
                new ScanQueryArgument()
                    .setQueryId(UUID.randomUUID().toString())
                    .setCacheName("TestCache")
                    .setPageSize(1_000)
            );

        executeAction(scanQryReq, (res) -> {
            TaskResponse taskRes = taskResult(scanQryReq.getId());

            return taskRes != null && taskRes.getStatus() == COMPLETED;
        });

        Request req = new Request()
            .setAction("QueryActions.history")
            .setId(UUID.randomUUID())
            .setArgument(1)
            .setNodeIds(singleton(cluster.localNode().id()));

        executeAction(req, (res) -> {
            JobResponse r = F.first(res);

            TaskResponse taskRes = taskResult(req.getId());

            if (taskRes != null && taskRes.getStatus() == COMPLETED && taskRes.getJobCount() == 1) {
                DocumentContext ctx = parse(r.getResult());

                JSONArray results = ctx.read("$[*]");
                JSONArray qryTypes = ctx.read("$[*].queryType");
                JSONArray queries = ctx.read("$[*].query");

                return results.size() == 2 &&
                    qryTypes.stream().allMatch(t -> t.equals("SCAN") || t.equals("SQL_FIELDS")) &&
                    queries.stream().allMatch(
                        q -> q.equals("SELECT count(*), sleep() FROM \"TestCache\".STRING") || q.equals("TestCache")
                    );
            }

            return false;
        });
    }

    /**
     * 1. Create and fill the cache with Integer key and String value.
     * 2. Execute sql query: SELECT count(*), sleep() FROM "TestCache".STRING and wait completion.
     * 3. Execute scan query for TestCache and wait completion.
     * 4. Send the "QueryActions.history" action with "since" argument equals to 1.
     * 5. Assert that query history contains 2 queries.
     * 6. Assert that SCAN and SQL_FIELDS query present in query history.
     * 7. Assert that history queries contains a correct queries text presentation.
     */
    @Test
    public void shouldReturnQueryHistoryForQueriesInvokedByDirectApi() {
        GridQueryProcessor qryProc = ((IgniteEx) cluster.ignite()).context().query();

        IgniteCache<Object, Object> cache = cluster.ignite().createCache(
            new CacheConfiguration<>("TestCache")
                .setQueryDetailMetricsSize(1)
                .setIndexedTypes(Integer.class, String.class)
                .setSqlFunctionClasses(GridTestUtils.SqlTestFunctions.class)
        );

        for (Integer i = 0; i < 1_000; i++)
            cache.put(i, i.toString());

        GridTestUtils.SqlTestFunctions.sleepMs = 5_000;

        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT count(*), sleep() FROM \"TestCache\".STRING");
        qry.setSchema("TestCache");

        try (FieldsQueryCursor<List<?>> lists = qryProc.querySqlFields(qry, true)) {
            lists.getAll();
        }

        cache.withKeepBinary().query(new ScanQuery<>()).getAll();

        Request req = new Request()
            .setAction("QueryActions.history")
            .setId(UUID.randomUUID())
            .setArgument(1)
            .setNodeIds(singleton(cluster.localNode().id()));

        executeAction(req, (res) -> {
            JobResponse r = F.first(res);

            TaskResponse taskRes = taskResult(req.getId());

            if (taskRes != null && taskRes.getStatus() == COMPLETED && taskRes.getJobCount() == 1) {
                DocumentContext ctx = parse(r.getResult());

                JSONArray results = ctx.read("$[*]");
                JSONArray qryTypes = ctx.read("$[*].queryType");
                JSONArray queries = ctx.read("$[*].query");

                return results.size() == 2 &&
                    qryTypes.stream().allMatch(t -> t.equals("SCAN") || t.equals("SQL_FIELDS")) &&
                    queries.stream().allMatch(
                        q -> q.equals("SELECT count(*), sleep() FROM \"TestCache\".STRING") || q.equals("TestCache")
                    );
            }

            return false;
        });
    }

    /**
     * 1. Execute sql query action: SELECT count(*), can_fail() FROM "TestCache".STRING and wait completion.
     * 2. Send the "QueryActions.history" action with "since" argument equals to 1.
     * 3. Assert that query history contains 1 query.
     * 4. Assert that history query contains a correct query text presentation.
     */
    @Test
    public void shouldReturnQueryHistoryWithFailedQuery() {
        IgniteCache<Object, Object> cache = cluster.ignite().createCache(
            new CacheConfiguration<>("TestCache")
                .setQueryDetailMetricsSize(1)
                .setIndexedTypes(Integer.class, String.class)
                .setSqlFunctionClasses(GridTestUtils.SqlTestFunctions.class)
        );

        for (Integer i = 0; i < 1_000; i++)
            cache.put(i, i.toString());

        GridTestUtils.SqlTestFunctions.fail = true;

        Request sqlQryReq = new Request()
            .setAction("QueryActions.executeSqlQuery")
            .setNodeIds(singleton(cluster.localNode().id()))
            .setId(UUID.randomUUID())
            .setArgument(
                new QueryArgument()
                    .setQueryId(UUID.randomUUID().toString())
                    .setQueryText("SELECT count(*), can_fail() FROM \"TestCache\".STRING")
                    .setPageSize(1)
                    .setDefaultSchema("TestCache")
            );

        executeAction(sqlQryReq, (res) -> {
            TaskResponse taskRes = taskResult(sqlQryReq.getId());

            return taskRes != null && taskRes.getStatus() == FAILED;
        });

        GridTestUtils.SqlTestFunctions.fail = false;

        Request req = new Request()
            .setAction("QueryActions.history")
            .setId(UUID.randomUUID())
            .setArgument(1)
            .setNodeIds(singleton(cluster.localNode().id()));

        executeAction(req, (res) -> {
            JobResponse r = F.first(res);

            TaskResponse taskRes = taskResult(req.getId());

            if (taskRes != null && taskRes.getStatus() == COMPLETED && taskRes.getJobCount() == 1) {
                DocumentContext ctx = parse(r.getResult());

                JSONArray results = ctx.read("$[*]");
                JSONArray queries = ctx.read("$[*].query");

                return results.size() == 1 &&
                    queries.stream().allMatch(
                        q -> q.equals("SELECT count(*), can_fail() FROM \"TestCache\".STRING")
                    );
            }

            return false;
        });
    }

    /**
     * @return Create table query string.
     */
    private String getCreateQuery() {
        return getCreateQuery("mc_agent_test_table", null);
    }

    /**
     * @return Create table query string with special table name.
     */
    private String getCreateQuery(String name, String additionaParams) {
        return "CREATE TABLE " + name + " (id int, value int, PRIMARY KEY (id)) " +
            (additionaParams != null ? "WITH \"" + additionaParams + "\"": "") + ";";
    }

    /**
     * @param id  Id.
     * @param val Value.
     * @return Insert query string.
     */
    private String getInsertQuery(int id, int val) {
        return getInsertQuery("mc_agent_test_table", id, val);
    }

    /**
     * @param name Table name.
     * @param id  ID.
     * @param val Value.
     * @return Insert query string.
     */
    private String getInsertQuery(String name, int id, int val) {
        return String.format("INSERT INTO " + name + " VALUES(%s, %s);", id, val);
    }

    /**
     * @return Select query string.
     */
    private String getSelectQuery() {
        return getSelectQuery("mc_agent_test_table");
    }

    /**
     * @param name Name.
     * @return Select query string.
     */
    private String getSelectQuery(String name) {
        return "SELECT * FROM " + name + " ORDER BY ID;";
    }

    /**
     * @return Select query string.
     */
    private String getSelectQueryWithParameter() {
        return "SELECT * FROM mc_agent_test_table WHERE id = ?;";
    }

    /**
     * @param obj Object.
     */
    private DocumentContext parse(Object obj) {
        try {
            return JsonPath.parse(mapper.writeValueAsString(obj));
        }
        catch (JsonProcessingException e) {
            throw new IgniteException(e);
        }
    }
}
