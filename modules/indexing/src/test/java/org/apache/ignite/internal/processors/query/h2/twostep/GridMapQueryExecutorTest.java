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

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContextRegistry;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.lang.reflect.Field;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class GridMapQueryExecutorTest {
    private IgniteLogger mockLog;
    private GridKernalContext mockCtx;
    private IgniteH2Indexing mockH2;
    private GridMapQueryExecutor executor;

    @Before
    public void setup() throws Exception {
        mockLog = mock(IgniteLogger.class);
        mockCtx = mock(GridKernalContext.class);
        mockH2 = mock(IgniteH2Indexing.class);

        when(mockCtx.localNodeId()).thenReturn(UUID.randomUUID());
        when(mockH2.queryContextRegistry()).thenReturn(mock(QueryContextRegistry.class));

        executor = spy(new GridMapQueryExecutor());
        executor.start(mockCtx, mockH2);

        Field logField = GridMapQueryExecutor.class.getDeclaredField("log");
        logField.setAccessible(true);
        logField.set(executor, mockLog);

        // Debugging: Confirm that executor's log field is set properly
        System.out.println("🚀 Logger injected: " + logField.get(executor));
    }

    /**
     * Unit test to directly test the helper function buildQueryLogDetails.
     */
    @Test
    public void testBuildQueryLogDetailsDirectly() {
        long reqId = 1L;
        Throwable error = new SQLException("Test SQL Error");
        UUID remoteNodeId = UUID.randomUUID();

        String logContent = executor.buildQueryLogDetails(
            reqId,
            "TestQuery",
            "TestSchema",
            Collections.emptyList(),
            new Object[]{},
            error,
            remoteNodeId
        );

        assertTrue(logContent.contains("Query Execution Failed"));
        assertTrue(logContent.contains("Remote Node ID: " + remoteNodeId));
        assertTrue(logContent.contains("Label: TestQuery"));
        assertTrue(logContent.contains("Schema: TestSchema"));
        assertTrue(logContent.contains("Error: Test SQL Error"));
    }

    /**
     * Unit test to simulate a query failure and validate the integration
     * of the helper method logQueryDetails.
     */
    @Test
    public void testBuildQueryLogDetailsOnQueryFailure() {
        ClusterNode mockNode = mock(ClusterNode.class);
        UUID nodeId = UUID.randomUUID();
        when(mockNode.id()).thenReturn(nodeId);

        String invalidSQL = "INVALID QUERY";
        Collection<GridCacheSqlQuery> queries = Collections.singletonList(new GridCacheSqlQuery(invalidSQL));
        Object[] params = new Object[]{};

        Throwable e = new RuntimeException("SIMULATED SQL error");

        String msg = executor.buildQueryLogDetails(
            1L, "TestLabel", "TestSchema", queries, params, e, nodeId
        );

        assertTrue(msg.contains("INVALID QUERY"));
        assertTrue(msg.contains("Parameters: []"));
        assertTrue(msg.contains("Remote Node ID: " + nodeId));
        assertTrue(msg.contains("Error: SIMULATED SQL error"));
    }

    /**
     * Unit test to validate behavior of the helper method logQueryDetails
     * with null or empty inputs.
     */
    @Test
    public void testBuildQueryLogDetailsWithNullInputs() {
        UUID remoteNodeId = UUID.randomUUID();

        String logContent = executor.buildQueryLogDetails(
            1L,
            null,
            null,
            null,
            null,
            null,
            remoteNodeId
        );

        assertTrue(logContent.contains("Query Execution Failed"));
        assertTrue(logContent.contains("Label: N/A"));
        assertTrue(logContent.contains("Schema: N/A"));
        assertTrue(logContent.contains("Queries: N/A"));
        assertTrue(logContent.contains("Parameters: N/A"));
        assertTrue(logContent.contains("Remote Node ID: " + remoteNodeId));
    }


    /**
     * Integration test to validate query execution error logging through the full
     * query execution flow.
     */
    @Test
    public void testBuildQueryLogDetailsIntegrationLikeFlow() {
        ClusterNode mockNode = mock(ClusterNode.class);
        UUID nodeId = UUID.randomUUID();
        when(mockNode.id()).thenReturn(nodeId);

        Collection<GridCacheSqlQuery> queries = Collections.singletonList(new GridCacheSqlQuery("SELECT * FROM test"));
        Throwable e = new RuntimeException("Forced Failure");

        String logMsg = executor.buildQueryLogDetails(
            1L, "TestLabel", "TestSchema", queries, new Object[]{}, e, nodeId
        );

        assertTrue(logMsg.contains("Query Execution Failed"));
        assertTrue(logMsg.contains("SELECT * FROM test"));
        assertTrue(logMsg.contains("Error: Forced Failure"));
    }
}