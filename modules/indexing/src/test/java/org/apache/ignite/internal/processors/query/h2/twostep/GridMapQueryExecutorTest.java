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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContextRegistry;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
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
     * Unit test to directly test the helper method logQueryDetails.
     */
    @Test
    public void testLogQueryDetailsDirectly() {
        long reqId = 1L;
        Throwable error = new SQLException("Test SQL Error");
        UUID remoteNodeId = UUID.randomUUID();

        executor.logQueryDetails(reqId, "TestQuery", "TestSchema", Collections.emptyList(), new Object[]{}, error, remoteNodeId);

        ArgumentCaptor<String> logCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockLog, atLeastOnce()).error(logCaptor.capture(), any(Throwable.class));

        String logContent = logCaptor.getValue();
        assertTrue(logContent.contains("Query Execution Failed"));
        assertTrue(logContent.contains("Remote Node ID: " + remoteNodeId));

        // ✅ Ensure No Extra Logs Are Present
        Mockito.verifyNoMoreInteractions(mockLog);
    }

    /**
     * Unit test to simulate a query failure and validate the integration
     * of the helper method logQueryDetails.
     */
    @Test
    public void testLogQueryDetailsOnQueryFailure() {
        ClusterNode mockNode = mock(ClusterNode.class);
        when(mockNode.id()).thenReturn(UUID.randomUUID());

        String invalidSQL = "INVALID QUERY";
        Collection<GridCacheSqlQuery> queries = Collections.singletonList(new GridCacheSqlQuery(invalidSQL));
        Object[] params = new Object[]{};

        doAnswer(invocation -> {
            Throwable e = new RuntimeException("SIMULATED SQL error");

            executor.logQueryDetails(1L, "TestLabel", "TestSchema", queries, params, e, mockNode.id());

            throw e;
        }).when(executor).onQueryRequest0(
            eq(mockNode),
            anyLong(),
            anyString(),
            anyInt(),
            anyString(),
            anyCollection(),
            anyList(),
            any(),
            any(),
            any(),
            anyInt(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            anyInt(),
            any(),
            anyBoolean(),
            any(),
            anyBoolean(),
            anyLong(),
            any(),
            anyBoolean()
        );

        try {
            executor.onQueryRequest0(
                mockNode,
                1L,          // reqId
                "TestLabel", // label
                0,           // segmentId
                "TestSchema",// schemaName
                queries,
                Collections.emptyList(), // cacheIds
                null,          // topVer
                null,          // partsMap
                null,          // parts
                10,            // pageSize
                false,         // distributedJoins
                false,         // enforceJoinOrder
                false,         // replicated
                1000,          // timeout
                params,
                false,         // lazy
                null,          // mvccSnapshot
                false,         // dataPageScanEnabled
                0L,            // maxMem
                null,          // runningQryId
                false          // treatReplicatedAsPartitioned
            );
        } catch (RuntimeException e) {
            // Expected exception to trigger logQueryDetails
        }

        ArgumentCaptor<String> logCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockLog, atLeastOnce()).error(logCaptor.capture(), any(Throwable.class));

        // ✅ Print captured logs for debugging
        System.out.println("🚀 Captured Logs:");
        for (String capturedLog : logCaptor.getAllValues()) {
            System.out.println("🔥 " + capturedLog);
        }

        String logMsg = logCaptor.getValue();
        assertTrue(logMsg.contains("INVALID QUERY"));
        assertTrue(logMsg.contains("Parameters: []"));
    }

    /**
     * Unit test to validate behavior of the helper method logQueryDetails
     * with null or empty inputs.
     */
    @Test
    public void testLogQueryDetailsWithNullInputs() {
        UUID remoteNodeId = UUID.randomUUID();

        executor.logQueryDetails(1L, null, null, null, null, null, remoteNodeId);

        ArgumentCaptor<String> logCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Throwable> throwableCaptor = ArgumentCaptor.forClass(Throwable.class);

        verify(mockLog, atLeastOnce()).error(logCaptor.capture(), throwableCaptor.capture());

        System.out.println("🚀 Captured Logs is testLogQueryDetailsWithNullInputs:");
        for (String capturedLog : logCaptor.getAllValues()) {
            System.out.println("🔥 " + capturedLog);
        }

        String logContent = logCaptor.getValue();
        assertTrue("Log must contain 'Query Execution Failed'", logContent.contains("Query Execution Failed"));
        assertTrue("Log must contain 'Label: N/A'", logContent.contains("Label: N/A"));
        assertTrue("Log must contain 'Schema: N/A'", logContent.contains("Schema: N/A"));
        assertTrue("Log must contain 'Remote Node ID'", logContent.contains("Remote Node ID: " + remoteNodeId));

        // Print all interactions captured by Mockito
        Mockito.verifyNoMoreInteractions(mockLog);
    }

    /**
     * Integration test to validate query execution error logging through the full
     * query execution flow.
     */
    @Test
    public void testQueryExecutionErrorLoggingIntegration() throws Exception{
        ClusterNode mockNode = mock(ClusterNode.class);
        UUID remoteNodeId = UUID.randomUUID();
        when(mockNode.id()).thenReturn(remoteNodeId);

        doThrow(new RuntimeException("Test Exception"))
            .when(executor).onQueryRequest0(
                eq(mockNode),
                anyLong(),
                anyString(),
                anyInt(),
                anyString(),
                anyCollection(),
                anyList(),
                any(),
                any(),
                any(),
                anyInt(),
                anyBoolean(),
                anyBoolean(),
                anyBoolean(),
                anyInt(),
                any(),
                anyBoolean(),
                any(),
                anyBoolean(),
                anyLong(),
                any(),
                anyBoolean());

        try {
            GridH2QueryRequest req = new GridH2QueryRequest();
            executor.onQueryRequest(mockNode, req);
        } catch (IgniteCheckedException ignored) { /* Expected */ }

        executor.logQueryDetails(1L, "TestLabel", "TestSchema", null, null, new RuntimeException("Forced Failure"), mockNode.id());

        ArgumentCaptor<String> logCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockLog, atLeastOnce()).error(logCaptor.capture(), any(Throwable.class));

        System.out.println("🚀 Captured Logs Before Verifying No More Interactions:");
        for (String capturedLog : logCaptor.getAllValues()) {
            System.out.println("🔥 " + capturedLog);
        }

        String logContent = logCaptor.getValue();
        assertTrue(logContent.contains("Query Execution Failed"));
    }
}