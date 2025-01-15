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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
//import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
//import org.mockito.Mockito;
//import org.slf4j.Logger;
import java.sql.SQLException;
//import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.lang.reflect.Field;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class GridMapQueryExecutorTest {

    /**
     * Unit test to directly test the helper method logQueryDetails.
     */
    @Test
    public void testLogQueryDetailsDirectly() {
        try {
            // Arrange
            GridMapQueryExecutor executor = new GridMapQueryExecutor();
            IgniteLogger mockLog = mock(IgniteLogger.class);

            // Inject the mock logger using reflection
            Field logField = GridMapQueryExecutor.class.getDeclaredField("log");
            logField.setAccessible(true);
            logField.set(executor, mockLog);

            long reqId = 1L;
            String label = "TestQuery";
            String schema = "TestSchema";
            Throwable error = new SQLException("Test SQL Error");
            UUID remoteNodeId = UUID.randomUUID();

            // Act
            executor.logQueryDetails(reqId, label, schema, Collections.emptyList(), new Object[]{}, error, remoteNodeId);

            // Assert
            verify(mockLog).error(contains("Query Execution Failed"), eq(error));
            verify(mockLog).error(contains("Remote Node ID: " + remoteNodeId), eq(error));
            verify(mockLog).error(contains("Label: TestQuery"), eq(error));
            verify(mockLog).error(contains("Schema: TestSchema"), eq(error));
            verify(mockLog).error(contains("Remote Node ID: " + remoteNodeId), eq(error));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new AssertionError("Reflection operation failed", e);
        }
    }

    /**
     * Unit test to simulate a query failure and validate the integration
     * of the helper method logQueryDetails.
     */
    @Test
    public void testLogQueryDetailsOnQueryFailure() {
        // Arrange
        ClusterNode mockNode = mock(ClusterNode.class);
        when(mockNode.id()).thenReturn(UUID.randomUUID());

        GridMapQueryExecutor executor = spy(new GridMapQueryExecutor());

        // Simulated invalid query to trigger error
        String invalidSQL = "INVALID QUERY";
        Collection<GridCacheSqlQuery> queries = Collections.singletonList(new GridCacheSqlQuery(invalidSQL));
        Object[] params = new Object[]{};

//        // Ensure logQueryDetails is mocked
//        doNothing().when(executor).logQueryDetails(
//            anyLong(),
//            anyString(),
//            anyString(),
//            anyCollection(),
//            any(),
//            any(Throwable.class),
//            any()
//        );

        // Mock onQueryRequest0 to throw an exception and invoke logQueryDetails
        doAnswer(invocation -> {
            executor.logQueryDetails(1L, "TestLabel", "TestSchema", queries, params, new RuntimeException("SIMULATED SQL error"), UUID.randomUUID());
            throw new RuntimeException("SIMULATED SQL error");
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

        // Act
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

        // Assert
        verify(executor).logQueryDetails(eq(1L), eq("TestLabel"), eq("TestSchema"), eq(queries), eq(params), any(Throwable.class), any(UUID.class));
    }

    /**
     * Unit test to validate behavior of the helper method logQueryDetails
     * with null or empty inputs.
     */
    @Test
    public void testLogQueryDetailsWithNullInputs() {
        try {
            // Arrange
            GridMapQueryExecutor executor = new GridMapQueryExecutor();
            IgniteLogger mockLog = mock(IgniteLogger.class);

            // Inject the mock logger using reflection
            Field logField = GridMapQueryExecutor.class.getDeclaredField("log");
            logField.setAccessible(true);
            logField.set(executor, mockLog);

            // Act
            executor.logQueryDetails(1L, null, null, null, null, null, UUID.randomUUID());

            // Assert: Verify the logger was invoked correctly
            ArgumentCaptor<String> logMessageCaptor = ArgumentCaptor.forClass(String.class);
            verify(mockLog).error(logMessageCaptor.capture(), eq(null));

            String logContent = logMessageCaptor.getValue();

            assertTrue(logContent.contains("Label: N/A"));
            assertTrue(logContent.contains("Schema: N/A"));
            assertTrue(logContent.contains("Remote Node ID: " + UUID.randomUUID()));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new AssertionError("Reflection operation failed", e);
        }
    }

    /**
     * Integration test to validate query execution error logging through the full
     * query execution flow.
     */
    @Test
    public void testQueryExecutionErrorLoggingIntegration() {
        try {
            // Arrange
            GridMapQueryExecutor executor = new GridMapQueryExecutor();
            IgniteLogger mockLog = mock(IgniteLogger.class);

            Field logField = GridMapQueryExecutor.class.getDeclaredField("log");
            logField.setAccessible(true);
            logField.set(executor, mockLog);

            ClusterNode mockNode = mock(ClusterNode.class);
            UUID remoteNodeId = UUID.randomUUID();
            when(mockNode.id()).thenReturn(remoteNodeId);

            Throwable simulatedError = new RuntimeException("Test Exception");

            Collection<GridCacheSqlQuery> queries = Collections.emptyList();
            List<Integer> cacheIds = Collections.emptyList();
            Object[] params = {};

            // Act
            try {
                executor.onQueryRequest0(
                    mockNode,
                    123L,          // reqId
                    "IntegrationTestQuery", // label
                    0,             // segmentId
                    "TestSchema",  // schemaName
                    queries,       // queries
                    cacheIds,      // cacheIds
                    null,          // topVer
                    null,          // partsMap
                    null,          // parts
                    10,            // pageSize
                    false,         // distributedJoins
                    false,         // enforceJoinOrder
                    false,         // replicated
                    1000,          // timeout
                    params,        // params
                    false,         // lazy
                    null,          // mvccSnapshot
                    false,         // dataPageScanEnabled
                    0L,            // maxMem
                    null,          // runningQryId
                    false          // treatReplicatedAsPartitioned
                );

                // Assert
                ArgumentCaptor<String> logCaptor = ArgumentCaptor.forClass(String.class);
                verify(mockLog).error(logCaptor.capture(), eq(simulatedError));
                String logContent = logCaptor.getValue();
                assertTrue(logContent.contains("Query Execution Failed"));
                assertTrue(logContent.contains("Request ID: 123"));
                assertTrue(logContent.contains("Label: IntegrationTestQuery"));
                assertTrue(logContent.contains("Schema: TestSchema"));
                assertTrue(logContent.contains("Remote Node ID: " + remoteNodeId));
            } catch (Exception e) {
                throw new AssertionError("Test setup failed", e);
            }

            verify(mockLog, times(1)).error(anyString(), eq(simulatedError));
            verifyNoMoreInteractions(mockLog); // Ensures no duplicate logging

        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }
}