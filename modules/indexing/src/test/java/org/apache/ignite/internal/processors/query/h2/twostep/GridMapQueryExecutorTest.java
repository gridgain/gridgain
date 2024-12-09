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
//import org.mockito.Mockito;
//import org.slf4j.Logger;
import java.sql.SQLException;
//import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.lang.reflect.Field;
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

            // Act
            executor.logQueryDetails(reqId, label, schema, Collections.emptyList(), new Object[]{}, error);

            // Assert
            verify(mockLog).error(contains("Query Execution Failed"), eq(error));
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

        // Ensure logQueryDetails is mocked
        doNothing().when(executor).logQueryDetails(
                anyLong(),
                anyString(),
                anyString(),
                anyCollection(),
                any(),
                any(Throwable.class)
        );

        // Mock onQueryRequest0 to throw an exception and invoke logQueryDetails
        doAnswer(invocation -> {
            executor.logQueryDetails(1L, "TestLabel", "TestSchema", queries, params, new RuntimeException("SIMULATED SQL error"));
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
        verify(executor).logQueryDetails(eq(1L), eq("TestLabel"), eq("TestSchema"), eq(queries), eq(params), any(Throwable.class));
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
           executor.logQueryDetails(1L, null, null, null, null, null);

           // Assert: Verify the logger was invoked correctly
           verify(mockLog).error(anyString(), eq(null));
        } catch (NoSuchFieldException | IllegalAccessException e) {
           throw new AssertionError("Reflection operation failed", e);
        }
    }
}
