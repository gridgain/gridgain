/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

public class GridMapQueryExecutorTest {

    private final GridMapQueryExecutor executor = new GridMapQueryExecutor();

    @Test
    public void testBuildQueryLogDetailsDirectly() {
        long reqId = 1L;
        Throwable error = new SQLException("Test SQL Error");
        UUID localNodeId = UUID.randomUUID();
        UUID remoteNodeId = UUID.randomUUID();

        String logContent = executor.buildQueryLogDetails(
                reqId,
                "TestQuery",
                "TestSchema",
                Collections.emptyList(),
                new Object[]{},
                error,
                remoteNodeId,
                localNodeId
        );

        assertTrue(logContent.contains("Query Execution Failed"));
        assertTrue(logContent.contains("Remote Node ID: " + remoteNodeId));
        assertTrue(logContent.contains("Label: TestQuery"));
        assertTrue(logContent.contains("Schema: TestSchema"));
        assertTrue(logContent.contains("Error: Test SQL Error"));
    }

    @Test
    public void testBuildQueryLogDetailsWithNullInputs() {
        UUID remoteNodeId = UUID.randomUUID();
        UUID localNodeId = UUID.randomUUID();

        String logContent = executor.buildQueryLogDetails(
                1L,
                null,
                null,
                null,
                null,
                null,
                remoteNodeId,
                localNodeId
        );

        assertTrue(logContent.contains("Query Execution Failed"));
        assertTrue(logContent.contains("Label: N/A"));
        assertTrue(logContent.contains("Schema: N/A"));
        assertTrue(logContent.contains("Queries: N/A"));
        assertTrue(logContent.contains("Parameters: N/A"));
        assertTrue(logContent.contains("Remote Node ID: " + remoteNodeId));
    }

    @Test
    public void testBuildQueryLogDetailsIntegrationLikeFlow() {
        UUID nodeId = UUID.randomUUID();
        UUID localNodeId = UUID.randomUUID();

        Collection<GridCacheSqlQuery> queries =
                Collections.singletonList(new GridCacheSqlQuery("SELECT * FROM test"));
        Throwable e = new RuntimeException("Forced Failure");

        String logMsg = executor.buildQueryLogDetails(
                1L,
                "TestLabel",
                "TestSchema",
                queries,
                new Object[]{},
                e,
                nodeId,
                localNodeId
        );

        assertTrue(logMsg.contains("Query Execution Failed"));
        assertTrue(logMsg.contains("SELECT * FROM test"));
        assertTrue(logMsg.contains("Error: Forced Failure"));
    }
}
