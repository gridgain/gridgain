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

        String logContent = GridMapQueryExecutor.buildQueryLogDetails(
                reqId,
                "TestQuery",
                "TestSchema",
                Collections.emptyList(),
                new Object[]{},
                error,
                remoteNodeId,
                localNodeId
        );

        assertTrue(logContent.contains("reqId=1"));
        assertTrue(logContent.contains("label=TestQuery"));
        assertTrue(logContent.contains("schema=TestSchema"));
        assertTrue(logContent.contains("queries=N/A"));
        assertTrue(logContent.contains("localNodeId=" + localNodeId));
        assertTrue(logContent.contains("remoteNodeId=" + remoteNodeId));
        assertTrue(logContent.contains("params=HIDDEN"));
    }

    @Test
    public void testBuildQueryLogDetailsWithNullInputs() {
        long reqId = 123;
        String label = null;
        String schemaName = null;
        Collection<GridCacheSqlQuery> queries = null;
        Object[] params = null;
        UUID remoteNodeId = UUID.randomUUID();
        UUID localNodeId = UUID.randomUUID();

        String logContent = GridMapQueryExecutor.buildQueryLogDetails(
                reqId,
                label,
                schemaName,
                queries,
                params,
                null,
                remoteNodeId,
                localNodeId
        );

        assertTrue(logContent.contains("reqId=123"));
        assertTrue(logContent.contains("label=N/A"));
        assertTrue(logContent.contains("schema=N/A"));
        assertTrue(logContent.contains("queries=N/A"));
        assertTrue(logContent.contains("params=N/A"));
        assertTrue(logContent.contains("localNodeId=" + localNodeId));
        assertTrue(logContent.contains("remoteNodeId=" + remoteNodeId));
    }

    @Test
    public void testBuildQueryLogDetailsIntegrationLikeFlow() {
        UUID nodeId = UUID.randomUUID();
        UUID localNodeId = UUID.randomUUID();

        Collection<GridCacheSqlQuery> queries =
                Collections.singletonList(new GridCacheSqlQuery("SELECT * FROM test"));
        Throwable e = new RuntimeException("Forced Failure");

        String logMsg = GridMapQueryExecutor.buildQueryLogDetails(
                1L,
                "TestLabel",
                "TestSchema",
                queries,
                new Object[]{},
                e,
                nodeId,
                localNodeId
        );

        assertTrue(logMsg.contains("reqId=1"));
        assertTrue(logMsg.contains("label=TestLabel"));
        assertTrue(logMsg.contains("schema=TestSchema"));
        assertTrue(logMsg.contains("queries=HIDDEN"));
        assertTrue(logMsg.contains("localNodeId=" + localNodeId));
        assertTrue(logMsg.contains("remoteNodeId=" + nodeId));
        assertTrue(logMsg.contains("params=HIDDEN"));
    }
}
