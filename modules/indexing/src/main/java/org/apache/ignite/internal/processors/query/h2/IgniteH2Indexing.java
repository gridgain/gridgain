package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.h2.H2PooledConnection;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

public class GridMapQueryExecutorTest extends GridCommonAbstractTest {
    private GridMapQueryExecutor executor;
    private GridKernalContext ctx;
    private IgniteH2Indexing h2;
    private ClusterNode node;
    private GridH2QueryRequest req;

    @Before
    public void setUp() throws Exception {
        ctx = Mockito.mock(GridKernalContext.class);
        h2 = Mockito.mock(IgniteH2Indexing.class);
        executor = new GridMapQueryExecutor(ctx); // Provide ctx to the constructor
        node = Mockito.mock(ClusterNode.class);
        req = Mockito.mock(GridH2QueryRequest.class);

        when(ctx.log(GridMapQueryExecutor.class)).thenReturn(Mockito.mock(IgniteLogger.class));
    }

    @Test
    public void testQueryExecutionFailure() throws Exception {
        GridMapQueryExecutor executorSpy = Mockito.spy(new GridMapQueryExecutor(ctx)); // Provide ctx to the constructor
        IgniteLogger log = ctx.log(GridMapQueryExecutor.class);

        // Use reflection to set the private log field if getLog() is not available
        java.lang.reflect.Field logField = GridMapQueryExecutor.class.getDeclaredField("log");
        logField.setAccessible(true);
        logField.set(executorSpy, log);

        // Simulate query execution failure
        doThrow(new IgniteCheckedException(new SQLException("Forced error"))).when(executorSpy).onQueryRequest(any(), any());

        try {
            executorSpy.onQueryRequest(Mockito.mock(ClusterNode.class), Mockito.mock(GridH2QueryRequest.class));
        } catch (IgniteCheckedException e) {
            // expected
        }

        verify(log).error(contains("Failed to execute query"), any(SQLException.class));
    }

    @Test
    public void testOnQueryRequest() throws Exception {
        // Mocking behaviors for query request
        GridCacheSqlQuery mockQuery = mock(GridCacheSqlQuery.class);
        when(mockQuery.query()).thenReturn("SELECT * FROM test;");

        List<GridCacheSqlQuery> mockQueries = Collections.singletonList(mockQuery);
        when(req.queries()).thenReturn(mockQueries);
        when(req.partitions()).thenReturn(null);
        when(req.queryPartitions()).thenReturn(null);
        when(req.caches()).thenReturn(Collections.emptyList());
        when(req.parameters()).thenReturn(new Object[]{});
        when(req.isDataPageScanEnabled()).thenReturn(false);
        when(req.schemaName()).thenReturn("PUBLIC");
        when(req.topologyVersion()).thenReturn(AffinityTopologyVersion.NONE);
        when(req.timeout()).thenReturn(0);
        when(req.explicitTimeout()).thenReturn(false);
        when(req.pageSize()).thenReturn(0);
        when(req.isFlagSet(GridH2QueryRequest.FLAG_DISTRIBUTED_JOINS)).thenReturn(false);
        when(req.isFlagSet(GridH2QueryRequest.FLAG_ENFORCE_JOIN_ORDER)).thenReturn(false);
        when(req.isFlagSet(GridH2QueryRequest.FLAG_EXPLAIN)).thenReturn(false);
        when(req.isFlagSet(GridH2QueryRequest.FLAG_REPLICATED)).thenReturn(false);
        when(req.isFlagSet(GridH2QueryRequest.FLAG_LAZY)).thenReturn(false);
        when(req.isFlagSet(GridH2QueryRequest.FLAG_REPLICATED_AS_PARTITIONED)).thenReturn(false);

        // Simulate query execution failure to check logging
        doThrow(new IgniteCheckedException(new SQLException("Test exception"))).when(h2).executeSqlQueryWithTimer(
                any(PreparedStatement.class),
                any(H2PooledConnection.class),
                anyString(),
                anyInt(),
                any(GridQueryCancel.class),
                anyBoolean(),
                any(), // Assuming MapH2QueryInfo is not necessary or use the correct class
                anyLong()
        );

        // Perform the request and check that IgniteCheckedException is thrown
        try {
            executor.onQueryRequest(node, req);
            fail("Expected IgniteCheckedException to be thrown");
        } catch (IgniteCheckedException e) {
            // expected
        }

        // Verify that the error is logged with the expected message
        verify(ctx.log(GridMapQueryExecutor.class)).error(eq("Failed to execute local query: SELECT * FROM test; with parameters: []. Node ID: " + node.id()), any(SQLException.class));
    }
}
