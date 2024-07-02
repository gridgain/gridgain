package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;

import static org.mockito.Mockito.*;

public class GridMapQueryExecutorTest extends GridCommonAbstractTest {
    private GridMapQueryExecutor executor;
    private GridKernalContext ctx;
    private IgniteH2Indexing h2;
    private ClusterNode node;
    private GridH2QueryRequest req;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        ctx = Mockito.mock(GridKernalContext.class);
        h2 = Mockito.mock(IgniteH2Indexing.class);
        executor = new GridMapQueryExecutor(ctx, h2);
        node = Mockito.mock(ClusterNode.class);
        req = Mockito.mock(GridH2QueryRequest.class);

        when(ctx.log(GridMapQueryExecutor.class)).thenReturn(Mockito.mock(IgniteLogger.class));
    }

    @Test
    public void testQueryExecutionFailure() {
        GridMapQueryExecutor executorSpy = Mockito.spy(new GridMapQueryExecutor(ctx, h2));
        IgniteLogger log = ctx.log(GridMapQueryExecutor.class);
        Mockito.doReturn(log).when(executorSpy).log();
        
        // Simulate query execution failure
        Mockito.doThrow(new SQLException("Forced error")).when(executorSpy).onQueryRequest(any(), any());

        try {
            executorSpy.onQueryRequest(Mockito.mock(ClusterNode.class), Mockito.mock(GridH2QueryRequest.class));
        } catch (IgniteCheckedException e) {
            // expected
        }

        Mockito.verify(log).error(contains("Failed to execute query"), any(SQLException.class));
    }

    @Test
    public void testOnQueryRequest() throws Exception {
        // Mocking behaviors for query request
        when(req.query()).thenReturn("SELECT * FROM test;");
        when(req.partitions()).thenReturn(null);
        when(req.queryPartitions()).thenReturn(null);
        when(req.caches()).thenReturn(Collections.emptyList());
        when(req.parameters()).thenReturn(new Object[]{});
        when(req.isDataPageScanEnabled()).thenReturn(false);
        when(req.schemaName()).thenReturn("PUBLIC");
        when(req.queries()).thenReturn(Collections.singletonList(new GridCacheSqlQuery()));
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
        doThrow(new SQLException("Test exception")).when(h2).executeSqlQueryWithTimer(
                any(PreparedStatement.class),
                any(H2PooledConnection.class),
                anyString(),
                anyInt(),
                any(GridQueryCancel.class),
                anyBoolean(),
                any(MapH2QueryInfo.class),
                anyLong()
        );

        // Perform the request and check that IgniteCheckedException is thrown
        assertThrows(IgniteCheckedException.class, () -> {
            executor.onQueryRequest(node, req);
        });

        // Verify that the error is logged with the expected message
        verify(ctx.log(GridMapQueryExecutor.class)).error(eq("Failed to execute local query: SELECT * FROM test; with parameters: []. Node ID: " + node.id()), any(SQLException.class));
    }
}
