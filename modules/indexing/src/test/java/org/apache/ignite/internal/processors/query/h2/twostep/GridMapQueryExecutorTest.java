package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.h2.H2PooledConnection;
import org.apache.ignite.internal.processors.query.h2.MapH2QueryInfo;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class GridMapQueryExecutorTest extends GridCommonAbstractTest {
    private GridMapQueryExecutor executor;
    private GridKernalContext ctx;
    private ClusterNode node;
    private GridH2QueryRequest req;

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        ctx = Mockito.mock(GridKernalContext.class);
        executor = new GridMapQueryExecutor(ctx);
        node = Mockito.mock(ClusterNode.class);
        req = Mockito.mock(GridH2QueryRequest.class);

        when(ctx.log(GridMapQueryExecutor.class)).thenReturn(Mockito.mock(IgniteLogger.class));
    }

    @Test
    public void testQueryExecutionFailure() {
        GridMapQueryExecutor executorSpy = Mockito.spy(new GridMapQueryExecutor(ctx));
        IgniteLogger log = ctx.log(GridMapQueryExecutor.class);
        Mockito.doReturn(log).when(executorSpy).getLog();

        // Simulate query execution failure
        Mockito.doThrow(new IgniteCheckedException(new SQLException("Forced error"))).when(executorSpy).onQueryRequest(any(), any());

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
        when(req.queries()).thenReturn(Collections.singletonList(new GridCacheSqlQuery()));
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
        doThrow(new IgniteCheckedException(new SQLException("Test exception"))).when(executor).executeSqlQueryWithTimer(
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
