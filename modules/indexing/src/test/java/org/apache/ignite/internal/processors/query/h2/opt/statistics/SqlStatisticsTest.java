package org.apache.ignite.internal.processors.query.h2.opt.statistics;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.mockito.Mockito;

public class SqlStatisticsTest extends GridCommonAbstractTest {

    protected SqlStatisticsStoreImpl getStore() {

        IgniteCacheDatabaseSharedManager databaseSharedManager = Mockito.mock(IgniteCacheDatabaseSharedManager.class);

        GridCacheSharedContext sharedContext = Mockito.mock(GridCacheSharedContext.class);
        Mockito.when(sharedContext.database()).thenReturn(databaseSharedManager);

        GridCacheProcessor cacheProcessor = Mockito.mock(GridCacheProcessor.class);
        Mockito.when(cacheProcessor.context()).thenReturn(sharedContext);

        GridKernalContext ctx = Mockito.mock(GridKernalContext.class);
        Mockito.when(ctx.cache()).thenReturn(cacheProcessor);

        GridInternalSubscriptionProcessor subscriptionProccessor = Mockito.mock(GridInternalSubscriptionProcessor.class);
        Mockito.when(ctx.internalSubscriptionProcessor()).thenReturn(subscriptionProccessor);

        SqlStatisticsRepository repository = Mockito.mock(SqlStatisticsRepository.class);
        SqlStatisticsStoreImpl store = new SqlStatisticsStoreImpl(ctx);
        store.start(repository);
        return store;
    }

    protected QueryTable tbl(int idx) {
        return new QueryTable("SCHEMA_NAME","TBL_" + idx);
    }
}
