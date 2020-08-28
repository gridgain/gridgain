package org.apache.ignite.internal.processors.query.h2.opt.statistics;

import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test different configurations of statistics repository.
 */
public class SqlStatisticsRepositorySelfTest extends GridCommonAbstractTest {



    @Test
    public void testDataNodeWithoutPersistence() {
        SqlStatisticsRepository repository = getDataNoPersistenceRepository();

        repository.clearGlobalStatistics(tbl(1));


    }

    private QueryTable tbl(int idx) {
        return new QueryTable("SCHEMA_NAME","TBL_" + idx);
    }

    private SqlStatisticsRepository getClientRepository() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(true);

        GridKernalContext ctx = Mockito.mock(GridKernalContext.class);
        Mockito.when(ctx.config()).thenReturn(cfg);

        SqlStatisticsRepositoryImpl result = new SqlStatisticsRepositoryImpl(ctx);
        result.start();
        return result;
    }

    private SqlStatisticsRepository getDataNoPersistenceRepository() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        GridKernalContext ctx = Mockito.mock(GridKernalContext.class);
        Mockito.when(ctx.config()).thenReturn(cfg);

        SqlStatisticsRepositoryImpl result = new SqlStatisticsRepositoryImpl(ctx);
        result.start();
        return result;
    }

    private SqlStatisticsRepository getDataPersistenceRepository() {
        DataRegionConfiguration dataRegionConfiguration = new DataRegionConfiguration();
        dataRegionConfiguration.setPersistenceEnabled(true);

        DataStorageConfiguration dataStorageConfiguration = new DataStorageConfiguration();
        dataStorageConfiguration.setDefaultDataRegionConfiguration(dataRegionConfiguration);

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setDataStorageConfiguration(dataStorageConfiguration);

        GridKernalContext ctx = Mockito.mock(GridKernalContext.class);
        Mockito.when(ctx.config()).thenReturn(cfg);

        GridInternalSubscriptionProcessor subscriptionProccessor = Mockito.mock(GridInternalSubscriptionProcessor.class);
        Mockito.when(ctx.internalSubscriptionProcessor()).thenReturn(subscriptionProccessor);

        SqlStatisticsRepositoryImpl result = new SqlStatisticsRepositoryImpl(ctx);
        result.start();
        return result;
    }
}
