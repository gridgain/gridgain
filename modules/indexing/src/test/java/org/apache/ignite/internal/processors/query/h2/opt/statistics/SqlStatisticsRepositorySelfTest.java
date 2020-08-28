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

import java.util.Collections;

/**
 * Test different configurations of statistics repository.
 */
public class SqlStatisticsRepositorySelfTest extends SqlStatisticsTest {



    @Test
    public void testDataNodeWithoutPersistence() {
        SqlStatisticsRepository repository = getDataNoPersistenceRepository();

        testLocalPartitionStatistics(repository);
        testLocalStatistics(repository);
        testGlobalStatistics(repository);
    }

    private void testLocalPartitionStatistics(SqlStatisticsRepository repository) {
        ObjectPartitionStatistics stat1 = new ObjectPartitionStatistics(1, true, 1, 100,
                Collections.emptyMap());


    }

    private void testLocalStatistics(SqlStatisticsRepository repository) {
        QueryTable tbl1 = tbl(1);
        ObjectStatistics stat1 = new ObjectStatistics(1, Collections.emptyMap());
        repository.cacheLocalStatistics(tbl1, stat1);

        ObjectStatistics stat1r = repository.getLocalStatistics(tbl(1), false);

        assertEquals(stat1, stat1r);


    }

    /**
     * Test:
     * 1) null stats for tbl1
     * 2) can clear empty stat
     * 3) can save and read stat
     * 4) can clean and read null stat
     * 5) can cache and read stat
     * 6) can crean and read null stat
     * @param repository
     */
    private void testGlobalStatistics(SqlStatisticsRepository repository) {
        QueryTable tbl1 = tbl(1);

        // 1) null stats for tbl1
        assertNull(repository.getGlobalStatistics(tbl1, false));

        // 2) can clear empty stat
        repository.clearGlobalStatistics(tbl1);

        assertNull(repository.getGlobalStatistics(tbl1, false));

        // 3) can save and read stat
        ObjectStatistics statistics1 = new ObjectStatistics(1, Collections.emptyMap());
        repository.saveGlobalStatistics(tbl1, statistics1, true);

        assertNull(repository.getGlobalStatistics(tbl(2), false));
        ObjectStatistics statistics1r = repository.getGlobalStatistics(tbl(1), false);

        assertEquals(statistics1, statistics1r);

        // 4) can clear and read null stat
        repository.clearGlobalStatistics(tbl1);

        assertNull(repository.getGlobalStatistics(tbl1, false));

        // 5) can cache and read stat
        QueryTable tbl3 = tbl(3);
        ObjectStatistics statistics3 = new ObjectStatistics(3, Collections.emptyMap());
        repository.saveGlobalStatistics(tbl3, statistics3, true);

        assertNull(repository.getGlobalStatistics(tbl(4), false));
        ObjectStatistics statistics3r = repository.getGlobalStatistics(tbl(3), false);

        assertEquals(statistics3, statistics3r);

        // 6) can crean and read null stat
        repository.clearGlobalStatistics(tbl3);

        assertNull(repository.getGlobalStatistics(tbl3, false));
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
        cfg.setClientMode(false);

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
