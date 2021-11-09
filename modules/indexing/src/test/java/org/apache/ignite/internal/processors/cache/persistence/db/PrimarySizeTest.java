package org.apache.ignite.internal.processors.cache.persistence.db;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;

public class PrimarySizeTest extends GridCommonAbstractTest {

    public static final String CACHE = "test";

    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                                .setPersistenceEnabled(true)
                )
                        .setCheckpointFrequency(60000)
        );

        cfg.setCacheConfiguration(new CacheConfiguration<String, Integer>()
                .setName(CACHE)
                .setBackups(0)
                .setSqlSchema(DFLT_SCHEMA)
                .setIndexedTypes(String.class, Integer.class));

        return cfg;
    }

    /** {@inheritDoc */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    @Test
    public void test() throws Exception {
        IgniteEx srv = startGrids(1);

        srv.cluster().state(ClusterState.ACTIVE);

        IgniteCache<String, Integer> cache = srv.getOrCreateCache(CACHE);

        cache.query(new SqlFieldsQuery("create table t (id varchar primary key, v int)")).getAll();

        for (int i = 10_000_000; i < 10_030_000; i++) {
            cache.query(new SqlFieldsQuery("insert into t (id, v) values (?, ?)").setArgs(String.valueOf(i),i)).getAll();
        }

        forceCheckpoint();
        Thread.sleep(1000);
    }
}
