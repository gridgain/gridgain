package org.apache.ignite.internal.processors.cache.persistence.db;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;

/**
 * https://www.gridgain.com/docs/latest/perf-troubleshooting-guide/sql-tuning#increasing-index-inline-size
 */
public class PrimarySizeTest extends GridCommonAbstractTest {

    public static final String TABLE_NAME = "t";
    public static final String COLUMN_ID = "id";
    public static final String COLUMN_VALUE = "v";
    public static final String CACHE_TABLE = "SQL_PUBLIC_T";

    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
                .setCheckpointFrequency(60000)
        );

        return cfg;
    }

    /** {@inheritDoc */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    @Test
    public void test() throws Exception {
        try (IgniteEx srv = startGrids(1)) {

            srv.cluster().state(ClusterState.ACTIVE);

            final IgniteCache cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME);

            cache.query(new SqlFieldsQuery("create table " + TABLE_NAME + " (" + COLUMN_ID + " varchar primary key, " + COLUMN_VALUE + " varchar)")).getAll();
//            cache.query(new SqlFieldsQuery("create table t (id varchar PRIMARY KEY INLINE_SIZE 15, v varchar)")).getAll();
//            cache.query(new SqlFieldsQuery("create table t (id varchar, v varchar, PRIMARY KEY (id) INLINE_SIZE 15)")).getAll();
//            cache.query(new SqlFieldsQuery("create table t (id varchar primary key, v varchar) WITH INLINE_SIZE=15")).getAll();
//            System.setProperty("IGNITE_MAX_INDEX_PAYLOAD_SIZE", "20");
            // org.apache.ignite.internal.processors.query.h2.database.H2TreeIndexBase.IGNITE_VARIABLE_TYPE_DEFAULT_INDEX_SIZE

            final File indexBinSizeCsv = new File(U.defaultWorkDirectory(), "index_bin_size.csv");

            log.info(indexBinSizeCsv.getAbsolutePath());

            try (PrintWriter out = new PrintWriter(new FileWriter(indexBinSizeCsv))) {
                out.println("cache size\t" + INDEX_FILE_NAME + " size");

                final GridCacheProcessor gcp = srv.context().cache();

                final FilePageStoreManager pageStoreManager = (FilePageStoreManager)(gcp.context().pageStore());

                final File cacheDir = pageStoreManager.cacheWorkDir(gcp.cacheDescriptor(CACHE_TABLE).cacheConfiguration());

                final File indexBin = new File(cacheDir, INDEX_FILE_NAME);

                for (int i = 1; i < 10_000_000; i++) {
                    final SqlFieldsQuery query = new SqlFieldsQuery("insert into " + TABLE_NAME + " (" + COLUMN_ID + ", " + COLUMN_VALUE + ") values (?, ?)");

                    query.setArgs(Integer.toString(i) + UUID.randomUUID(), Integer.toHexString(i) + UUID.randomUUID());

                    cache.query(query).getAll();

                    if (i % 10000 == 0) {
                        forceCheckpoint();

                        out.println(i + "\t" + indexBin.length());
                        out.flush();
                    }
                }
            }
        }
    }
}
