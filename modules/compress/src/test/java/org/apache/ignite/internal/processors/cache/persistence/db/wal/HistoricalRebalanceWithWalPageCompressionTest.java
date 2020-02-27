package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.db.SlowHistoricalRebalanceSmallHistoryTest;

import static org.apache.ignite.configuration.DiskPageCompression.ZSTD;

/** */
public class HistoricalRebalanceWithWalPageCompressionTest extends SlowHistoricalRebalanceSmallHistoryTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.getDataStorageConfiguration()
            .setWalPageCompression(ZSTD);

        return cfg;
    }
}