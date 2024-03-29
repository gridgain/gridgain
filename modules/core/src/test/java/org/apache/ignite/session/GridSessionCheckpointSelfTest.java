/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.session;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.spi.checkpoint.cache.CacheCheckpointSpi;
import org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpi;
import org.apache.ignite.spi.checkpoint.sharedfs.SharedFsCheckpointSpi;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.hsqldb.jdbc.jdbcDataSource;
import org.junit.Test;

/**
 * Grid session checkpoint self test.
 */
@GridCommonTest(group = "Task Session")
public class GridSessionCheckpointSelfTest extends GridSessionCheckpointAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSharedFsCheckpoint() throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        cfg.setCheckpointSpi(spi = new SharedFsCheckpointSpi());

        checkCheckpoints(cfg);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJdbcCheckpoint() throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        jdbcDataSource ds = new jdbcDataSource();

        ds.setDatabase("jdbc:hsqldb:mem:gg_test");
        ds.setUser("sa");
        ds.setPassword("");

        JdbcCheckpointSpi spi = new JdbcCheckpointSpi();

        spi.setDataSource(ds);
        spi.setCheckpointTableName("checkpoints");
        spi.setKeyFieldName("key");
        spi.setValueFieldName("value");
        spi.setValueFieldType("longvarbinary");
        spi.setExpireDateFieldName("create_date");

        GridSessionCheckpointSelfTest.spi = spi;

        cfg.setCheckpointSpi(spi);

        checkCheckpoints(cfg);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheCheckpoint() throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        String cacheName = "test-checkpoints";

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(cacheName);

        CacheCheckpointSpi spi = new CacheCheckpointSpi();

        spi.setCacheName(cacheName);

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setCheckpointSpi(spi);

        if (cfg.getMarshaller() instanceof BinaryMarshaller) {
            BinaryMarshaller marsh = (BinaryMarshaller)cfg.getMarshaller();

            BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), cfg, new NullLogger());

            marsh.setContext(new MarshallerContextTestImpl(null));

            marsh.setBinaryContext(ctx, cfg);
        }

        GridSessionCheckpointSelfTest.spi = spi;

        checkCheckpoints(cfg);
    }
}
