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
package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;

/**
 * Test for OOM if case of using large entries. This OOM may be caused by inaccurate results caching on
 * some stage of the query execution.
 */
public class IgniteCacheQueryLargeRecordsOomTest extends GridCacheAbstractSelfTest{
    /**
     * @throws Exception If error.
     */
    @Test
    public void testMemoryLeak() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        long heapMaxSize = Runtime.getRuntime().maxMemory();

        for (long i = 0; i < 1000; i++) {
            Person val = new Person(new byte[1024 * 1024]); // 1 MB entry.

            cache.put(i, val);
        }

        // We expect that each entry size is 1 MB and in PK BPlusTree cursor at least 100 entries may be cached.
        // It means that each cursor may have ~100 MB of cached data. So let's choose the number of open cursors
        // near to the half of available heap to find out if there is any leak here.
        final int nReaders = (int)(heapMaxSize / 1e8) / 2;

        if (log.isInfoEnabled())
            log.info("Data loaded. Number of readers=" + nReaders);

        ExecutorService ex = Executors.newFixedThreadPool(nReaders);

        Collection<Future> futs = new ArrayList<>(nReaders);

        for (int i = 0; i < nReaders; i++) {
            Future f = ex.submit(new Runnable() {
                @Override public void run() {
                    for (int j = 0; j < 10; j++) {
                        if (log.isInfoEnabled())
                            log.info("Iteration " + j);

                        FieldsQueryCursor<List<?>> qry =
                            cache.query(new SqlFieldsQuery("select * from Person limit 10").setLazy(true));
                        qry.getAll();
                        qry.close();
                    }
                }
            });

            futs.add(f);
        }

        for (Future  f : futs)
            f.get(getTestTimeout(), TimeUnit.MILLISECONDS);

        ex.shutdown();
        ex.awaitTermination(getTestTimeout(), TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setCacheConfiguration(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME).setIndexedTypes(Long.class, Person.class));
        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setMetricsEnabled(true)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setMetricsEnabled(true)));
        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 120 * 1000;
    }

    /** */
    private static class Person {
        /** */
        byte[] b;

        /** */
        Person(byte[] b) {
            this.b = b;
        }
    }
}
