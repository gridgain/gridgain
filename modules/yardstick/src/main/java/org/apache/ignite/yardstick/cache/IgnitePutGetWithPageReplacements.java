/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.yardstick.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.ignite.events.EventType.EVT_PAGE_REPLACEMENT_STARTED;

/**
 * Ignite benchmark that performs payload with active page replacement.
 */
public class IgnitePutGetWithPageReplacements extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** Cache name. */
    private static final String CACHE_NAME = "CacheWithReplacement";

    /** Cache configuration */
    private CacheConfiguration<Integer, Object> cacheWithRep = new CacheConfiguration<>(CACHE_NAME);

    /** Active replacement flag. */
    boolean replacement;

    /** Payload counter. */
    int progress;

    /** In mem reg capacity. */
    int replCntr;

    /** End of replacement payload flag. */
    boolean lessRegionSizePayload;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        ignite().events().remoteListen(new IgniteBiPredicate<UUID, Event>() {
            @Override public boolean apply(UUID uuid, Event evt) {
                if (evt.type() == EVT_PAGE_REPLACEMENT_STARTED)
                    replacement = true;

                return true;
            }
        }, null, EVT_PAGE_REPLACEMENT_STARTED);
    }

    /** */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().getOrCreateCache(cacheWithRep);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        int portion = 100;

        Map<Integer, TestValue> putMap = new HashMap<>(portion, 1.f);

        if (replacement) {
            BenchmarkUtils.println("Replacement triggered.");

            replCntr = progress + portion;

            replacement = false;
        }

        if (replCntr != 0 && progress > 2 * replCntr) {
            BenchmarkUtils.println("DataRegion fullfill complete. progress=" + progress + " replCntr=" + replCntr + ".");

            lessRegionSizePayload = true;

            int cacheSize = 0;

            try (QueryCursor cursor = cache.query(new ScanQuery())) {
                for (Object o : cursor)
                    cacheSize++;
            }

            BenchmarkUtils.println("cache size=" + cacheSize);
        }

        for (int i = 0; i < portion; i++) {
            if (lessRegionSizePayload) {
                if (progress > replCntr / 2)
                    progress = 0;
            }

            putMap.put(progress + i, new TestValue(progress + i));
        }

        progress += portion;

        if (progress % 1000 == 0)
            BenchmarkUtils.println("progress=" + progress);

        cache().putAll(putMap);

        return true;
    }

    /**
     * Class for test purpose.
     */
    private static class TestValue {
        /** */
        private int id;

        /** */
        @QuerySqlField(index = true)
        private final byte[] payload = new byte[64];

        /**
         * @param id ID.
         */
        private TestValue(int id) {
            this.id = id;
        }

        /**
         * @return ID.
         */
        public int getId() {
            return id;
        }

        /**
         * @return Payload.
         */
        public boolean hasPayload() {
            return payload != null;
        }
    }
}
