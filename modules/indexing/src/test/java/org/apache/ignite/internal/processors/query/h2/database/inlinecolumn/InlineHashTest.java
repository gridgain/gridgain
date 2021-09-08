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
package org.apache.ignite.internal.processors.query.h2.database.inlinecolumn;

import java.util.HashSet;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE;
import static org.apache.ignite.internal.processors.query.h2.database.H2Tree.IGNITE_THROTTLE_INLINE_SIZE_CALCULATION;

/**
 * Tests that correct warning is printed when hash collision on Java Object PK happened.
 */
public class InlineHashTest extends GridCommonAbstractTest {
    /** */
    private static final String KEY_FIELD = "name";

    /** */
    private static final Pattern WARN_PTRN = Pattern.compile(
        "Indexed columns of a row cannot be fully inlined.*?" +
        "idxCols=\\[\\[column=_KEY, type=19, inlineSize=5\\], \\[column=NAME, type=13, inlineSize=11\\]\\].*?" +
        "idxIncludesInlinedHash=true.*"
    );

    /** */
    private final LogListener logLsnr = LogListener.matches(WARN_PTRN).build();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(new ListeningTestLogger(log).registerListener(logLsnr))
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setQueryEntities(asList(
                    new QueryEntity(KeyClass.class, Integer.class)
                        .setKeyFields(new HashSet<>(asList(KEY_FIELD)))
                ))
                .setKeyConfiguration(new CacheKeyConfiguration(KeyClass.class).setAffinityKeyFieldName(KEY_FIELD))
            );
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_THROTTLE_INLINE_SIZE_CALCULATION, value = "2")
    @WithSystemProperty(key = IGNITE_MAX_INDEX_PAYLOAD_SIZE, value = "8")
    public void test() throws Exception {
        IgniteEx ignite = startGrids(1);

        IgniteCache<KeyClass, Integer> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        // Put objects with colliding hashes.
        cache.put(new KeyClass("k1117323", 1117323), 1);
        cache.put(new KeyClass("k4059899", 4059899), 1);

        assertTrue(logLsnr.check(10_000));
    }

    /**
     * Class for primary key objects.
     */
    private static class KeyClass {
        /** */
        @QuerySqlField
        private final String name;

        /** */
        @QuerySqlField
        private final Integer int0;

        /** */
        public KeyClass(String name, int int0) {
            this.name = name;
            this.int0 = int0;
        }
    }
}
