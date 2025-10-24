/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.cache;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.local.atomic.GridLocalAtomicCache;
import org.apache.ignite.testframework.MemorizingAppender;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.log4j.Level;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Tests that local cache usage causes warnings.
 */
public class LocalCacheWarningsTest extends GridCommonAbstractTest {
    /** Log4j Appender to peek at logging. */
    private final MemorizingAppender appender = new MemorizingAppender();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        appender.installSelfOn(GridLocalAtomicCache.class);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        appender.removeSelfFromEverywhere();

        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void localCacheStartProducesWarning() throws Exception {
        IgniteEx node = startGrid("first");

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        ccfg.setCacheMode(LOCAL);

        node.createCache(ccfg);

        assertWarningIsLogged();
    }

    /** */
    private void assertWarningIsLogged() {
        assertTrue(
            appender.events().stream()
                .anyMatch(event -> {
                    String message = event.getRenderedMessage();
                    return event.getLevel() == Level.WARN
                        && message.contains("LOCAL cache mode is deprecated.")
                        && message.contains("[cacheName=" + DEFAULT_CACHE_NAME + "]");
                })
        );
    }
}
