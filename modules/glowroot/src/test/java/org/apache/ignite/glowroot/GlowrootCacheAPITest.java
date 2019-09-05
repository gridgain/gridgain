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

package org.apache.ignite.glowroot;



import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 */
public class GlowrootCacheAPITest extends GridCommonAbstractTest {
    /**
     *
     */
    @Test
    public void testBasicApi() throws Exception {
        try {
            IgniteEx grid = startGrid(0);

            IgniteCache<Object, Object> cache = grid.getOrCreateCache(new CacheConfiguration<Object, Object>(DEFAULT_CACHE_NAME));

            cache.put("1", "1");

            LockSupport.park();
        }
        finally {
            stopAllGrids();
        }
    }
}
