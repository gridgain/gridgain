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

package org.apache.ignite.internal.processors.cache.local;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Isolated nodes LOCAL cache self test.
 */
public class GridCacheLocalIsolatedNodesSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public GridCacheLocalIsolatedNodesSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.LOCAL_CACHE);

        startGrids(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testIsolatedNodes() throws Exception {
        Ignite g1 = grid(0);
        Object nid1 = g1.cluster().localNode().consistentId();

        Ignite g2 = grid(1);
        Object nid2 = g2.cluster().localNode().consistentId();

        Ignite g3 = grid(2);
        Object nid3 = g3.cluster().localNode().consistentId();

        assert !nid1.equals(nid2);
        assert !nid1.equals(nid3);

        // Local cache on first node only.
        CacheConfiguration<String, String> ccfg1 = new CacheConfiguration<>("A1");
        ccfg1.setCacheMode(LOCAL);
        ccfg1.setNodeFilter(new NodeConsistentIdFilter(nid1));

        IgniteCache<String, String> c1 = g1.createCache(ccfg1);
        c1.put("g1", "c1");

        // Local cache on second node only.
        CacheConfiguration<String, String> ccfg2 = new CacheConfiguration<>("A2");
        ccfg2.setCacheMode(LOCAL);
        ccfg2.setNodeFilter(new NodeConsistentIdFilter(nid2));

        IgniteCache<String, String> c2 = g2.createCache(ccfg2);
        c2.put("g2", "c2");

        // Local cache on third node only.
        CacheConfiguration<String, String> ccfg3 = new CacheConfiguration<>("A3");
        ccfg3.setCacheMode(LOCAL);
        ccfg3.setNodeFilter(new NodeConsistentIdFilter(nid3));

        IgniteCache<String, String> c3 = g3.createCache(ccfg3);
        c3.put("g3", "c3");

        assertNull(c1.get("g2"));
        assertNull(c1.get("g3"));
        assertNull(c2.get("g1"));
        assertNull(c2.get("g3"));
        assertNull(c3.get("g1"));
        assertNull(c3.get("g2"));
    }

    /** Filter by consistent id. */
    private static class NodeConsistentIdFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private final Object consistentId;

        /**
         * @param consistentId Consistent id where cache should be started.
         */
        NodeConsistentIdFilter(Object consistentId) {
            this.consistentId = consistentId;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode n) {
            return n.consistentId().equals(consistentId);
        }
    }
}
