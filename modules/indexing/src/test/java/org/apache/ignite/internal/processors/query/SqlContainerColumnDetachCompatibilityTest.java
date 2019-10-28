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


package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2JavaObject;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** */
public class SqlContainerColumnDetachCompatibilityTest extends AbstractIndexingCommonTest {
    /** */
    private boolean disableDetachOnRemoteJvm;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setIndexedTypes(Integer.class, EntryValue.class));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmArgs() {
        return Collections.singletonList("-DDISABLE_OBJECTS_IN_CONTAINER_DETACH=" + disableDetachOnRemoteJvm);
    }

    /** */
    @Test
    public void testLocalDetachRemoteNoDetach() throws Exception {
        disableDetachOnRemoteJvm = true;

        doTest();
    }

    /** */
    @Test
    public void testLocalNoDetachRemoteDetach() throws Exception {
        disableDetachOnRemoteJvm = false;

        GridTestUtils.setFieldValue(null, GridH2JavaObject.class, "DISABLE_OBJECTS_IN_CONTAINER_DETACH", true);

        try {
            doTest();
        }
        finally {
            GridTestUtils.setFieldValue(null, GridH2JavaObject.class, "DISABLE_OBJECTS_IN_CONTAINER_DETACH", false);
        }
    }

    /** */
    private void doTest() throws Exception {
        // local
        IgniteEx loc = startGrid(0);

        // remote
        startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = loc.cache(DEFAULT_CACHE_NAME);

        Set<Holder> saved = Collections.singleton(new Holder(new byte[100]));

        Integer key = null;

        for (int i = 0; i < 100; i++) {
            // search for a key for remote node
            if (!loc.context().affinity().mapKeyToNode(DEFAULT_CACHE_NAME, i).id().equals(loc.localNode().id())) {
                key = i;

                break;
            }
        }

        assert key != null;

        cache.put(key, new EntryValue(saved));

        Set<Holder> loaded = (Set<Holder>)cache.query(new SqlFieldsQuery("select payload from EntryValue")).getAll().get(0).get(0);

        assertEquals(saved, loaded);
    }

    /** */
    private static class EntryValue {
        /** */
        @QuerySqlField
        Object payload;

        /** */
        public EntryValue(Object payload) {
            this.payload = payload;
        }
    }

    /** */
    private static class Holder {
        /** */
        byte[] x;

        /** */
        public Holder(byte[] x) {
            this.x = x;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Holder holder = (Holder)o;

            return Arrays.equals(x, holder.x);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Arrays.hashCode(x);
        }
    }
}
