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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteCachePutKeyAttachedBinaryObjectTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "test";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setCacheConfiguration(new CacheConfiguration<>(CACHE_NAME));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        IgniteCache<BinaryObject, BinaryObject> binCache = grid().cache(CACHE_NAME).withKeepBinary();

        BinaryObjectBuilder keyBuilder = grid().binary().builder(AttachedKey.class.getName());

        keyBuilder.setField("id", 0);

        BinaryObject key = keyBuilder.build();

        // create a new value
        BinaryObjectBuilder empBuilder = grid().binary().builder(ValueWithKey.class.getName());

        empBuilder.setField("val", "val_0", String.class);
        empBuilder.setField("id", key); // The composite key is also a part of the    value !

        BinaryObject emp = empBuilder.build();

        // put the first entry
        binCache.put(emp.field("id"), emp);

        // create a new key / value
        keyBuilder = grid().binary().builder(AttachedKey.class.getName());

        keyBuilder.setField("id", 1, Integer.class);

        key = keyBuilder.build();

        empBuilder = grid().binary().builder(ValueWithKey.class.getName());

        empBuilder.setField("val", "val_1", String.class);
        empBuilder.setField("id", key); // The composite key is also a part of the    value !

        emp = empBuilder.build();

        // put the second entry.
        try {
            binCache.put(emp.field("id"), emp); // CRASH!!! CorruptedTreeException: B+Tree is corrupted
        }
        catch (Exception ignored) {
            // No-op.
        }

        U.sleep(500);

        assertFalse(grid().context().isStopping());
    }


    /**
     *
     */
    public static class AttachedKey {
        /** */
        public int id;
    }

    /**
     *
     */
    public static class ValueWithKey {
        /** */
        public String val;

        /** */
        public AttachedKey key;
    }

}
