/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.sqltests.affinity.pojo;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.sqltests.affinity.AbstractAffinityColumnTest;
import org.junit.Test;

public abstract class AffinityColumnPojoClassTest extends AbstractAffinityColumnTest {

    @Override protected String getKeyType() {
        return getKeysCls().getName();
    }

    protected abstract Class<?> getKeysCls();

    /**
     * This will throw exception:
     * Binary type has different affinity key fields [typeName=... affKeyFieldName1=null, affKeyFieldName2=GROUPID]
     */
    @Test
    public void testInsert() throws Exception {
        insert(0);
    }

    /**
     * If make a 'put' first it will work
     */
    @Test
    public void testPut() throws Exception {
        put(0);
        logAndAssertTable(1);
    }

    /**
     * If make a 'put' first it will work
     */
    @Test
    public void testPutBinary() throws Exception {
        putBinary(0);
        logAndAssertTable(1);
    }

    /**
     * If make a 'put' first further inserts will also work
     */
    @Test
    public void testPutAndInsert() throws Exception {
        put(0);
        insert(1);
        insert(2);
        put(3);

        logAndAssertTable(4);
    }

    /**
     * Data streamers also work
     */
    @Test
    public void testPutAndInsertAndDataStream() throws Exception {
        put(0);
        insert(1);
        insert(2);
        put(3);

        try (IgniteDataStreamer<Object, Val> ds = ignite(0).dataStreamer(FOO_CACHE);) {
            ds.addData(genKey(4), Val.from(4));
            ds.addData(genKey(5), Val.from(5));
            ds.flush();
        }

        put(6);
        insert(7);

        logAndAssertTable(8);
    }

    /**
     * This will throw exception:
     * Binary type has different affinity key fields [typeName=... affKeyFieldName1=null, affKeyFieldName2=GROUPID]
     */
    @Test
    public void testConcurrentWritesWithInsertFirst() throws Exception {
        insert(FOO_TABLE, 0);
        put(BAR_CACHE, 0);
    }

    @Test
    public void testConcurrentWritesWithPutFirst() throws Exception {
        put(BAR_CACHE, 0);
        insert(FOO_TABLE, 0);
    }

}
