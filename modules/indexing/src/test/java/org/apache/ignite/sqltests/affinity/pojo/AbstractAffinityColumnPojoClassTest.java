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

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.sqltests.affinity.AbstractAffinityColumnTest;
import org.junit.Test;

/**
 * Base class for all tests where SQL table KV types are the class names of POJOs available in classpath "
 */
public abstract class AbstractAffinityColumnPojoClassTest extends AbstractAffinityColumnTest<AbstractAffinityColumnPojoClassTest.PojoTable> {

    @Override protected String getKeyType() {
        return getKeyCls().getName();
    }

    protected abstract Class<?> getKeyCls();

    protected abstract Object genKey(long id);

    public void put(long id) {
        fooTable.put(id);
    }

    @Override protected void initTables() {
        fooTable = new PojoTable(ignite(0), FOO_TABLE, FOO_CACHE, this);
        barTable = new PojoTable(ignite(0), BAR_TABLE, BAR_CACHE, this);

        fooTable.create(backups);
        barTable.create(backups);
    }

    /**
     * This will throw exception:
     * Binary type has different affinity key fields [typeName=... affKeyFieldName1=null, affKeyFieldName2=GROUPID]
     */
    @Test
    public void testInsert() throws Exception {
        insert(0);
    }

    /**
     * If we make a 'put' first it will work
     */
    @Test
    public void testPut() throws Exception {
        put(0);
        logAndAssertTable(1);
    }

    /**
     * If we make a 'binary put' first it will work
     */
    @Test
    public void testPutBinary() throws Exception {
        putBinary(0);
        logAndAssertTable(1);
    }

    /**
     * If we make a 'put' first further inserts will also work
     */
    @Test
    public void testPutAndInsert() throws Exception {
        put(0);
        insert(1);

        logAndAssertTable(2);
    }

    /**
     * If we make a 'put' first further inserts will also work
     */
    @Test
    public void testInsertAndPut() throws Exception {
        insert(0);
        put(1);

        logAndAssertTable(2);
    }

    /**
     * If we make put first to cache A, tables from other caches also would be safe
     */
    @Test
    public void testInsertFirstAfterPutWasAppliedToDifferentCache() throws Exception {
        fooTable.put(0);
        barTable.insert(0);
    }

    public static class PojoTable extends AbstractAffinityColumnTest.Table {

        private final AbstractAffinityColumnPojoClassTest test;

        public PojoTable(
            IgniteEx ignite,
            String tableName,
            String cacheName,
            AbstractAffinityColumnPojoClassTest test
        ) {
            super(ignite, tableName, cacheName, test.getKeyType());
            this.test = test;
        }

        public void put(long id) {
            getCache().put(test.genKey(id), Val.from(id));
        }

    }

}
