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

package org.apache.ignite.sqltests.affinity.arbitrary;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.sqltests.affinity.AbstractAffinityColumnTest;
import org.junit.Test;

public class AffinityColumnArbitraryTypeTest extends AbstractAffinityColumnTest {

    static final String KEY_TYPE = "ACME_KEY_TYPE";
    static final String VAL_TYPE = FOO_TABLE;

    @Override protected String getKeyType() {
        return KEY_TYPE;
    }

    @Override protected String getValType() {
        return VAL_TYPE;
    }

    @Override protected Object genKey(long id) {
        // actually we're not going to use this method
        return genBinaryKey(id);
    }

    @Override protected void put(String cache, long id) {
        putBinary(cache, id);
    }

    @Override protected void createTables() {
        super.createTables();
    }

    /**
     * This will throw exception:
     * Binary type has different affinity key fields [typeName=... affKeyFieldName1=null, affKeyFieldName2=GROUPID]
     */
    @Test
    public void testInsert() throws Exception {
        insert(0);
        put(1);
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
     * If we make a 'put' first further inserts will also work
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

        try (IgniteDataStreamer<Object, BinaryObject> ds = ignite(0).dataStreamer(FOO_CACHE);) {
            ds.addData(genBinaryKey(4), genBinaryVal(4));
            ds.addData(genBinaryKey(5), genBinaryVal(5));
            ds.flush();
        }

        put(6);
        insert(7);

        logAndAssertTable(8);
    }

    /**
     * Data streamers also work
     */
    @Test
    public void testDataStreamAndInsert() throws Exception {
        String qry = createQry(FOO_TABLE, FOO_CACHE, backups);
        System.out.println("\n\n>>> " + qry);

        defaultCache().query(new SqlFieldsQuery(qry));

        IgniteDataStreamer<BinaryObject, BinaryObject> ds = getDs();
        ds.addData(genBinaryKey(0), genBinaryVal(0));
        //ds.flush();
        ds.close();

        insert(1);
        put(2);

        logAndAssertTable(3);
    }

    public IgniteDataStreamer<BinaryObject, BinaryObject> getDs() {
        IgniteDataStreamer<BinaryObject, BinaryObject> streamer = ignite(0).dataStreamer(FOO_CACHE);
        streamer.perNodeParallelOperations(10);
        streamer.autoFlushFrequency(1000);
        streamer.allowOverwrite(true);
        return streamer;
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
