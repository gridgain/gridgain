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

import org.junit.Test;

public class AffinityColumnArbitraryTypeAndFieldsNameMismatchTest extends AbstractAffinityColumnArbitraryTypeTest {

    /** OK */
    @Test
    public void testInsertFirstNormal() throws Exception {
        testInsertFirst(ID_FIELD, GROUP_ID_FIELD);
    }

    /** OK */
    @Test
    public void testInsertFirstLower() throws Exception {
        testInsertFirst(ID_FIELD, "groupid");
    }

    /** OK */
    @Test
    public void testInsertFirstCapitalized() throws Exception {
        testInsertFirst(ID_FIELD, "Groupid");
    }

    /**
     * Both write operations are successful, but 2 versions of each field created:
     * <li>
     *     <ul>USERID</ul>
     *     <ul>GROUPID</ul>
     *     <ul>groupid</ul>
     *     <ul>userId</ul>
     * </li>
     */
    @Test
    public void testInsertFirstInverseOrder() throws Exception {
        testInsertFirst(GROUP_ID_FIELD, ID_FIELD);
    }

    /** Fail, see {@link #testInsertFirstInverseOrder()} */
    @Test
    public void testInsertFirstInverseOrderLowerLower() throws Exception {
        testInsertFirst("groupid", ID_FIELD);
    }

    /** Fail, see {@link #testInsertFirstInverseOrder()} */
    @Test
    public void testInsertFirstInverseOrderCap() throws Exception {
        testInsertFirst("Groupid", ID_FIELD);
    }

    public void testInsertFirst(String f1, String f2) throws Exception {
        insert(0);
        putBinaryVerbose(1, f1, f2);
        logAndAssertTable(2);
        assertBinaryMeta();
    }

    /**
     * Both write operations are successful, but affinity key of binary metadata is <code>null</code>
     */
    @Test
    public void testPutFirstWritesOk() throws Exception {
        testPutFirst(ID_FIELD, GROUP_ID_FIELD);
    }

    /** Fail, see {@link #testPutFirstWritesOk()} */
    @Test
    public void testPutFirstLower() throws Exception {
        testPutFirst(ID_FIELD, "groupid");
    }

    /** Fail, see {@link #testPutFirstWritesOk()} */
    @Test
    public void testPutFirstNormalCapitalized() throws Exception {
        testPutFirst(ID_FIELD, "Groupid");
    }

    /** Fails, with BinaryObjectException because of binary meta affinityKey mismatch */
    @Test
    public void testPutFirstInverseOrder() throws Exception {
        testPutFirst(GROUP_ID_FIELD, ID_FIELD);
    }

    /** Fail, see {@link #testPutFirstInverseOrder()} */
    @Test
    public void testPutFirstInverseOrderLower() throws Exception {
        testPutFirst("groupid", ID_FIELD);
    }

    /** Fail, see {@link #testPutFirstInverseOrder()} */
    @Test
    public void testPutFirstInverseOrderCapitalized() throws Exception {
        testPutFirst("Groupid", ID_FIELD);
    }

    public void testPutFirst(String f1, String f2) throws Exception {
        putBinaryVerbose(0, f1, f2);
        insertVerbose(1);
        logAndAssertTable(2);
        assertBinaryMeta();
    }

    protected void putBinaryVerbose(long id, String firstField, String secondField) {
        try {
            fooTable.putBinary(id, firstField, secondField);
        }
        catch (Exception e) {
            dumpBinaryMeta();
            throw e;
        }
    }

    protected void insertVerbose(long id) {
        try {
            insert(id);
        }
        catch (Exception e) {
            dumpBinaryMeta();
            throw e;
        }
    }
}
