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

public class AffinityColumnArbitraryTypeTest extends AbstractAffinityColumnArbitraryTypeTest {

    /**
     * Ok
     */
    @Test
    public void testInsert() throws Exception {
        insert(0);
        logAndAssertTable(1);
    }

    /**
     * Ok
     */
    @Test
    public void testPut() throws Exception {
        putBinary(123);
        logAndAssertTable(1);
    }

    /**
     * Ok
     */
    @Test
    public void testPutFirst() throws Exception {
        putBinary(0);
        insert(1);
        logAndAssertTable(2);
    }

    /**
     * Ok
     */
    @Test
    public void testInsertFirst() throws Exception {
        insert(0);
        putBinary(1);
        logAndAssertTable(2);
    }

    /**
     * Ok
     */
    @Test
    public void testConcurrentWritesToDifferentCachesWithInsertFirst() throws Exception {
        fooTable.insert(0);
        barTable.putBinary(0);
        fooTable.logAndAssertTable(1);
    }

    /**
     * Ok
     */
    @Test
    public void testConcurrentWritesDifferentCachesWithPutFirst() throws Exception {
        fooTable.insert(0);
        barTable.insert(0);
        fooTable.logAndAssertTable(1);
    }
}
