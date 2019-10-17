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

import java.util.List;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test that verifies H2 reuse result optimization.
 */
public class SubqueryCacheTest extends SubqueryCacheAbstractTest {
    /**
     * Ensure that optimization works for sub-query in IN clause.
     */
    @Test
    public void testOptimizationInClause() {
        verify("SELECT * FROM " + OUTER_TBL + " WHERE JID IN (SELECT ID FROM "
            + INNER_TBL + " WHERE det_foo(ID) = ID)", INNER_SIZE);
    }

    /**
     * Ensure that optimization doesn't work for sub-query in SELECT expression.
     * May be it is not how it SHOULD work, but it is how it DOES work.
     *
     * NB: H2 closes cached result on each iteration over rows of inner query,
     * so test verifies current behavior.
     */
    @Test
    public void testOptimizationSelectExpr() {
        verify("SELECT (SELECT det_foo(ID) FROM " + INNER_TBL + " WHERE ID = 1) FROM " + OUTER_TBL, OUTER_SIZE);
    }

    /**
     * Ensure that optimization doesn't work for sub-query in table expression.
     * May be it is not how it SHOULD work, but it is how it DOES work.
     *
     * NB: H2 closes cached result on each iteration over rows of inner query,
     * so test verifies current behavior.
     */
    @Test
    public void testOptimizationTblExpr() {
        verify("SELECT * FROM " + OUTER_TBL + ", (SELECT det_foo(A_JID) as I_ID FROM " + INNER_TBL
                + ") WHERE ID = I_ID", (INNER_SIZE + 1) * OUTER_SIZE);
    }

    /**
     * Ensure that optimization not applied if sub-query has non-determenistic func.
     */
    @Test
    public void testOptimizationNotWorkWithNonDetermenisticFunctions() {
        verify("SELECT * FROM " + OUTER_TBL + " WHERE JID IN (SELECT ID FROM "
            + INNER_TBL + " WHERE nondet_foo(ID) = ID)", INNER_SIZE * OUTER_SIZE);
    }

    /**
     * Ensure that optimization not applied to correlated sub-query
     */
    @Test
    public void testOptimizationNotWorkWithCorrelatedQry() {
        verify("SELECT * FROM " + OUTER_TBL + " as O WHERE JID IN (SELECT ID FROM "
            + INNER_TBL + " WHERE det_foo(ID) = ID OR O.ID = -ID)", INNER_SIZE * OUTER_SIZE);
    }

    /**
     * @param qry Query.
     * @param expCallCnt Expected callable count.
     */
    private void verify(String qry, long expCallCnt) {
        TestSQLFunctions.CALL_CNT.set(0);

        FieldsQueryCursor<List<?>> res = sql(qry);

        Assert.assertEquals(0, TestSQLFunctions.CALL_CNT.get());

        res.getAll();

        Assert.assertEquals(expCallCnt, TestSQLFunctions.CALL_CNT.get());
    }
}
