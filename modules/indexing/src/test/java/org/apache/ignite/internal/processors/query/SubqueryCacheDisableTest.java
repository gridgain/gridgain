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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.testframework.junits.SystemPropertiesRule;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * Test verifies that optimization can be disabled with system property.
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_H2_OPTIMIZE_REUSE_RESULTS, value = "false")
public class SubqueryCacheDisableTest extends SubqueryCacheAbstractTest {
    /** Class rule. */
    @ClassRule
    public static final TestRule classRule = new SystemPropertiesRule();

    /**
     * Ensure that optimization is disabled when property
     * {@link IgniteSystemProperties#IGNITE_H2_OPTIMIZE_REUSE_RESULTS}
     * is set to {@code false}.
     */
    @Test
    public void testOptimizationDisabled() {
        TestSQLFunctions.CALL_CNT.set(0);

        FieldsQueryCursor<List<?>> res = sql("SELECT * FROM " + OUTER_TBL + " WHERE JID IN (SELECT ID FROM "
            + INNER_TBL + " WHERE det_foo(ID) = ID)");

        Assert.assertEquals(0, TestSQLFunctions.CALL_CNT.get());

        res.getAll();

        Assert.assertEquals(INNER_SIZE * OUTER_SIZE, TestSQLFunctions.CALL_CNT.get());
    }
}
