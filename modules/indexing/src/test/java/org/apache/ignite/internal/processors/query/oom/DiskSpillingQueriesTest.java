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

package org.apache.ignite.internal.processors.query.oom;

import java.util.Arrays;
import java.util.Collection;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test for the intermediate query results disk offloading (disk spilling).
 * TODO Refactor AbstractExternalResult
 * TODO Shallow copy of ManagedResult - how to handle memory reserved by the result set detached from the query?
 * TODO Move tracker outside of external result.
 * TODO Memory tracking overhaul - see Andrey's notes in PR.
 * TODO Tests for huge results
 * TODO Unit tests?
 */
@RunWith(Parameterized.class)
public class DiskSpillingQueriesTest extends DiskSpillingAbstractTest {
    /** */
    @Parameterized.Parameter(0)
    public boolean fromClient;

    /** */
    @Parameterized.Parameter(1)
    public boolean loc;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "fromClient={0}, local={1}")
    public static Collection parameters() {
        return Arrays.asList(new Object[][] {
            //client, local
            { false, false },
            { false, true },
            { true,  false },
        });
    }

    /** */
    @Override protected boolean fromClient() {
        return fromClient;
    }

    /** */
    @Override protected boolean localQuery() {
        return loc;
    }

    /** */
    @Test
    public void simpleSelect() {
        assertInMemoryAndOnDiskSameResults(false, "SELECT * FROM person");
    }

    /** */
    @Test
    public void simpleSelectWithSort() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(false, "SELECT * FROM person ORDER BY id DESC");
    }

    /** */
    @Test
    public void simpleSelectWithSortLazy() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(true, "SELECT * FROM person ORDER BY id ASC, code ASC");
    }

    /** */
    @Test
    public void simpleSelectWithDistinct() {
        assertInMemoryAndOnDiskSameResults(false, "SELECT DISTINCT code FROM person");
    }

    /** */
    @Test
    public void simpleSelectWithDistinctLazy() {
        assertInMemoryAndOnDiskSameResults(true, "SELECT DISTINCT depId, code FROM person");
    }

    /** */
    @Test
    public void simpleSelectWithDistinctOrderBy() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(false, "SELECT DISTINCT code FROM person ORDER BY code");
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-21599")
    @Test
    public void simpleSelectWithDistinctOrderByLazy() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(true, "SELECT DISTINCT ON (code, depId) salary  " +
            "FROM person ORDER BY code, salary DESC");
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-21595")
    @Test
    public void simpleSelectWithDistinctOrderByAggregate() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(false, "SELECT DISTINCT code, depId " +
            "FROM person ORDER BY CONCAT(depId, code)");
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-21595")
    @Test
    public void simpleSelectWithDistinctOrderByAggregateLazy() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(true, "SELECT DISTINCT code, depId, temperature " +
            "FROM person ORDER BY CONCAT(depId, code)");
    }

    /** */
    @Test
    public void simpleSubSelect() {
        assertInMemoryAndOnDiskSameResults(false, "SELECT * " +
            "FROM person WHERE depId IN (SELECT id FROM department)");
    }

    /** */
    @Test
    public void simpleSubSelectLazy() {
        assertInMemoryAndOnDiskSameResults(true, "SELECT * " +
            "FROM person WHERE depId IN (SELECT id FROM department) ORDER BY salary DESC");
    }

    /** */
    @Test
    public void simpleSelectWithDistinctOn() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(false, "SELECT DISTINCT ON (code) code, depId " +
            "FROM person ORDER BY code, depId");
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-21599")
    @Test
    public void simpleSelectWithDistinctOnLazy() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(true, "SELECT DISTINCT ON (code, depId) age " +
            "FROM person ORDER BY code, depId");
    }

    /** */
    @Test
    public void simpleJoin() {
        assertInMemoryAndOnDiskSameResults(false, "SELECT p.id, p.name, p.depId, d.title " +
            "FROM person p, department d " +
            " WHERE p.depId = d.id");
    }

    /** */
    @Test
    public void simpleJoinLazy() {
        assertInMemoryAndOnDiskSameResults(true, "SELECT p.id, p.name, p.depId, d.title " +
            "FROM person p, department d " +
            " WHERE p.depId = d.id AND  (p.id > 10 OR p.id < 10000) ORDER BY p.salary DESC OFFSET 10");
    }

    /** */
    @Test
    public void simpleUnion() {
        assertInMemoryAndOnDiskSameResults(false,
            "SELECT id, name, code, depId FROM person WHERE depId < 10 " +
                " UNION " +
                "SELECT id, name, code, depId FROM person WHERE depId > 5 ");
    }

    /** */
    @Test
    public void simpleUnionLazy() {
        assertInMemoryAndOnDiskSameResults(true,
            "(SELECT DISTINCT id, name, code, depId FROM person WHERE depId < 10  ORDER BY id DESC OFFSET 20)" +
                " UNION " +
                "SELECT id, name, code, depId FROM person WHERE depId > 5 ORDER BY id DESC LIMIT 200");
    }

    /** */
    @Test
    public void simpleExcept() {
        assertInMemoryAndOnDiskSameResults(false,
            "SELECT id, name, code, depId FROM person WHERE depId >= 0 " +
            " EXCEPT " +
            "SELECT id, name, code, depId FROM person WHERE depId > 5 ");
    }

    /** */
    @Test
    public void simpleExceptLazy() {
        assertInMemoryAndOnDiskSameResults(true,
            "SELECT id, name, code, depId FROM person WHERE depId >= 1 " +
                " EXCEPT " +
                "SELECT DISTINCT id, name, code, depId FROM person WHERE depId > 5 ");
    }

    /** */
    @Test
    public void simpleIntersect() {
        assertInMemoryAndOnDiskSameResults(false,
            "SELECT id, name, code, depId FROM person WHERE depId < 10 " +
                " INTERSECT " +
                "SELECT id, name, code, depId FROM person WHERE depId > 5 ");
    }

    /** */
    @Test
    public void simpleIntersectLazy() {
        assertInMemoryAndOnDiskSameResults(true,
            "(SELECT id, name, code, depId FROM person WHERE depId < 10 ORDER BY code, id DESC LIMIT 900 OFFSET 10) " +
                " INTERSECT " +
                "SELECT id, name, code, depId FROM person WHERE depId > 5 ");
    }

    /** */
    @Test
    public void simpleSelectLimitOffset() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(false, "SELECT * FROM person ORDER BY name LIMIT 500 OFFSET 100");
    }

    /** */
    @Test
    public void simpleSelectLimitOffsetLazy() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(true, "SELECT * FROM person ORDER BY name LIMIT 800 OFFSET 20");
    }

    /** */
    @Test
    public void simpleSelectOffset() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(false, "SELECT * FROM person ORDER BY name DESC OFFSET 300");
    }

    /** */
    @Test
    public void simpleSelectOffsetLazy() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(true, "SELECT code, male, age, height, salary, tax, weight, " +
            "temperature, time, date, timestamp, uuid FROM person ORDER BY name DESC OFFSET 300");
    }

    /** */
    @Test
    public void simpleSelectLimit() {
        assertInMemoryAndOnDiskSameResults(false, "SELECT * FROM person ORDER BY name LIMIT 700");
    }


    /** */
    @Test
    public void simpleSelectLimitLazy() {
        assertInMemoryAndOnDiskSameResults(true, "SELECT DISTINCT" +
            " code, male, age, height, salary, tax, weight FROM person ORDER BY tax DESC LIMIT 700");
    }


    /** */
    @Test
    public void intersectUnionAllLimitOffset() {
        assertInMemoryAndOnDiskSameResults(false,
            "(SELECT *FROM person WHERE depId < 20 " +
                "INTERSECT " +
                "SELECT * FROM person WHERE depId > 1 )" +
                "UNION ALL " +
                "SELECT * FROM person WHERE age > 50 ORDER BY id LIMIT 1000 OFFSET 50 ");
    }

    /** */
    @Test
    public void intersectUnionAllLimitOffsetLazy() {
        assertInMemoryAndOnDiskSameResults(true,
            "((SELECT * FROM person WHERE depId < 20 ORDER BY depId) " +
                "INTERSECT " +
                "SELECT * FROM person WHERE depId > 10 )" +
                "UNION ALL " +
                "SELECT DISTINCT * FROM person WHERE age <100  ORDER BY id LIMIT 10000 OFFSET 20 ");
    }

    /** */
    @Test
    public void intersectJoinAllLimitOffset() {
        assertInMemoryAndOnDiskSameResults(false,
            "(SELECT DISTINCT p.age FROM person p JOIN department d WHERE d.id = p.depId AND depId < 10 " +
                "UNION " +
                "SELECT MAX(height) FROM person WHERE depId > 5  )" +
                "UNION ALL " +
                "SELECT DISTINCT id FROM person WHERE age < (SELECT SUM(id) FROM department WHERE id > 3)  " +
                "UNION " +
                "SELECT DISTINCT salary FROM person p JOIN department d ON p.age=d.id OR p.weight < 60");
    }

    /** */
    @Test
    public void intersectJoinAllLimitOffsetLazy() {
        assertInMemoryAndOnDiskSameResults(true,
            "(SELECT DISTINCT p.age FROM person p JOIN department d WHERE d.id = p.depId AND depId < 44 " +
                "UNION " +
                "SELECT MAX(height) FROM person WHERE depId > 5  )" +
                "UNION ALL " +
                "SELECT DISTINCT id FROM person WHERE age < (SELECT SUM(id) FROM department WHERE id > 2)  " +
                "EXCEPT " +
                "SELECT DISTINCT salary FROM person p JOIN department d ON p.age=d.id OR p.weight > 10");
    }

    /** */
    @Test
    public void simpleGroupBy() {
        assertInMemoryAndOnDiskSameResults(false,
            "SELECT depId, COUNT(*) FROM person GROUP BY depId");
    }

    /** */
    @Test
    public void simpleGroupByLazy() {
        assertInMemoryAndOnDiskSameResults(true,
            "SELECT depId, COUNT(*), SUM(salary) FROM person GROUP BY depId");
    }
}
