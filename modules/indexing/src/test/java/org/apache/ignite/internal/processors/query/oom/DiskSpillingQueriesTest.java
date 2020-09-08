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
            { true, false },
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
    @Ignore("https://ggsystems.atlassian.net/browse/GG-21599")
    @Test
    public void simpleSelectWithDistinctOrderByAggregate() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(false, "SELECT DISTINCT code, depId " +
            "FROM person ORDER BY CONCAT(depId, code)");
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-21599")
    @Test
    public void simpleSelectWithDistinctOrderByAggregateLazy() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(true, "SELECT DISTINCT code, depId, temperature " +
            "FROM person ORDER BY CONCAT(depId, code)");
    }

    /** */
    @Test
    public void simpleSubSelectIn() {
        assertInMemoryAndOnDiskSameResults(false, "SELECT * " +
            "FROM person WHERE depId IN (SELECT id FROM department)");
    }

    /** */
    @Test
    public void simpleSubSelectInLazy() {
        assertInMemoryAndOnDiskSameResults(true, "SELECT * " +
            "FROM person WHERE depId IN (SELECT id FROM department) ORDER BY salary DESC");
    }

    /** */
    @Test
    public void simpleSubSelectExists() {
        assertInMemoryAndOnDiskSameResults(false, "SELECT * " +
            "FROM person WHERE EXISTS (SELECT * FROM department)");
    }

    /** */
    @Test
    public void simpleSubSelectExistsLazy() {
        assertInMemoryAndOnDiskSameResults(true, "SELECT * " +
            "FROM person WHERE EXISTS (SELECT * FROM department) ORDER BY salary DESC");
    }

    /** */
    @Test
    public void simpleSubSelect() {
        assertInMemoryAndOnDiskSameResults(false, "SELECT * " +
            "FROM person WHERE age = (SELECT MAX(age) FROM (SELECT * FROM Person WHERE id < 200) WHERE id=age)");
    }

    /** */
    @Test
    public void simpleSubSelectLazy() {
        assertInMemoryAndOnDiskSameResults(true, "SELECT * " +
            "FROM person WHERE EXISTS (SELECT * FROM department) ORDER BY salary DESC");
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

        assertInMemoryAndOnDiskSameResults(false, "SELECT * FROM person ORDER BY name DESC OFFSET 50");
    }

    /** */
    @Test
    public void simpleSelectOffsetLazy() {
        checkSortOrder = true;

        assertInMemoryAndOnDiskSameResults(true, "SELECT code, male, age, height, salary, tax, weight, " +
            "temperature, time, date, timestamp, uuid FROM person ORDER BY name DESC OFFSET 50");
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
                "SELECT DISTINCT salary FROM person p JOIN department d ON p.age=d.id OR p.weight < 60 " +
                "UNION " +
                "SELECT MAX(salary) FROM person WHERE age > 10  GROUP BY temperature");
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
        checkGroupsSpilled = true;

        assertInMemoryAndOnDiskSameResults(false,
            "SELECT age, depId, COUNT(*), MAX(salary), MIN(salary), AVG(salary), SUM(salary) " +
                "FROM person GROUP BY age, depId");
    }

    /** */
    @Test
    public void simpleGroupByLazy() {
        checkGroupsSpilled = true;

        assertInMemoryAndOnDiskSameResults(true,
            "SELECT depId, code, age, COUNT(*), SUM(salary),  LISTAGG(uuid) " +
                "FROM person GROUP BY age, depId, code ");
    }

    /** */
    @Test
    public void groupByWithUnion() {
        checkGroupsSpilled = true;

        assertInMemoryAndOnDiskSameResults(true,
            "SELECT age, code, COUNT(*), AVG(salary) FROM person GROUP BY code, age " +
                "UNION " +
                "SELECT age, name, SUM(age), MIN(salary) FROM person GROUP BY name, age");
    }

    /** */
    @Test
    public void groupByWithExcept() {
        checkGroupsSpilled = true;

        assertInMemoryAndOnDiskSameResults(true,
            "SELECT age, code, COUNT(id) FROM person GROUP BY code, age " +
                "EXCEPT " +
                "SELECT age, name, COUNT(temperature) FROM person GROUP BY name, age");
    }

    /** */
    @Test
    public void simpleGroupByAllSupportedAggregates() {
        checkGroupsSpilled = true;

        assertInMemoryAndOnDiskSameResults(false,
            "SELECT code, age, COUNT(*), COUNT(temperature), AVG(temperature), SUM(temperature), MAX(temperature), " +
                "MIN(temperature), LISTAGG(temperature)  " +
                "FROM person GROUP BY age, code"
            );
    }

    /** */
    @Test
    public void simpleGroupByNullableAggregates() {
        checkGroupsSpilled = true;

        assertInMemoryAndOnDiskSameResults(false,
            "SELECT age, code, COUNT(nulls), COUNT(temperature), AVG(nulls), SUM(temperature), MAX(nulls), " +
                "LISTAGG(temperature) " +
                "FROM person GROUP BY age, code"
        );
    }

    /** */
    @Test
    public void groupByNullableAggregatesBigGroups() {
        checkGroupsSpilled = true;

        listAggs = Arrays.asList(4);

        assertInMemoryAndOnDiskSameResults(false,
            "SELECT male, COUNT(nulls), COUNT(temperature), AVG(nulls), LISTAGG(temperature) " +
                "FROM person GROUP BY male"
        );
    }

    /** */
    @Test
    public void groupByNullableAggregatesManySmallGroups() {
        checkGroupsSpilled = true;

        assertInMemoryAndOnDiskSameResults(false,
            "SELECT weight, age, COUNT(nulls), COUNT(temperature), AVG(temperature), SUM(temperature),  LISTAGG(name) " +
                "FROM person GROUP BY age, weight"
        );
    }

    /** */
    @Test
    public void groupBySNullsAggregates() {
        checkGroupsSpilled = true;

        assertInMemoryAndOnDiskSameResults(false,
            "SELECT depId, age, COUNT(nulls), AVG(nulls), LISTAGG(nulls) " +
                "FROM person GROUP BY age, depId");
    }

    /** */
    @Test
    public void groupByAllSupportedDistinctAggregates() {
        checkGroupsSpilled = true;

        assertInMemoryAndOnDiskSameResults(false,
            "SELECT code, age, AVG(DISTINCT temperature), SUM(DISTINCT temperature), MAX(DISTINCT temperature), " +
                "MIN(DISTINCT temperature)  " +
                "FROM person GROUP BY age, code"
        );
    }

    /** */
    @Test
    public void groupByNullableDistinctAggregates() {
        checkGroupsSpilled = true;

        assertInMemoryAndOnDiskSameResults(false,
            "SELECT age, code, COUNT(DISTINCT nulls), COUNT(DISTINCT temperature), AVG(DISTINCT nulls), " +
                "SUM(DISTINCT temperature), MAX(DISTINCT nulls), LISTAGG(temperature) " +
                "FROM person GROUP BY age, code"
        );
    }

    /** */
    @Test
    public void groupByValuesWithHaving() {
        checkGroupsSpilled = true;

        listAggs = Arrays.asList(2);

        assertInMemoryAndOnDiskSameResults(false,
            "SELECT temperature, count(*), LISTAGG(name) " +
                "FROM person GROUP BY temperature HAVING temperature > 38"
        );
    }

    /** */
    @Test
    public void simpleAggregate() {
        checkGroupsSpilled = true;

        listAggs = Arrays.asList(1, 2, 3, 4);

        assertInMemoryAndOnDiskSameResults(false,
            "SELECT count(*), LISTAGG(name), LISTAGG(name), LISTAGG(name), LISTAGG(name) FROM person"
        );
    }
}
