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

package org.apache.ignite.yardstick.cache;

import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.yardstick.cache.model.Person;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs query operations.
 */
public class IgniteSqlQueryBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** Last salary. */
    private static final String LAST_SALARY = "LAST_SALARY";

    /** Salary step. */
    private static final int SALARY_STEP = 1000;

    /** */
    private static final int SALARY_RANGE = 10 * SALARY_STEP;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        loadCachesData();
    }

    /** {@inheritDoc} */
    @Override protected void loadCacheData(String cacheName) {
        try (IgniteDataStreamer<Integer, Person> dataLdr = ignite().dataStreamer(cacheName)) {
            for (int i = 0; i < args.range(); i++) {
                if (i % 100 == 0 && Thread.currentThread().isInterrupted())
                    break;

                dataLdr.addData(i, new Person(i, "firstName" + i, "lastName" + i, i * SALARY_STEP));

                if (i % 100000 == 0)
                    println(cfg, "Populated persons: " + i);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        double minSalary = (double)ctx.getOrDefault(LAST_SALARY, 0.0);

        double maxSalary = minSalary + SALARY_RANGE;

        List<List<?>> rows = executeQuery(minSalary, maxSalary);

        for (List<?> row : rows) {
            double salary = (Double)row.get(0);

            if (salary < minSalary || salary > maxSalary)
                throw new Exception("Invalid person retrieved [min=" + minSalary + ", max=" + maxSalary +
                        ", salary=" + salary + ']');
        }

        double newMinSalary =  maxSalary - SALARY_RANGE * 1.0 / 2;

        if (newMinSalary + SALARY_RANGE > args.range() * SALARY_STEP)
            newMinSalary = 0.0;

        ctx.put(LAST_SALARY, newMinSalary);

        return true;
    }

    /**
     * @param minSalary Min salary.
     * @param maxSalary Max salary.
     * @return Query result.
     * @throws Exception If failed.
     */
    private List<List<?>> executeQuery(double minSalary, double maxSalary) {
        IgniteCache<Integer, Object> cache = cacheForOperation(true);

        SqlFieldsQuery qry = new SqlFieldsQuery("select salary, id, firstName, lastName from Person where salary >= ? and salary <= ?");

        qry.setArgs(minSalary, maxSalary);

        return cache.query(qry).getAll();
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("query");
    }
}
