/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.compatibility.sql;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.apache.ignite.compatibility.sql.model.City;
import org.apache.ignite.compatibility.sql.model.Company;
import org.apache.ignite.compatibility.sql.model.Country;
import org.apache.ignite.compatibility.sql.model.Department;
import org.apache.ignite.compatibility.sql.model.ModelFactory;
import org.apache.ignite.compatibility.sql.model.Person;
import org.apache.ignite.compatibility.sql.randomsql.RandomQuerySupplier;
import org.apache.ignite.compatibility.sql.randomsql.Schema;
import org.apache.ignite.compatibility.sql.randomsql.Table;
import org.apache.ignite.compatibility.sql.runner.QueryWithParams;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class RandomQuerySupplierSelfTest extends GridCommonAbstractTest {
    /** Count of queries to generate for every check. */
    private static final int COUNT = 1000;

    /** */
    private static final Schema SCHEMA = new Schema();

    static {
        Stream.of(
            new Person.Factory(),
            new Department.Factory(),
            new Country.Factory(),
            new City.Factory(),
            new Company.Factory()
        )
            .map(ModelFactory::queryEntity)
            .map(Table::new)
            .forEach(SCHEMA::addTable);
    }

    /**
     * Creates two query geneartors with the same seed and checks that
     * generated queries are the same as well.
     */
    @Test
    public void generatorReproducibilityTest() {
        int seed = ThreadLocalRandom.current().nextInt();

        log.info("seed=" + seed);

        RandomQuerySupplier sup1 = new RandomQuerySupplier(SCHEMA, seed);
        RandomQuerySupplier sup2 = new RandomQuerySupplier(SCHEMA, seed);

        for (int i = 0; i < COUNT; i++)
            assertEquals(sup1.get(), sup2.get());
    }

    /**
     * Creates query generator and checks that generated queries are not repeated.
     */
    @Test
    public void generatorRandomnessTest() {
        int seed = ThreadLocalRandom.current().nextInt();

        log.info("seed=" + seed);

        RandomQuerySupplier sup = new RandomQuerySupplier(SCHEMA, seed);

        Set<QueryWithParams> qrys = new HashSet<>(COUNT);

        for (int i = 0; i < COUNT; i++)
            assertTrue(i + "th element already exists", qrys.add(sup.get()));
    }
}
