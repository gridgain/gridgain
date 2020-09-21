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

package org.apache.ignite.internal;

import java.lang.reflect.Constructor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * Using cache API in P2P closure.
 */
public class P2PCacheOperationIntoCouputeTest extends GridCommonAbstractTest {

    /** Person class name. */
    public static final String PERSON_CLASS_NAME = "org.apache.ignite.tests.p2p.cache.Person";

    /** Closure class name. */
    public static final String AVARAGE_PERSON_SALERY_CLOSURE_NAME = "org.apache.ignite.tests.p2p.compute.AvaragePersonSaleryCallable";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME));
    }

    @Test
    public void test() throws Exception {
        Constructor personCtor = getExternalClassLoader().loadClass(PERSON_CLASS_NAME).getConstructor(String.class);

        IgniteCallable<Double> avgSaleryClosure = (IgniteCallable<Double>)getExternalClassLoader().loadClass(AVARAGE_PERSON_SALERY_CLOSURE_NAME)
            .getConstructor(String.class, int.class, int.class).newInstance(DEFAULT_CACHE_NAME, 0, 10);

        Ignite ignite0 = startGrids(2);

        awaitPartitionMapExchange();

        Ignite client = startClientGrid(2);

        IgniteCache cache = client.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 10; i++)
            cache.put(i, createPerson(personCtor, i));

        Double avg = client.compute().call(avgSaleryClosure);

        info("Avarege salery is " + avg);
    }

    /**
     * Creates a new persone instance.
     *
     * @param personConst Constructor.
     * @param id Person id.
     * @return A person instance.
     * @throws Exception If failed.
     */
    @NotNull private Object createPerson(Constructor personConst, int id) throws Exception {
        Object person = personConst.newInstance("Person" + id);
        GridTestUtils.setFieldValue(person, "id", id);
        GridTestUtils.setFieldValue(person, "lastName", "Last name " + id);
        GridTestUtils.setFieldValue(person, "salary", id * Math.PI);
        return person;
    }

}
