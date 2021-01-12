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

package org.apache.ignite.springdata;

import org.apache.ignite.springdata.misc.ApplicationConfiguration;
import org.apache.ignite.springdata.misc.Person;
import org.apache.ignite.springdata.misc.PersonExpressionRepository;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Test with using repository which is configured by Spring EL
 */
public class IgniteSpringDataCrudSelfExpressionTest extends GridCommonAbstractTest {
    /** Repository. */
    private static PersonExpressionRepository repo;

    /** Context. */
    private static AnnotationConfigApplicationContext ctx;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ctx = new AnnotationConfigApplicationContext();

        ctx.register(ApplicationConfiguration.class);

        ctx.refresh();

        repo = ctx.getBean(PersonExpressionRepository.class);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        assertEquals(0, repo.count());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        repo.deleteAll();

        assertEquals(0, repo.count());

        super.afterTest();
    }

    /**
     * Test that put and get operations are working.
     */
    @Test
    public void testPutGet() {
        assertNotNull(repo);

        Person person = new Person("some_name", "some_surname");

        assertEquals(person, repo.save(0, person));

        assertTrue(repo.existsById(0));

        assertEquals(person, repo.findById(0).get());
    }

    /**
     * Test that saving without an id fails.
     */
    @Test
    public void testFailIfSaveWithoutId() {
        assertNotNull(repo);

        Person person = new Person("some_name", "some_surname");

        try {
            repo.save(person);

            fail("Managed to save a Person without ID");
        }
        catch (UnsupportedOperationException e) {
            //excepted
        }
    }
}
