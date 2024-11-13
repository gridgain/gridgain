package org.apache.ignite.springdata;

import java.time.LocalDateTime;
import org.apache.ignite.springdata.misc.ApplicationConfiguration;
import org.apache.ignite.springdata.misc.CustomConvertersApplicationConfiguration;
import org.apache.ignite.springdata.misc.Person;
import org.apache.ignite.springdata.misc.PersonRepository;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class IgniteSpringDataConversionsTest extends GridCommonAbstractTest {

    /** Repository. */
    protected PersonRepository repo;

    /** Context. */
    protected static AnnotationConfigApplicationContext ctx;

    @Test
    public void testPutGetWithDefaultConverters() {
        ctx = new AnnotationConfigApplicationContext();
        ctx.register(ApplicationConfiguration.class);
        ctx.refresh();
        repo = ctx.getBean(PersonRepository.class);

        Person person = new Person("some_name", "some_surname", LocalDateTime.now());

        int id = 1;

        assertEquals(person, repo.save(id, person));

        assertTrue(repo.existsById(id));

        assertEquals(person, repo.findWithCreatedAt(person.getFirstName()).get(0));

        try {
            repo.save(person);

            fail("Managed to save a Person without ID");
        }
        catch (UnsupportedOperationException e) {
            //excepted
        }
    }

    @Test
    public void testPutGetWithCustomConverters() {
        ctx = new AnnotationConfigApplicationContext();
        ctx.register(CustomConvertersApplicationConfiguration.class);
        ctx.refresh();
        repo = ctx.getBean(PersonRepository.class);

        Person person = new Person("some_name", "some_surname", LocalDateTime.now());

        int id = 1;

        assertEquals(person, repo.save(id, person));

        assertTrue(repo.existsById(id));

        Person actual = repo.findWithCreatedAt(person.getFirstName()).get(0);
        assertNull(actual.getCreatedAt());

        try {
            repo.save(person);

            fail("Managed to save a Person without ID");
        }
        catch (UnsupportedOperationException e) {
            //excepted
        }
    }

    @Override protected void afterTest() {
        ctx.close();
    }
}
