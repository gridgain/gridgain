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

package org.apache.ignite.cache.query;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.between;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.eq;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;

/** */
@RunWith(Parameterized.class)
public class IndexQueryFailoverTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final String IDX = "TEST_IDX";

    /** */
    private static final int CNT = 10_000;

    /** */
    private static IgniteCache<Long, Person> cache;

    /** Query index, {@code null} or index name. */
    @Parameterized.Parameter
    public String qryIdx;

    /** */
    @Parameterized.Parameters(name = "qryIdx={0}")
    public static List<String> params() {
        return F.asList(null, IDX);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Ignite crd = startGrids(2);

        cache = crd.cache(CACHE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Long, Person> ccfg = new CacheConfiguration<Long, Person>()
            .setName(CACHE)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setIndexedTypes(Long.class, Person.class);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** */
    @Test
    public void testQueryWithWrongCriteria() {
        GridTestUtils.assertThrowsAnyCause(null, () -> {
            IndexQuery<Long, Person> qryNullCriteria = new IndexQuery<Long, Person>(Person.class, qryIdx)
                .setCriteria(lt(null, 12));

            return cache.query(qryNullCriteria);
        }, NullPointerException.class, "Ouch! Argument cannot be null: field");
    }

    /** */
    @Test
    public void testQueryWrongType() {
        GridTestUtils.assertThrows(null, () -> new IndexQuery<Long, Integer>((String)null, qryIdx),
            NullPointerException.class, "Ouch! Argument cannot be null: valType");

        GridTestUtils.assertThrows(null, () -> new IndexQuery<Long, Integer>("", qryIdx),
            IllegalArgumentException.class, "Ouch! Argument is invalid: valType must not be empty");

        GridTestUtils.assertThrowsAnyCause(null, () -> {
            IndexQuery<Long, Integer> qry = new IndexQuery<Long, Integer>(Integer.class, qryIdx)
                .setCriteria(lt("id", Integer.MAX_VALUE));

            return cache.query(qry).getAll();
        }, IgniteException.class, "Failed to find SQL table for type: " + Integer.class.getSimpleName());
    }

    /** */
    @Test
    public void testQueryWrongIndexName() {
        GridTestUtils.assertThrowsAnyCause(null, () -> {
            IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, "")
                .setCriteria(lt("id", Integer.MAX_VALUE));

            return cache.query(qry).getAll();
        }, IllegalArgumentException.class, "Ouch! Argument is invalid: idxName must not be empty.");

        GridTestUtils.assertThrowsAnyCause(null, () -> {
            IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, "DUMMY")
                .setCriteria(lt("id", Integer.MAX_VALUE));

            return cache.query(qry).getAll();
        }, CacheException.class, "Index \"DUMMY\" not found");
    }

    /** */
    @Test
    public void testQueryWrongQuery() {
        GridTestUtils.assertThrowsAnyCause(null, () -> {
            IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, qryIdx)
                .setCriteria(lt("dummy", Integer.MAX_VALUE));

            return cache.query(qry).getAll();
        }, CacheException.class, "Column \"DUMMY\" not found.");

        GridTestUtils.assertThrowsAnyCause(null, () -> {
            IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, qryIdx)
                .setCriteria(
                    lt("id", Integer.MAX_VALUE),
                    lt("nonExistedField", Integer.MAX_VALUE));

            return cache.query(qry).getAll();
        }, IgniteException.class, "Column \"NONEXISTEDFIELD\" not found.");

        {
            IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, qryIdx)
                .setCriteria(between("id", 432, 40));

            // Ensures that there will be no exceptions.
            cache.query(qry).getAll();
        }

        Stream.of(
            Arrays.asList(lt("id", 100), gt("id", 101)),
            Arrays.asList(eq("id", 100), eq("id", 101)),
            Arrays.asList(eq("id", 101), eq("id", 100)),
            Arrays.asList(eq("id", 101), between("id", 19, 40))
        ).forEach(crit -> {
            String msg = "Failed to merge criterion " + crit.get(1).toString().replace("id", "ID")
                + " with previous criteria range " + crit.get(0).toString().replace("id", "ID");

            IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, qryIdx)
                .setCriteria(crit);

            // Ensures that there will be no exceptions.
            cache.query(qry).getAll();
        });
    }

    /** */
    @Test
    public void testConcurrentUpdateIndex() {
        insertData(0, CNT);

        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, qryIdx)
            .setCriteria(between("id", CNT / 2, CNT + CNT / 2));

        Iterator<Cache.Entry<Long, Person>> cursor = cache.query(qry).iterator();

        for (int i = 0; i < CNT / 10; i++)
            cursor.next();

        insertData(CNT, CNT * 2);

        int size = CNT / 10;

        while (cursor.hasNext()) {
            cursor.next();
            size++;
        }

        assertEquals(CNT + 1, size);
    }

    /** */
    private void insertData(int from, int to) {
        for (int i = from; i < to; i++)
            cache.put((long)i, new Person(i));
    }

    /** */
    private static class Person {
        /** */
        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = IDX, order = 0))
        final int id;

        /** */
        @QuerySqlField
        final int descId;

        /** */
        Person(int id) {
            this.id = id;
            this.descId = id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Person[id=" + id + "]";
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person)o;

            return Objects.equals(id, person.id)
                && Objects.equals(descId, person.descId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, descId);
        }
    }
}
