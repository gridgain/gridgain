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

import java.util.List;
import java.util.stream.Collectors;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lte;

/** */
public class IndexQueryWrongIndexTest extends GridCommonAbstractTest {
    /** */
    private static final String ID_IDX = "ID_IDX";

    /** */
    private static final String DESC_ID_IDX = "DESC_ID_IDX";

    /** */
    private static IgniteCache<Integer, Person> cache;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        Ignite crd = startGrids(2);

        cache = crd.getOrCreateCache(new CacheConfiguration<Integer, Person>()
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setName("CACHE")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setIndexedTypes(Integer.class, Person.class));
    }

    /** */
    @Test
    public void testNonIndexedFields() {
        IndexQuery<Integer, Person> qry1 = new IndexQuery<Integer, Person>(Person.class, DESC_ID_IDX)
            .setCriteria(lt("id", Integer.MAX_VALUE));

        // Ensures that an exception is not thrown.
        cache.query(qry1).getAll();

        IndexQuery<Integer, Person> qry2 = new IndexQuery<Integer, Person>(Person.class, ID_IDX)
            .setCriteria(lt("descId", Integer.MAX_VALUE));

        // Ensures that an exception is not thrown.
        cache.query(qry2).getAll();
    }

    /** */
    @Test
    public void testSimilarIndexName() {
        cache.query(new SqlFieldsQuery("create index \"aA\" on Person (descId);")).getAll();
        cache.query(new SqlFieldsQuery("create index \"AA\" on Person (id);")).getAll();
        cache.query(new SqlFieldsQuery("create index \"Aa\" on Person (descId desc);")).getAll();

        cache.query(new SqlFieldsQuery("insert into Person (_KEY, id, descId) values (1, 2, 1);")).getAll();
        cache.query(new SqlFieldsQuery("insert into Person (_KEY, id, descId) values (2, 1, 2);")).getAll();
        cache.query(new SqlFieldsQuery("insert into Person (_KEY, id, descId) values (3, 3, 3);")).getAll();

        checkIndex("aA", "descId", F.asList(1, 2, 3));
        checkIndex("AA", "id", F.asList(2, 1, 3));
        checkIndex("Aa", "descId", F.asList(3, 2, 1));
        checkIndex("aa", "id", F.asList(2, 1, 3));
    }

    /** */
    private void checkIndex(String idxName, String fldName, List<Integer> expectedKeys) {
        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, idxName)
            .setCriteria(lte(fldName, Integer.MAX_VALUE));

        assertEquals(expectedKeys,
            cache.query(qry).getAll().stream().map(Cache.Entry::getKey).collect(Collectors.toList()));
    }

    /** */
    private static class Person {
        /** */
        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = ID_IDX, order = 0))
        final int id;

        /** */
        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = DESC_ID_IDX, order = 0))
        final int descId;

        /** */
        Person(int id) {
            this.id = id;
            descId = id;
        }
    }
}
