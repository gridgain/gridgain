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

package org.apache.ignite.internal.processors.cache.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import javax.cache.CacheException;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests for query entity validation.
 */
public class QueryEntityValidationSelfTest extends AbstractIndexingCommonTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /**
     * Create table with wrapped key and user value type and insert value by cache API.
     * Check inserted value.
     * @throws Exception In case of errors.
     */
    @Test
    public void testWrappedKeyValidation() throws Exception {
        IgniteCache c1 = ignite(0).getOrCreateCache("WRAP_KEYS");
        c1.query(new SqlFieldsQuery("CREATE TABLE TestKeys (\n" +
            "  namePK varchar primary key,\n" +
            "  notUniqueId int\n" +
            ") WITH \"wrap_key=true," +
            "value_type=org.apache.ignite.internal.processors.cache.index.QueryEntityValidationSelfTest$TestKey\""))
            .getAll();

        IgniteCache<String, TestKey> keys = ignite(0).cache("SQL_PUBLIC_TESTKEYS");
        TestKey k1 = new TestKey(1);

        keys.put("1", k1);

        TestKey rk1 = keys.get("1");

        assertEquals(k1, rk1);
    }

    /**
     * Test null value type.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testValueTypeNull() throws Exception {
        final CacheConfiguration ccfg = new CacheConfiguration().setName(CACHE_NAME);

        QueryEntity entity = new QueryEntity();

        entity.setKeyType("Key");

        ccfg.setQueryEntities(Collections.singleton(entity));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                grid(0).createCache(ccfg);

                return null;
            }
        }, IgniteCheckedException.class, "Value type cannot be null or empty");
    }

    /**
     * Test failure if index type is null.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIndexTypeNull() throws Exception {
        final CacheConfiguration ccfg = new CacheConfiguration().setName(CACHE_NAME);

        QueryEntity entity = new QueryEntity();

        entity.setKeyType("Key");
        entity.setValueType("Value");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("a", Integer.class.getName());

        entity.setFields(fields);

        LinkedHashMap<String, Boolean> idxFields = new LinkedHashMap<>();

        idxFields.put("a", true);

        QueryIndex idx = new QueryIndex().setName("idx").setFields(idxFields).setIndexType(null);

        List<QueryIndex> idxs = new ArrayList<>();

        idxs.add(idx);

        entity.setIndexes(idxs);

        ccfg.setQueryEntities(Collections.singleton(entity));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                grid(0).createCache(ccfg);

                return null;
            }
        }, IgniteCheckedException.class, "Index type is not set");
    }

    /**
     * Test duplicated index name.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIndexNameDuplicate() throws Exception {
        final CacheConfiguration ccfg = new CacheConfiguration().setName(CACHE_NAME);

        QueryEntity entity = new QueryEntity();

        entity.setKeyType("Key");
        entity.setValueType("Value");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("a", Integer.class.getName());
        fields.put("b", Integer.class.getName());

        entity.setFields(fields);

        LinkedHashMap<String, Boolean> idx1Fields = new LinkedHashMap<>();
        LinkedHashMap<String, Boolean> idx2Fields = new LinkedHashMap<>();

        idx1Fields.put("a", true);
        idx1Fields.put("b", true);

        QueryIndex idx1 = new QueryIndex().setName("idx").setFields(idx1Fields);
        QueryIndex idx2 = new QueryIndex().setName("idx").setFields(idx2Fields);

        List<QueryIndex> idxs = new ArrayList<>();

        idxs.add(idx1);
        idxs.add(idx2);

        entity.setIndexes(idxs);

        ccfg.setQueryEntities(Collections.singleton(entity));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                grid(0).createCache(ccfg);

                return null;
            }
        }, IgniteCheckedException.class, "Duplicate index name");
    }

    /**
     * Test class for sql queryable test key
     */
    private static class TestKey {

        /**
         * Not unique id
         */
        @QuerySqlField
        int notUniqueId;

        public TestKey(int notUniqueId) {
            this.notUniqueId = notUniqueId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestKey testKey = (TestKey) o;

            return notUniqueId == testKey.notUniqueId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(notUniqueId);
        }
    }

    /**
     * Test class for sql queryable test value
     */
    private static class TestValue {

        /**
         * Field with nested queryable field
         */
        @QuerySqlField
        TestValueField field;

    }

    /**
     * Test class for nested sql queryable field
     */
    private static class TestValueField {

        /**
         * Not unique id
         */
        @QuerySqlField
        int notUniqueId;

    }

    /**
     * Test duplicated nested annotations
     */
    @Test
    public void testNestedDuplicatedAnnotations() {
        final CacheConfiguration<TestKey, TestValue> ccfg = new CacheConfiguration<TestKey, TestValue>().setName(CACHE_NAME);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() {
                ccfg.setIndexedTypes(TestKey.class, TestValue.class);

                return null;
            }
        }, CacheException.class, "Property with name 'notUniqueId' already exists");
    }

    /**
     * Test class for sql queryable test key with unique annotation's name property.
     */
    private static class TestKeyWithUniqueName {
        /** Non-unique id. */
        @QuerySqlField(name = "Name1")
        int notUniqueId;
    }

    /**
     * Test class for sql queryable test value with unique annotation's name property.
     */
    private static class TestValueWithUniqueName {
        /** Not unique id. */
        @QuerySqlField(name = "Name2")
        int notUniqueId;
    }

    /**
     * Test to check validation of known fields names with unique QuerySqlField annotation's name properties
     *
     * Steps:
     * 1) Create 2 classes with same field name, but with different name property for QuerySqlField annotation
     * 2) Check that CacheConfiguration.setIndexedTypes() works correctly
     */
    @Test
    public void testUniqueNameInAnnotation() {
        final CacheConfiguration<TestKeyWithUniqueName, TestValueWithUniqueName> ccfg = new CacheConfiguration<TestKeyWithUniqueName, TestValueWithUniqueName>().setName(CACHE_NAME);

        assertNotNull(ccfg.setIndexedTypes(TestKeyWithUniqueName.class, TestValueWithUniqueName.class));
    }

    /**
     * Test class for sql queryable test key with not unique annotation's name property.
     */
    private static class TestKeyWithNotUniqueName {
        /** Unique id. */
        @QuerySqlField(name = "Name3")
        int uniqueId1;
    }

    /**
     * Test class for sql queryable test value with not unique annotation's name property.
     */
    private static class TestValueWithNotUniqueName {
        /** Unique id. */
        @QuerySqlField(name = "Name3")
        int uniqueId2;
    }

    /**
     * Test to check validation of known fields names with not unique QuerySqlField annotation's name properties
     *
     * Steps:
     * 1) Create 2 classes with different field names and with same name property for QuerySqlField annotation
     * 2) Check that CacheConfiguration.setIndexedTypes() fails with "Property with name ... already exists" exception
     */
    @Test
    public void testNotUniqueNameInAnnotation() {
        final CacheConfiguration<TestKeyWithNotUniqueName, TestValueWithNotUniqueName> ccfg = new CacheConfiguration<TestKeyWithNotUniqueName, TestValueWithNotUniqueName>().setName(CACHE_NAME);

        GridTestUtils.assertThrows(log, (Callable<Void>)() -> {
            ccfg.setIndexedTypes(TestKeyWithNotUniqueName.class, TestValueWithNotUniqueName.class);

            return null;
        }, CacheException.class, "Property with name 'Name3' already exists");
    }
}
