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

package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for correct distributed sql joins.
 */
public class IgniteSqlDistributedJoin2SelfTest extends AbstractIndexingCommonTest {
    /** */
    private static final int NODES_COUNT = 3;

    /**
     *
     */
    private static final String PERSON_CACHE = "person";

    /**
     *
     */
    private static final String MED_INFO_CACHE = "medical_info";

    /**
     *
     */
    private static final String BLOOD_INFO_PJ_CACHE = "blood_group_info_PJ";

    /**
     *
     */
    private static final String BLOOD_INFO_P_CACHE = "blood_group_info_P";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Check distributed OUTER join of 3 tables (T1 -> T2 -> T3) returns correct result
     * for non-collocated data.
     *
     * <ul>
     *     <li>Create single cache with multiple query entities.</li>
     *     <li>Put data into cache.</li>
     *     <li>Check query with distributedJoin=true returns correct results.</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-29449")
    @Test
    public void testNonCollocatedDistributedJoinSingleCache() throws Exception {
        startGridsMultiThreaded(NODES_COUNT, false);

        QueryEntity bgQueryEntity = new QueryEntity(String.class, BloodGroupInfoP.class)
            .setTableName(BLOOD_INFO_P_CACHE)
            .setKeyFieldName("blood_group");

        bgQueryEntity.getFields().put("blood_group", String.class.toString());

        IgniteCache<Object, Object> cache = ignite(0).createCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setQueryEntities(Arrays.asList(
                    new QueryEntity(String.class, Person.class).setTableName(PERSON_CACHE),
                    new QueryEntity(Long.class, MedicalInfo.class).setTableName(MED_INFO_CACHE),
                    new QueryEntity(Long.class, BloodGroupInfoPJ.class).setTableName(BLOOD_INFO_PJ_CACHE),
                    bgQueryEntity
                ))
        );

        awaitPartitionMapExchange();

        populatePersonData(cache);
        populateMedInfoData(cache);
        populateBloodGrpPJData(cache);
        populateBloodGrpPData(cache);

        checkQueries(cache);
    }

    /**
     * Check distributed OUTER join of 3 tables (T1 -> T2 -> T3) returns correct result
     * for non-collocated data.
     *
     * <ul>
     *     <li>Create caches for query entities (single query entity per cache).</li>
     *     <li>Put data into cache.</li>
     *     <li>Check query with distributedJoin=true returns correct results.</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNonCollocatedDistributedJoin() throws Exception {
        startGridsMultiThreaded(NODES_COUNT, false);

        IgniteCache<Object, Object> personCache = ignite(0).createCache(
            new CacheConfiguration<>(PERSON_CACHE).setQueryEntities(Collections.singleton(
                new QueryEntity(String.class, Person.class)))
                .setSqlSchema("PUBLIC")
        );

        IgniteCache<Object, Object> medInfoCache = ignite(0).createCache(
            new CacheConfiguration<>(MED_INFO_CACHE).setQueryEntities(Collections.singleton(
                new QueryEntity(Long.class, MedicalInfo.class).setTableName(MED_INFO_CACHE)))
                .setSqlSchema("PUBLIC")
        );

        IgniteCache<Object, Object> bloodGrpCache1 = ignite(0).createCache(
            new CacheConfiguration<>(BLOOD_INFO_PJ_CACHE).setQueryEntities(Collections.singleton(
                new QueryEntity(Long.class, BloodGroupInfoPJ.class).setTableName(BLOOD_INFO_PJ_CACHE)))
                .setSqlSchema("PUBLIC")
        );

        final QueryEntity bgQueryEntity = new QueryEntity(String.class, BloodGroupInfoP.class).setTableName(BLOOD_INFO_P_CACHE)
            .setKeyFieldName("blood_group");
        bgQueryEntity.getFields().put("blood_group", String.class.toString());

        IgniteCache<Object, Object> bloodGrpCache2 = ignite(0).createCache(
            new CacheConfiguration<>(BLOOD_INFO_P_CACHE).setQueryEntities(Collections.singleton(
                bgQueryEntity))
                .setSqlSchema("PUBLIC")
        );

        awaitPartitionMapExchange();

        populatePersonData(personCache);
        populateMedInfoData(medInfoCache);
        populateBloodGrpPJData(bloodGrpCache1);
        populateBloodGrpPData(bloodGrpCache2);

        checkQueries(personCache);
    }

    /**
     * Start queries and check query results.
     *
     * @param cache Cache.
     */
    private void checkQueries(IgniteCache<Object, Object> cache) {
        // Join on secondary index.
        SqlFieldsQuery qry1 = new SqlFieldsQuery("SELECT person.id, person.name, medical_info.blood_group, blood_group_info_PJ.universal_donor FROM person\n" +
            "  LEFT JOIN medical_info ON medical_info.name = person.name \n" +
            "  LEFT JOIN blood_group_info_PJ ON blood_group_info_PJ.blood_group = medical_info.blood_group;");

        // Join on primary index.
        SqlFieldsQuery qry2 = new SqlFieldsQuery("SELECT person.id, person.name, medical_info.blood_group, blood_group_info_P.universal_donor FROM person\n" +
            "  LEFT JOIN medical_info ON medical_info.name = person.name \n" +
            "  LEFT JOIN blood_group_info_P ON blood_group_info_P.blood_group = medical_info.blood_group;");

        qry1.setDistributedJoins(true);
        qry2.setDistributedJoins(true);

        final String res1 = queryResultAsString(cache.query(qry1).getAll());
        final String res2 = queryResultAsString(cache.query(qry2).getAll());

        log.info("Query1 result: \n" + res1);
        log.info("Query2 result: \n" + res2);

        String expOut = "2001,Shravya,null,null\n" +
            "2002,Kiran,O+,O+A+B+AB+\n" +
            "2003,Harika,AB+,AB+\n" +
            "2004,Srinivas,null,null\n" +
            "2005,Madhavi,A+,A+AB+\n" +
            "2006,Deeps,null,null\n" +
            "2007,Hope,null,null\n";

        assertEquals("Wrong result", expOut, res1);
        assertEquals("Wrong result", expOut, res2);
    }

    /**
     * Convert query result to string.
     *
     * @param res Query result set.
     * @return String representation.
     */
    private String queryResultAsString(List<List<?>> res) {
        List<String> results = new ArrayList<>();

        for (List<?> row : res) {
            StringBuilder sb = new StringBuilder();
            for (Iterator<?> iterator = row.iterator(); iterator.hasNext(); ) {
                sb.append(iterator.next());

                if (iterator.hasNext())
                    sb.append(',');
            }
            results.add(sb.toString());
        }

        results.sort(String::compareTo);

        StringBuilder sb = new StringBuilder();

        for (String result : results)
            sb.append(result).append('\n');

        return sb.toString();
    }

    /**
     * @param cache Ignite cache.
     */
    private void populatePersonData(IgniteCache<Object, Object> cache) {
        cache.put("Shravya", new Person(2001, "Shravya"));
        cache.put("Kiran", new Person(2002, "Kiran"));
        cache.put("Harika", new Person(2003, "Harika"));
        cache.put("Srinivas", new Person(2004, "Srinivas"));
        cache.put("Madhavi", new Person(2005, "Madhavi"));
        cache.put("Deeps", new Person(2006, "Deeps"));
        cache.put("Hope", new Person(2007, "Hope"));
    }

    /**
     * @param cache Ignite cache.
     */
    private void populateMedInfoData(IgniteCache<Object, Object> cache) {
        cache.put(2001L, new MedicalInfo(2001, "Madhavi", "A+"));
        cache.put(2002L, new MedicalInfo(2002, "Diggi", "B+"));
        cache.put(2003L, new MedicalInfo(2003, "Kiran", "O+"));
        cache.put(2004L, new MedicalInfo(2004, "Harika", "AB+"));
    }

    /**
     * @param cache Ignite cache.
     */
    private void populateBloodGrpPJData(IgniteCache<Object, Object> cache) {
        cache.put(2001L, new BloodGroupInfoPJ(2001, "A+", "A+AB+"));
        cache.put(2002L, new BloodGroupInfoPJ(2002, "O+", "O+A+B+AB+"));
        cache.put(2003L, new BloodGroupInfoPJ(2003, "B+", "B+AB+"));
        cache.put(2004L, new BloodGroupInfoPJ(2004, "AB+", "AB+"));
        cache.put(2005L, new BloodGroupInfoPJ(2005, "O-", "EveryOne"));
    }

    /**
     * @param cache Ignite cache.
     */
    private void populateBloodGrpPData(IgniteCache<Object, Object> cache) {
        cache.put("A+", new BloodGroupInfoP(2001, "A+", "A+AB+"));
        cache.put("O+", new BloodGroupInfoP(2002, "O+", "O+A+B+AB+"));
        cache.put("B+", new BloodGroupInfoP(2003, "B+", "B+AB+"));
        cache.put("AB+", new BloodGroupInfoP(2004, "AB+", "AB+"));
        cache.put("O-", new BloodGroupInfoP(2005, "O-", "EveryOne"));
    }

    /**
     *
     */
    private static class Person {
        /** */
        @QuerySqlField
        private long id;

        /** */
        @QuerySqlField
        private String name;

        Person(long id, String name) {
            this.id = id;
            this.name = name;
        }

        public long getId() { return id; }

        public void setId(long id) { this.id = id; }

        public String getName() { return name; }

        public void setName(String name) { this.name = name; }
    }

    /**
     *
     */
    private static class MedicalInfo {
        /** */
        @QuerySqlField
        private long id;

        /** */
        @QuerySqlField(index = true)
        private String name;

        /** */
        @QuerySqlField(name = "blood_group")
        private String bloodGroup;

        MedicalInfo(long id, String name, String bloodGroup) {
            this.id = id;
            this.name = name;
            this.bloodGroup = bloodGroup;
        }

        public void setId(long id) { this.id = id; }

        public long getId() { return id; }

        public String getName() { return name; }

        public void setName(String name) { this.name = name; }

        public String getBloodGroup() { return bloodGroup; }

        public void setBloodGroup(String bloodGroup) { this.bloodGroup = bloodGroup; }
    }

    /**
     *
     */
    private static class BloodGroupInfoPJ {
        /** */
        @QuerySqlField
        private long id;

        /** */
        @QuerySqlField(index = true, name = "blood_group")
        private String bloodGroup;

        /** */
        @QuerySqlField(name = "universal_donor")
        private String universalDonor;

        BloodGroupInfoPJ(long id, String bloodGroup, String universalDonor) {
            this.id = id;
            this.bloodGroup = bloodGroup;
            this.universalDonor = universalDonor;
        }

        public void setId(long id) { this.id = id; }

        public long getId() { return id; }

        public String getBloodGroup() { return bloodGroup; }

        public void setBloodGroup(String bloodGroup) { this.bloodGroup = bloodGroup; }

        public String getUniversalDonor() { return universalDonor; }

        public void setUniversalDonor(String universalDonor) { this.universalDonor = universalDonor; }
    }

    /**
     *
     */
    private static class BloodGroupInfoP {
        /** */
        private long id;

        /** */
        @QuerySqlField
        private String bloodGroup;  // PK

        /** */
        @QuerySqlField(name = "universal_donor")
        private String universalDonor;

        BloodGroupInfoP(long id, String bloodGroup, String universalDonor) {
            this.id = id;
            this.bloodGroup = bloodGroup;
            this.universalDonor = universalDonor;
        }

        public void setId(long id) { this.id = id; }

        public long getId() { return id; }

        public String getBloodGroup() { return bloodGroup; }

        public void setBloodGroup(String bloodGroup) { this.bloodGroup = bloodGroup; }

        public String getUniversalDonor() { return universalDonor; }

        public void setUniversalDonor(String universalDonor) { this.universalDonor = universalDonor; }
    }
}
