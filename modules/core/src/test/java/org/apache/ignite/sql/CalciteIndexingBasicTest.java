/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.sql;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * TODO: Add class description.
 */
@SuppressWarnings("unchecked") public class CalciteIndexingBasicTest extends GridCommonAbstractTest {

    @Override protected void beforeTestsStarted() throws Exception {
        Ignite grid = startGridsMultiThreaded(4);



        QueryEntity projEntity = new QueryEntity();
        projEntity.setKeyType(Integer.class.getName());
        projEntity.setKeyFieldName("id");
        projEntity.setValueType(Project.class.getName());
        projEntity.addQueryField("ver", Integer.class.getName(), null);
        projEntity.addQueryField("name", String.class.getName(), null);
        projEntity.addQueryField("id", Integer.class.getName(), null);
        projEntity.setTableName("Project");

        CacheConfiguration projCfg = cache(projEntity);

        QueryEntity devEntity = new QueryEntity();
        devEntity.setKeyType(Integer.class.getName());
        devEntity.setKeyFieldName("id");
        devEntity.setValueType(Developer.class.getName());
        devEntity.addQueryField("name", String.class.getName(), null);
        devEntity.addQueryField("projectId", Integer.class.getName(), null);
        devEntity.addQueryField("cityId", Integer.class.getName(), null);
        devEntity.addQueryField("id", Integer.class.getName(), null);
        devEntity.setTableName("Developer");

        CacheConfiguration devCfg = cache(devEntity);

        QueryEntity countryEntity = new QueryEntity();
        countryEntity.setKeyType(Integer.class.getName());
        countryEntity.setKeyFieldName("id");
        countryEntity.setValueType(Country.class.getName());
        countryEntity.addQueryField("countryCode", Integer.class.getName(), null);
        countryEntity.addQueryField("name", String.class.getName(), null);
        countryEntity.addQueryField("id", Integer.class.getName(), null);
        countryEntity.setTableName("Country");

        CacheConfiguration countryCfg = cache(countryEntity);


        QueryEntity cityEntity = new QueryEntity();
        cityEntity.setKeyType(Integer.class.getName());
        cityEntity.setKeyFieldName("id");
        cityEntity.setValueType(City.class.getName());
        cityEntity.addQueryField("countryId", Integer.class.getName(), null);
        cityEntity.addQueryField("name", String.class.getName(), null);
        cityEntity.addQueryField("id", Integer.class.getName(), null);
        cityEntity.setTableName("City");

        CacheConfiguration cityCfg = cache(cityEntity);


        IgniteCache projCache = grid.createCache(projCfg);
        IgniteCache devCache = grid.createCache(devCfg);
        IgniteCache countryCache = grid.createCache(countryCfg);
        IgniteCache cityCache = grid.createCache(cityCfg);


        projCache.put(1, new Project("Optiq", 3));
        projCache.put(2, new Project("Ignite", 3));
        projCache.put(3, new Project("Calcite", 1));
        projCache.put(4, new Project("GridGain", 2));

        devCache.put(1, new Developer("Aristotel", 1, 1));
        devCache.put(2, new Developer("Newton", 2, 2));
        devCache.put(3, new Developer("dAlamber", 1, 3));
        devCache.put(4, new Developer("Euler", 3, 2));
        devCache.put(5, new Developer("Laplas", 2, 4));
        devCache.put(6, new Developer("Einstein", 4, 2));

        countryCache.put(1, new Country("Russia", 3));
        countryCache.put(2, new Country("USA", 2));
        countryCache.put(3, new Country("England", 1));

        cityCache.put(1, new City("Moscow", 1));
        cityCache.put(2, new City("Khabarovsk", 1));
        cityCache.put(3, new City("NewYork", 2));
        cityCache.put(4, new City("London", 3));


        awaitPartitionMapExchange();
    }


    @Test
    public void testSelect() {
        IgniteCache<Integer, String> cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT d.id, d.name, d.projectId, p.id, p.name, p.ver " +
            "FROM Developer d JOIN Project p " +
            "ON d.projectId = p.id " +
            "WHERE d.projectId > 1")).getAll();

//        List<List<?>> res = cache.query(new SqlFieldsQuery(
//            "SELECT d.id, d.name, d.projectId, p.id, p.name, cnt.id, cnt.Name, ci.id, ci.Name, ci.countryId  " +
//            "FROM Developer d JOIN Project p " +
//            "ON d.projectId = p.id " +
//            "JOIN City ci " +
//            "ON ci.id = d.cityId " +
//            "JOIN Country cnt " +
//            "ON ci.countryId = cnt.id " +
//            "WHERE d.projectId > 1")).getAll();

//        Random r = new Random();
//        for (int i = 0; i < 1000; i++) {
//            long start = System.currentTimeMillis();
//
//            List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT d.name, d.projectId, p.name, p.id " +
//                "FROM Developer d JOIN Project p " +
//                (r.nextBoolean() ? "ON d.projectId = p.id " : "ON d.id = p.id ") +
//                "JOIN Country c " +
//                (r.nextBoolean() ? "ON c.countryCode = d.id " : "ON c.id = d.id ")+
//                "JOIN City ci " +
//                (r.nextBoolean() ? "ON ci.countryId = c.id " : "ON ci.id = c.id ") +
//                "WHERE d.projectId > 1")).getAll();
//
//            System.out.println("res="  + (System.currentTimeMillis() - start));
//        }


//        List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT id, name, ver FROM Project")).getAll();

//        List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT id, name, ver FROM Project " +
//            "WHERE ver = (SELECT MAX(projectId) FROM Developer)")).getAll();


//        for(int i = 0; i < 1000; i++)
//            res = cache.query(new SqlFieldsQuery("SELECT id, name " + (ThreadLocalRandom.current().nextBoolean() ? ", ver" : "") +
//                " FROM ProjectTbl WHERE" +
//                " ver > " + ThreadLocalRandom.current().nextInt(3) +
//                " OR id < " + ThreadLocalRandom.current().nextInt(3, 10) +
//                " ORDER BY name")).getAll();

        System.out.println("=================RESULT======================");
        for (List<?> row : res)
            System.out.println(row);
        System.out.println("=======================================");

    }


    private CacheConfiguration<Object, Object> cache(QueryEntity ent) {
        return new CacheConfiguration<>(ent.getTableName())
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(0)
            .setQueryEntities(Collections.singletonList(ent))
            .setSqlSchema("PUBLIC");
    }

    private static class Project {
        String name;
        int ver;

        public Project(String name, int ver) {
            this.name = name;
            this.ver = ver;
        }

        @Override public String toString() {
            return "Project{" +
                "name='" + name + '\'' +
                ", ver=" + ver +
                '}';
        }
    }

    private static class Developer {
        String name;
        int projectId;
        int cityId;

        public Developer(String name, int projectId, int cityId) {
            this.name = name;
            this.projectId = projectId;
            this.cityId = cityId;
        }

        @Override public String toString() {
            return "Developer{" +
                "name='" + name + '\'' +
                ", projectId=" + projectId +
                '}';
        }
    }

    private static class Country {
        String name;
        int countryCode;

        public Country(String name, int countryCode) {
            this.name = name;
            this.countryCode = countryCode;
        }

        @Override public String toString() {
            return "Country{" +
                "name='" + name + '\'' +
                ", countryCode=" + countryCode +
                '}';
        }
    }

    private static class City {
        String name;
        int countryId;

        public City(String name, int countryId) {
            this.name = name;
            this.countryId = countryId;
        }

        @Override public String toString() {
            return "City{" +
                "name='" + name + '\'' +
                ", countryId=" + countryId +
                '}';
        }
    }
}
