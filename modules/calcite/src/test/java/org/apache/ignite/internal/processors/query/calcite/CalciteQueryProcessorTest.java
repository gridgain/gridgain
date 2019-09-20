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

package org.apache.ignite.internal.processors.query.calcite;


import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
@WithSystemProperty(key = "calcite.debug", value = "true")
public class CalciteQueryProcessorTest extends GridCommonAbstractTest {

    @Override protected void beforeTestsStarted() throws Exception {
        Ignite grid = startGridsMultiThreaded(1);

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


        projCache.put(1, new Project(1, "Optiq", 3));
        projCache.put(2, new Project(2, "Ignite", 3));
        projCache.put(3, new Project(3, "Calcite", 1));
        projCache.put(4, new Project(4, "GridGain", 2));

        devCache.put(1, new Developer(1, "Aristotel", 1, 1));
        devCache.put(2, new Developer(2, "Newton", 2, 2));
        devCache.put(3, new Developer(3, "dAlamber", 1, 3));
        devCache.put(4, new Developer(4, "Euler", 3, 2));
        devCache.put(5, new Developer(5, "Laplas", 2, 4));
        devCache.put(6, new Developer(6, "Einstein", 4, 2));

        countryCache.put(1, new Country(1, "Russia", 3));
        countryCache.put(2, new Country(2, "USA", 2));
        countryCache.put(3, new Country(3, "England", 1));

        cityCache.put(1, new City(1, "Moscow", 1));
        cityCache.put(2, new City(2, "Khabarovsk", 1));
        cityCache.put(3, new City(3, "NewYork", 2));
        cityCache.put(4, new City(4, "London", 3));


        awaitPartitionMapExchange();
    }

    private QueryEngine queryProcessor(IgniteEx grid) {
        return grid.context().query().getQueryEngine();
    }

    private CacheConfiguration<Object, Object> cache(QueryEntity ent) {
        return new CacheConfiguration<>(ent.getTableName())
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(0)
                .setQueryEntities(Collections.singletonList(ent))
                .setSqlSchema("PUBLIC");
    }

    @Test
    public void simpleQuery() throws Exception {
        List<List<?>> all = queryProcessor(grid(0)).query(null, "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
                "FROM PUBLIC.Developer d JOIN (" +
                "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
                ") p " +
                "ON d.projectId = p.id0 " +
                "WHERE (d.projectId + 1) > ?", 2).get(0).getAll();

        assertNotNull(all);
    }

    private static class Project {
        int id;
        String name;
        int ver;

        public Project(int id, String name, int ver) {
            this.id = id;
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
        int id;
        String name;
        int projectId;
        int cityId;

        public Developer(int id, String name, int projectId, int cityId) {
            this.id = id;
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
        int id;
        String name;
        int countryCode;

        public Country(int id, String name, int countryCode) {
            this.id = id;
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
        int id;
        String name;
        int countryId;

        public City(int id, String name, int countryId) {
            this.id = id;
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