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

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * TODO: Add class description.
 */
public class CalciteIndexingBasicTest extends GridCommonAbstractTest {

    @Override protected void beforeTestsStarted() throws Exception {
        Ignite grid = startGrid(0);

        CacheConfiguration ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        QueryEntity projEntity = new QueryEntity();
        projEntity.setKeyType(Integer.class.getName());
        projEntity.setKeyFieldName("id");
        projEntity.setValueType(Project.class.getName());
        projEntity.addQueryField("ver", Integer.class.getName(), null);
        projEntity.addQueryField("name", String.class.getName(), null);
        projEntity.addQueryField("id", Integer.class.getName(), null);
        projEntity.setTableName("Project");

        QueryEntity devEntity = new QueryEntity();
        devEntity.setKeyType(Integer.class.getName());
        devEntity.setKeyFieldName("id");
        devEntity.setValueType(Developer.class.getName());
        devEntity.addQueryField("projectId", Integer.class.getName(), null);
        devEntity.addQueryField("name", String.class.getName(), null);
        devEntity.addQueryField("id", Integer.class.getName(), null);
        devEntity.setTableName("Developer");

        ccfg.setQueryEntities(Arrays.asList(projEntity, devEntity));

        ccfg.setSqlSchema("PUBLIC");

        IgniteCache cache = grid.createCache(ccfg);


        cache.put(1, new Project("Ignite", 3));
        cache.put(2, new Project("Calcite", 1));
        cache.put(3, new Project("GridGain", 9));

        cache.put(4, new Developer("Aristotel", 1));
        cache.put(5, new Developer("Newton", 2));
        cache.put(6, new Developer("dAlamber", 9));
        cache.put(7, new Developer("Euler", 3));
        cache.put(8, new Developer("Laplas", 2));
        cache.put(9, new Developer("Einstein", 1));
    }

    @Test
    public void testSelect() {
        IgniteCache<Integer, String> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        List res = cache.query(new SqlFieldsQuery("SELECT d.name, d.projectId, p.name, p.id " +
            "FROM Developer d JOIN Project p " +
            "ON d.projectId = p.id " +
            "WHERE p.ver > 1")).getAll();


//        List res = cache.query(new SqlFieldsQuery("SELECT id, name FROM Project WHERE ver > 1")).getAll();

        System.out.println("res="  + res);
//        for(int i = 0; i < 1000; i++)
//            res = cache.query(new SqlFieldsQuery("SELECT id, name " + (ThreadLocalRandom.current().nextBoolean() ? ", ver" : "") +
//                " FROM ProjectTbl WHERE" +
//                " ver > " + ThreadLocalRandom.current().nextInt(3) +
//                " OR id < " + ThreadLocalRandom.current().nextInt(3, 10) +
//                " ORDER BY name")).getAll();
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

        public Developer(String name, int projectId) {
            this.name = name;
            this.projectId = projectId;
        }

        @Override public String toString() {
            return "Developer{" +
                "name='" + name + '\'' +
                ", projectId=" + projectId +
                '}';
        }
    }
}
