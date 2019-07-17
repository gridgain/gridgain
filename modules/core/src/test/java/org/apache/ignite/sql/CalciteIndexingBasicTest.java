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
import java.util.concurrent.ThreadLocalRandom;
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

        QueryEntity qryEntity = new QueryEntity();
        qryEntity.setKeyType(Integer.class.getName());
        qryEntity.setKeyFieldName("id");
        qryEntity.setValueType(Project.class.getName());
        qryEntity.addQueryField("ver", Integer.class.getName(), null);
        qryEntity.addQueryField("name", String.class.getName(), null);
        qryEntity.addQueryField("id", Integer.class.getName(), null);
        qryEntity.setTableName("ProjectTbl");
        ccfg.setQueryEntities(Arrays.asList(qryEntity));

        ccfg.setSqlSchema("PUBLIC");

        IgniteCache<Integer, Project> cache = grid.createCache(ccfg);


        cache.put(1, new Project("Ignite", 3));
        cache.put(2, new Project("Calcite", 1));
        cache.put(3, new Project("Gridgain", 9));
    }

    @Test
    public void testSelect() {
        IgniteCache<Integer, String> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        List res;// = cache.query(new SqlFieldsQuery("SELECT id, name FROM ProjectTbl WHERE ver > 1 ORDER BY name")).getAll();


        for(int i = 0; i < 1000; i++)
            res = cache.query(new SqlFieldsQuery("SELECT id, name " + (ThreadLocalRandom.current().nextBoolean() ? ", ver" : "") +
                " FROM ProjectTbl WHERE" +
                " ver > " + ThreadLocalRandom.current().nextInt(3) +
                " OR id < " + ThreadLocalRandom.current().nextInt(3, 10) +
                " ORDER BY name")).getAll();
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
}
