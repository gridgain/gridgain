package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE;

@WithSystemProperty(key = IGNITE_MAX_INDEX_PAYLOAD_SIZE, value = "0")
@WithSystemProperty(key = "bike.row.format", value = "true")
public class BikeSqlTest extends GridCommonAbstractTest {
    @After
    public void stopCluster() {
        stopAllGrids();
    }

    @Test
    public void simple() throws Exception {
        startGrid(0);

        q("create table Person(id int primary key, name varchar, age number)");

        q("create index on Person(name)");

        q("insert into Person values(1, 'ivan', 30.1)");

        System.err.println(q("select * from Person"));

        System.err.println(q("select * from Person where id = 1"));

        System.err.println(q("select * from Person where name = 'ivan'"));
    }

    @Test
    public void severalNodesSmoke() throws Exception {
        startGrids(3);

        q("create table Person(id int primary key, name varchar, name2 varchar)");

        q("create index on Person(name)");

        for (int i = 0; i < 100; i++)
            q("insert into Person values(?, ?, 'p')", i, "ivan" + i);

        q("select * from Person").forEach(System.err::println);

        for (int i = 0; i < 100; i++)
            System.err.println(q("select * from Person where id = ?", 99 - i));

        for (int i = 0; i < 100; i++)
            System.err.println(q("select * from Person where name = ?", "ivan" + i));
    }

    @Test
    public void severalNodesSmokeQueryEntity() throws Exception {
        startGrids(3);

        LinkedHashMap<String, String> columns = new LinkedHashMap<>();
        columns.put("id", "java.lang.Integer");
        columns.put("name", "java.lang.String");
        columns.put("name2", "java.lang.String");

        grid(0).getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setQueryEntities(Collections.singleton(new QueryEntity()
                .setKeyFieldName("id")
                .setValueType("CUSTOM_SQL_VALUE_TYPE")
                .setFields(columns)
                .setIndexes(Collections.singleton(new QueryIndex("name"))))));

        q("create table Person(id int primary key, name varchar, name2 varchar)");

        q("create index on Person(name)");

        for (int i = 0; i < 100; i++)
            q("insert into Person values(?, ?, 'p')", i, "ivan" + i);

        q("select * from Person").forEach(System.err::println);

        for (int i = 0; i < 100; i++)
            System.err.println(q("select * from Person where id = ?", 99 - i));

        for (int i = 0; i < 100; i++)
            System.err.println(q("select * from Person where name = ?", "ivan" + i));
    }

    @Test
    public void cacheApiQueryEntity() throws Exception {
        IgniteCache<Object, Object> cache = startGrid(0).getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, PersonData.class));

        cache.put(1, new PersonData("ivan", "p"));

        System.err.println(cache.get(1));

        System.err.println(q("select * from PersonData where _key = 1"));
    }

    @Test
    public void cacheApiWithoutSql() throws Exception {
        IgniteCache<Object, Object> cache = startGrid(0).getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC));

        cache.put(1, new PersonData("ivan", "p"));

        System.err.println(cache.get(1));
    }

    static class PersonData {
        @QuerySqlField
        String name;
        @QuerySqlField
        String name2;

        PersonData(String name, String name2) {
            this.name = name;
            this.name2 = name2;
        }

        @Override public String toString() {
            return "PersonData{" +
                "name='" + name + '\'' +
                ", name2='" + name2 + '\'' +
                '}';
        }
    }

    @Test
    public void severalNodesSmokeQueryEntityPojoPaired() throws Exception {
        startGrids(3);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.Integer");
        fields.put("name", "java.lang.String");
        fields.put("name2", "java.lang.String");

        grid(0).getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setQueryEntities(Collections.singleton(new QueryEntity()
                .setKeyType(Integer.class.getName())
                .setValueType(PersonData.class.getName())
                .setKeyFieldName("id")
                .setFields(fields)
                .setIndexes(Collections.singleton(new QueryIndex("name")))
                .setTableName("Person")))
            .setSqlSchema("PUBLIC"));

        for (int i = 0; i < 100; i++)
            q("insert into Person values(?, ?, 'p')", i, "ivan" + i);

        q("select * from Person").forEach(System.err::println);

        for (int i = 0; i < 100; i++)
            System.err.println(q("select * from Person where id = ?", 99 - i));

        for (int i = 0; i < 100; i++)
            System.err.println(q("select * from Person where name = ?", "ivan" + i));
    }

    private List<List<?>> q(String sql, Object... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(sql).setArgs(args), false).getAll();
    }
}
