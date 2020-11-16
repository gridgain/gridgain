package org.apache.ignite;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class Sdsb11905 extends GridCommonAbstractTest {

    private static final String CACHE = "cache";

    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration conf = super.getConfiguration(igniteInstanceName);
        conf.getTransactionConfiguration().setDefaultTxTimeout(1000);
        conf.setCacheConfiguration(
                new CacheConfiguration().setName(CACHE).setQueryEntities(Collections.singletonList(queryEntity()))
        );

        return conf;
    }

    public void test() throws Exception {
        final IgniteEx igniteEx = startGrids(3);

        final IgniteCache<Object, Object> cache = igniteEx.getOrCreateCache(CACHE);

//        final SqlFieldsQuery query = new SqlFieldsQuery("insert into MY_ENTITY()");

        cache.put(0, new MyEntity(0, "1", "2"));

        System.out.println();


    }

//    private void loadCache(IgniteCache cache, int cnt) {
//        new SqlFieldsQuery("INSERT INTO ").setArgs()
//
//        cache.query()
//    }

    private QueryEntity queryEntity() {
        final QueryEntity queryEntity = new QueryEntity(Integer.class.getName(), MyEntity.class.getName());

        queryEntity.setKeyFields(Collections.singleton("keyField"));

        final LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("field1", String.class.getName());
        fields.put("field2", String.class.getName());
        queryEntity.setFields(fields);

        return queryEntity;
    }

    private static class MyEntity {
        public Integer keyField;
        public String field1;
        public String field2;

        public MyEntity() {
        }

        public MyEntity(Integer keyField, String field1, String field2) {
            this.keyField = keyField;
            this.field1 = field1;
            this.field2 = field2;
        }
    }

//    /** */
//    private static final String POI_CACHE_NAME = "POI_CACHE";
//
//    /** */
//    private static final String POI_SCHEMA_NAME = "DOMAIN";
//
//    /** */
//    private static final String POI_TABLE_NAME = "POI";
//
//    /** */
//    private static final String POI_CLASS_NAME = "PointOfInterest";
//
//    /** */
//    private static final String ID_FIELD_NAME = "id";
//
//    /** */
//    private static final String NAME_FIELD_NAME = "name";
//
//    /** */
//    private static final String LATITUDE_FIELD_NAME = "latitude";
//
//    /** */
//    private static final String LONGITUDE_FIELD_NAME = "longitude";
//
//    /** */
//    private static final int NUM_ENTITIES = 1_000;

//    private void loadData(IgniteEx node, int start, int end) {
//        try (IgniteDataStreamer<Object, Object> streamer = node.dataStreamer(POI_CACHE_NAME)) {
//            Random rnd = ThreadLocalRandom.current();
//
//            for (int i = start; i < end; i++) {
//                BinaryObject bo = node.binary().builder(POI_CLASS_NAME)
//                        .setField(NAME_FIELD_NAME, "POI_" + i, String.class)
//                        .setField(LATITUDE_FIELD_NAME, rnd.nextDouble(), Double.class)
//                        .setField(LONGITUDE_FIELD_NAME, rnd.nextDouble(), Double.class)
//                        .build();
//
//                streamer.addData(i, bo);
//            }
//        }
//    }
//
//    /** */
//    protected List<List<?>> query(Ignite ig, String sql) {
//        IgniteCache<Object, Object> cache = ig.cache(POI_CACHE_NAME).withKeepBinary();
//
//        return cache.query(new SqlFieldsQuery(sql).setSchema(POI_SCHEMA_NAME)).getAll();
//    }
//
//    /** */
//    private QueryEntity queryEntity() {
//        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
//        fields.put(ID_FIELD_NAME, Integer.class.getName());
//        fields.put(NAME_FIELD_NAME, String.class.getName());
//        fields.put(LATITUDE_FIELD_NAME, Double.class.getName());
//        fields.put(LONGITUDE_FIELD_NAME, Double.class.getName());
//
//        return new QueryEntity()
//                .setKeyType(Integer.class.getName())
//                .setKeyFieldName(ID_FIELD_NAME)
//                .setValueType(POI_CLASS_NAME)
//                .setTableName(POI_TABLE_NAME)
//                .setFields(fields);
//    }
}
