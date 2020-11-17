package org.apache.ignite;

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Collections;
import java.util.LinkedHashMap;

public class Sdsb11905 {

    private static final String CACHE = "cache";

    private static IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration conf = new IgniteConfiguration();
        conf.setIgniteInstanceName(igniteInstanceName).setDiscoverySpi(
                new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Collections.singletonList("127.0.0.1:47500..47510")))
        ).setCommunicationSpi(
                new TcpCommunicationSpi().setLocalPort(48100).setLocalPortRange(10).setSocketWriteTimeout(30_000)
        ).setTransactionConfiguration(new TransactionConfiguration().setDefaultTxTimeout(30_000))
                .setCacheConfiguration(
                        new CacheConfiguration().setName(CACHE).setQueryEntities(Collections.singletonList(queryEntity()))
                );

//        IgniteConfiguration conf = super.getConfiguration(igniteInstanceName);
//        conf.getTransactionConfiguration().setDefaultTxTimeout(1000);
//        conf.setCacheConfiguration(
//                new CacheConfiguration().setName(CACHE).setQueryEntities(Collections.singletonList(queryEntity()))
//        );

        return conf;
    }

    public static void main(String[] args) throws Exception {
        final Ignite ignite = Ignition.start(getConfiguration("Sdsb11905_1"));
        Ignition.start(getConfiguration("Sdsb11905_2"));
        Ignition.start(getConfiguration("Sdsb11905_3"));

        ignite.cluster().active(true);

        final IgniteCache<Object, Object> cache = ignite.getOrCreateCache(CACHE);

        loadCache(cache, 1_000_000);

        final SqlFieldsQuery query = new SqlFieldsQuery("select * from MYENTITY where FIELD2 > 89999"); //in (999000, 33000, 767000, 545, 123000, 4588, 3399)

        long start = System.currentTimeMillis();
        System.out.println("getAll: " + cache.query(query).getAll().size());

        System.out.println("duration: " + ((System.currentTimeMillis() - start)));

        Thread.currentThread().join();
    }

    private static void loadCache(IgniteCache cache, int cnt) {
        for (int i = 0; i < cnt; i++) {
            SqlFieldsQuery query = new SqlFieldsQuery("insert into MYENTITY(keyField, FIELD1, FIELD2) values(?, ?, ?)").setArgs(i, i, i);
            cache.query(query);
        }
    }

    private static QueryEntity queryEntity() {
        final QueryEntity queryEntity = new QueryEntity(Integer.class.getName(), MyEntity.class.getName());

        queryEntity.setKeyFieldName("keyField");

//        queryEntity.setKeyFields(Collections.singleton("keyField"));

        final LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("keyField", Integer.class.getName());
        fields.put("field1", String.class.getName());
        fields.put("field2", Integer.class.getName());
        queryEntity.setFields(fields);

        return queryEntity;
    }

    private static class MyEntity {
        public Integer keyField;
        public String field1;
        public Integer field2;

        public MyEntity() {
        }

        public MyEntity(Integer keyField, String field1, Integer field2) {
            this.keyField = keyField;
            this.field1 = field1;
            this.field2 = field2;
        }
    }
}
