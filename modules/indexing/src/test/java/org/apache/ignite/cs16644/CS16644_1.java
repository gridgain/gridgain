package org.apache.ignite.cs16644;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.cache.configuration.Factory;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

public class CS16644_1 {

    private static final String CACHE_NAME = "test_cache";

    public static void main(String[] args) throws Exception {
        try (final Ignite srv1 = Ignition.start(getConfiguration("server_1"))) {
            srv1.cluster().state(ClusterState.ACTIVE);

            System.out.println("cluster activated");

            try (final IgniteDataStreamer<Object, Object> streamer = srv1.dataStreamer(CACHE_NAME)) {
                GenericAffinityKey affinityKey;
                GenericKey key;
                Person person;
                for (int i = 0; i < 150_000; i++) {
                    affinityKey = new GenericAffinityKey(Integer.toString(i));
                    key = new GenericKey(UUID.randomUUID(), affinityKey);
                    person = new Person((long) i, UUID.randomUUID().toString());
                    streamer.addData(key, person);
                    if (i % 10_000 == 0)
                        System.out.println("i: " + i);
                }
            }

            for (int i = 0; i < 5; i++) {
                final List<List<?>> list1 = srv1.cache(CACHE_NAME)
                    .query(new SqlFieldsQuery("select count(_key) from Person")).getAll();
                System.out.println(list1);

                final List<List<?>> list2 = srv1.cache(CACHE_NAME)
                    .query(new SqlFieldsQuery("select * from Person")).getAll();
                System.out.println(list2.size());

                System.out.println(srv1.cache(CACHE_NAME).localMetrics().getCacheSize());

                Thread.sleep(5_000);

                System.out.println("=======================================================");
            }
        }
    }

    private static IgniteConfiguration getConfiguration(String igniteInstanceName) {
        final LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put("name", String.class.getName());

        Factory<ExpiryPolicy> expiryFactory =
            AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, 10));

        List<QueryIndex> indexes = new ArrayList<>();
        indexes.add(new QueryIndex("name"));

        CacheConfiguration cfg = new CacheConfiguration<>(CACHE_NAME);
        cfg.setExpiryPolicyFactory(expiryFactory);
        cfg.setSqlSchema(CACHE_NAME);
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cfg.setBackups(1);
        cfg.setQueryParallelism(5);

        QueryEntity e = new QueryEntity();
        e.setKeyType(GenericKey.class.getName());
        e.setValueType(Person.class.getName());
        e.setIndexes(indexes);
        e.setFields(map);
        cfg.setQueryEntities(Collections.singletonList(e));

        CacheKeyConfiguration cacheKeyConfig =
            new CacheKeyConfiguration(GenericKey.class);
        cacheKeyConfig.setTypeName(GenericKey.class.getName());
        cacheKeyConfig.setAffinityKeyFieldName("affinityKey");
        cfg.setKeyConfiguration(cacheKeyConfig);
        cfg.setAffinity(new RendezvousAffinityFunction());
        cfg.setStatisticsEnabled(true);
        cfg.setPartitionLossPolicy(PartitionLossPolicy.valueOf("READ_WRITE_SAFE"));
        cfg.setCopyOnRead(false);

        return new IgniteConfiguration()
            .setIgniteInstanceName(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCacheConfiguration(cfg).setPeerClassLoadingEnabled(true)
            .setDeploymentMode(DeploymentMode.SHARED)
            .setDiscoverySpi(
                new TcpDiscoverySpi().setIpFinder(
                    new TcpDiscoveryVmIpFinder()
                        .setAddresses(Collections.singletonList("localhost:47500..47509"))
                )
            );
    }
}
