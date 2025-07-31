
package org.apache.ignite.internal.benchmarks.trafgen;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.transactions.Transaction;

import java.util.*;

public class GG8Sample
{
    private IgniteCache<String, BinaryObject> dataCache;
    private IgniteCache<DAOTagKey, DAOTagValue> tagsCache;
    private Ignite client;

    private BinaryObject createDataValue(String aKey, Map<String, Set<String>> tags, byte[] binaryPayload)
    {
        BinaryObjectBuilder builder = client.binary().builder("com.hpe.cms.sde.binaryobj");
        Map<String, String> lvExt = Collections.singletonMap("ext", "extvalue" + aKey);
        long lvExpTime = System.currentTimeMillis() + 3600 * 24 * 1000;

        builder.setField("IVVERSIONID", 1);
        builder.setField("IVMODIFICATIONDATE", System.currentTimeMillis());
        builder.setField("IVVERSION", 1L);
        builder.setField("IVAPPVERSION", (short) 1);
        builder.setField("IVTAGS", tags);
        builder.setField("IVEXTENDEDATTRIBUTES", lvExt);
        builder.setField("IVEXPIRATIONDATE", lvExpTime);

        builder.setField("PARTSCATALOG", Collections.singletonMap("binpart", "PART_0"));
        builder.setField("PART_0", binaryPayload);
        return builder.build();
    }

    private Map<DAOTagKey, DAOTagValue> buildTagMap(String aKey, Map<String, Set<String>> tags)
    {
        Map<DAOTagKey, DAOTagValue> map = new HashMap<>();
        for (Map.Entry<String, Set<String>> tag : tags.entrySet()) 
        {
            for (String value : tag.getValue())
            {
                map.put(new DAOTagKey(tag.getKey(), value, aKey), new DAOTagValue());
            }
        }
        return map;
    }

    private QueryEntity createDataQueryEntity()
    {
        QueryEntity theQueryEntity = new QueryEntity();
        final List<String> keyFields = makeList("key");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        for (String field : keyFields)
        {
            fields.put(field, String.class.getName());
        }
        theQueryEntity.setFields(fields);
        theQueryEntity.setKeyFields(new HashSet<>(fields.keySet()));

        theQueryEntity.addQueryField("key", String.class.getName(), null);
        theQueryEntity.addQueryField("IVEXPIRATIONDATE", long.class.getName(), null);

        theQueryEntity.setIndexes(makeList(new QueryIndex("IVEXPIRATIONDATE")));
        theQueryEntity.setKeyType(String.class.getName());
        theQueryEntity.setValueType("com.hpe.cms.sde.dao.DAOBinaryObject");

        return theQueryEntity;
    }

    private QueryEntity createTagQueryEntity()
    {
        // definition of tags table
        QueryEntity theQueryEntity = new QueryEntity();

        final List<String> tagKeyFields = makeList("tagName", "tagValue", "key");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        for (String field : tagKeyFields)
        {
            fields.put(field, String.class.getName());
        }
        theQueryEntity.setFields(fields);
        theQueryEntity.setKeyFields(new HashSet<>(fields.keySet()));

        // Key type and value type
        theQueryEntity.setKeyType(DAOTagKey.class.getName());
        theQueryEntity.setValueType(DAOTagValue.class.getName());
        return theQueryEntity;
    }

    public void createOrReplace(String aKey, Map<String, Set<String>> lvTags, byte[] binaryPayload)
    {
        try (Transaction tx = client.transactions().txStart())
        {
            BinaryObject oldValue = dataCache.getAndPut(aKey, createDataValue(aKey, lvTags, binaryPayload));
            if (oldValue == null)
            {
                tagsCache.putAll(buildTagMap(aKey, lvTags));
            } else
            {
                // If a strict version control was required, we would use oldvalue to check its version versus the
                // expected one and then
                // update the records with a new version number. But the test does not go into this path.
                // dataView.put(tx, createDataKey(aKey), createDataValue(lvTags));
                // We would have to compute the changes in the tags map and remove those that are not longer there.
                // But the test don't go into this path.
                // tagsView.removeAll(tx, ...);
                tagsCache.putAll(buildTagMap(aKey, lvTags));
            }
            tx.commit();
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public byte[] get(String aKey)
    {
        BinaryObject obj = dataCache.get(aKey);
        return obj != null ? obj.field("PART_0") : null;
    }

    public void delete(String aKey)
    {
        try (Transaction tx = client.transactions().txStart())
        {
            BinaryObject oldValue = dataCache.getAndRemove(aKey);
            if (oldValue != null)
            {
                Map<String, Set<String>> tags = oldValue.field("IVTAGS");
                // If a strict version control was required, we would use oldvalue to check its version versus the
                // expected one and then
                // update the records with a new version number. But the test does not go into this path.
                tagsCache.removeAll(buildTagMap(aKey, tags).keySet());
            }
            tx.commit();
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public void replace(String aKey, Map<String, Set<String>> lvTags, byte[] binaryPayload)
    {
        try (Transaction tx = client.transactions().txStart())
        {
            BinaryObject oldvalue = dataCache.get(aKey);
            if (oldvalue != null)
            {
                dataCache.put(aKey, createDataValue(aKey, lvTags, binaryPayload));

                tagsCache.putAll(buildTagMap(aKey, lvTags));
            }
            tx.commit();
        } catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    public void intitialize(String realm, String storage)
    {

        System.out.println("PVD:: Before client start");

        client = Ignition.start(igniteConfiguration());
        client.cluster().state(ClusterState.ACTIVE);

        System.out.println("PVD:: Client started");

        dataCache = newCache("DATA_" + realm + "_" + storage, String.class, createDataQueryEntity()).withKeepBinary();
        tagsCache = newCache("TAGS_" + realm + "_" + storage, DAOTagKey.class, createTagQueryEntity());
    }

    private <K, V> IgniteCache<K, V> newCache(String name, Class<K> keyType, QueryEntity queryEntity)
    {
        RendezvousAffinityFunction rdvAffFunc = new RendezvousAffinityFunction();
        rdvAffFunc.setPartitions(256);

        CacheConfiguration<K, V> cacheCfg = new CacheConfiguration<>(name.toUpperCase());
        cacheCfg.setBackups(1);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE);
        cacheCfg.setDataRegionName("default");
        cacheCfg.setAffinity(rdvAffFunc);
        cacheCfg.setQueryEntities(makeList(queryEntity));
        cacheCfg.setKeyConfiguration(new CacheKeyConfiguration(keyType.getName(), "key"));

        return client.getOrCreateCache(cacheCfg);
    }

    private IgniteConfiguration igniteConfiguration()
    {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setPeerClassLoadingEnabled(true);
        cfg.setClientMode(true);
        cfg.setShutdownPolicy(ShutdownPolicy.GRACEFUL);
        String lvAdresses = "172.25.4.101";
        int port = 47500;
        int lvComPort = 47100;

        TcpDiscoverySpi lvSpi = new TcpDiscoverySpi();
        TcpCommunicationSpi lvCommSpi = new TcpCommunicationSpi();

        TcpDiscoveryVmIpFinder lvFinder = new TcpDiscoveryVmIpFinder();
        lvFinder.setAddresses(makeList(lvAdresses));
        lvSpi.setLocalPort(port);
        lvSpi.setLocalPortRange(10);
        lvCommSpi.setLocalPort(lvComPort);
        lvCommSpi.setLocalPortRange(10);

        lvSpi.setIpFinder(lvFinder);

        cfg.setDiscoverySpi(lvSpi);
        cfg.setCommunicationSpi(lvCommSpi);

        return cfg;
    }
    
    <T> List<T> makeList(T... val) {
        List<T> l = new ArrayList<>(val.length);

        for (T t : val) {
            l.add(t);
        }

        return l;
    }
}
