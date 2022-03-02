package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;
import javax.cache.Cache.Entry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class ClientBinaryMetadataTest extends GridCommonAbstractTest {

    public static final String CACHE_NAME = "DYNAMIC_CACHE";

    @Override
    protected void afterTestsStopped() throws Exception {
        G.stopAll(true);
    }

    @Test
    public void testCacheStartWithAffinityField() throws Exception {
        Random rnd = new Random();

        IgniteEx srv = startGrid(0);
        IgniteEx cli = startClientGrid(1);

        IgniteCache<Key, Value> cliCache = cli.createCache(cacheCfg(CACHE_NAME));

        Key key = new Key(rnd.nextInt(), rnd.nextInt());
        Key key2 = new Key(rnd.nextInt(), rnd.nextInt());
        Value val = new Value(GridTestUtils.randomString(rnd, 15));

        cliCache.put(key, val);

        validateCache(cliCache, 1);
        validateCache(srv.cache(CACHE_NAME), 1);

        IgniteCache<Key, Value> cliCache2 = startClientGrid(2).cache(CACHE_NAME);
        validateCache(cliCache2, 1);

        cliCache2.put(key2, val);

        validateCache(cliCache, 2);
        validateCache(cliCache2, 2);
        validateCache(srv.cache(CACHE_NAME), 2);
    }

    private static void validateCache(IgniteCache<Key, Value> cache, int expectedSize) {
        assertEquals(expectedSize, cache.size());

        //noinspection unchecked
        int partitions = cache.getConfiguration(CacheConfiguration.class).getAffinity().partitions();

        Queue<Key> keys = new ConcurrentLinkedQueue<>();
        int scanSize = IntStream.rangeClosed(0, partitions - 1).parallel().map(partition -> {
                    int cnt = 0;
                    try (QueryCursor<Entry<Key, Value>> queryCursor = cache.query(new ScanQuery<>(partition, null))) {

                        for (Entry<Key, Value> entry : queryCursor) {
                            keys.add(entry.getKey());
                            cnt++;
                        }
                    }

                    return cnt;
                }
        ).sum();

        assertEquals(expectedSize, scanSize);
        assertEquals(expectedSize, keys.size());
        assertEquals(expectedSize, keys.stream().mapToInt(k -> cache.get(k) != null ? 1 : 0).sum());

    }

    private static CacheConfiguration<Key, Value> cacheCfg(String cacheName) {

        Set<String> keyFields = new HashSet<>();

        keyFields.add("id");
        keyFields.add("affId");
//
//        Map<String, String> columnAliases = new HashMap<>();
//        columnAliases.put("cdtorID", "CDTORID");
//        columnAliases.put("dbtorAcc", "DBTORACC");
//        columnAliases.put("debtorAccountHash", "DEBTORACCOUNTHASH");
//        columnAliases.put("domTimestamp", "DOMTIMESTAMP");
//        columnAliases.put("mndtID", "MNDTID");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.Integer");
        fields.put("affId", "java.lang.Integer");
        fields.put("str", "java.lang.String");

        QueryEntity queryEntity = new QueryEntity().setTableName("VALUE")
                .setKeyType(Key.class.getName())
                .setKeyFields(keyFields)
                .setFields(fields)
                .setValueType(Value.class.getName())
//                .setAliases(columnAliases)
                ;

        return new CacheConfiguration<Key, Value>().setName(cacheName).setQueryEntities(Collections.singletonList(queryEntity));
    }

    static class Key {
        int id;

        @AffinityKeyMapped
        int affId;

        Key(int id, int affId) {
            this.id = id;
            this.affId = affId;
        }
    }

    static class Value {
        String str;

        Value(String str) {
            this.str = str;
        }
    }
}
