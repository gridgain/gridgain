package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;

public class ScanQueryConcurrentSqlUpdatesTest extends ScanQueryConcurrentUpdatesAbstractTest {
    private static final String DUMMY_CACHE_NAME = "dummy";

    @Override protected IgniteCache<Integer, Integer> createCache(String cacheName, CacheMode cacheMode,
        Duration expiration) {
        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>(cacheName);
        cacheCfg.setCacheMode(cacheMode);
        if (expiration != null) {
            cacheCfg.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(expiration));
            cacheCfg.setEagerTtl(true);
        }

        IgniteEx ignite = grid(0);
        ignite.addCacheConfiguration(cacheCfg);

        ignite.getOrCreateCache(DUMMY_CACHE_NAME).query(new SqlFieldsQuery("CREATE TABLE " + cacheName + " " +
            "(key int primary key, val int) " +
            "WITH \"template=" + cacheName + ",wrap_value=false\""));

        return ignite.cache("SQL_PUBLIC_" + cacheName.toUpperCase());
    }

    @Override protected void updateCache(IgniteCache<Integer, Integer> cache, int recordsNum) {
        String tblName = tableName(cache);

        for (int i = 0; i < recordsNum; i++) {
            cache.query(new SqlFieldsQuery(
                "INSERT INTO " + tblName + " (key, val) " +
                "VALUES (" + i + ", " + i + ")"));
        }
    }

    @Override protected void destroyCache(IgniteCache<Integer, Integer> cache) {
        grid(0).cache(DUMMY_CACHE_NAME).query(new SqlFieldsQuery("DROP TABLE " + tableName(cache)));
    }

    @SuppressWarnings("unchecked")
    private String tableName(IgniteCache<Integer, Integer> cache) {
        CacheConfiguration<Integer, Integer> cacheCfg =
                (CacheConfiguration<Integer, Integer>) cache.getConfiguration(CacheConfiguration.class);
        QueryEntity qe = cacheCfg.getQueryEntities().iterator().next();

        return qe.getTableName();
    }
}
