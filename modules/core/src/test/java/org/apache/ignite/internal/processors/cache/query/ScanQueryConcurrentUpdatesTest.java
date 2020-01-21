package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;

public class ScanQueryConcurrentUpdatesTest extends ScanQueryConcurrentUpdatesAbstractTest {
    @Override protected IgniteCache<Integer, Integer> createCache(String cacheName, CacheMode cacheMode,
        Duration expiration) {
        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>(cacheName);
        cacheCfg.setCacheMode(cacheMode);
        if (expiration != null) {
            cacheCfg.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(expiration));
            cacheCfg.setEagerTtl(true);
        }

        return grid(0).createCache(cacheCfg);
    }

    @Override protected void updateCache(IgniteCache<Integer, Integer> cache, int recordsNum) {
        for (int i = 0; i < recordsNum; i++)
            cache.put(i, i);
    }

    @Override protected void destroyCache(IgniteCache<Integer, Integer> cache) {
        cache.destroy();
    }
}
