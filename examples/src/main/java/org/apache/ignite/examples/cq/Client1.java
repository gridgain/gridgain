package org.apache.ignite.examples.cq;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;

import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import java.util.UUID;

public class Client1 {
    public static final String CACHE_NAME = "Cache1";

    public static void main(String[] args) {
        run();
    }

    public static void run() {
        Thread t = new Thread(new Runnable() {
            @Override public void run() {
                Ignite ignite = Ignition.start(new IgniteConfiguration()
                        .setClientMode(true)
                        .setIgniteInstanceName(UUID.randomUUID().toString()));
                IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME);

                ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();
                IgniteBiPredicate<Integer, String> predicate = new IgniteBiPredicate<Integer, String>() {
                    @Override public boolean apply(Integer key, String value) {
                        return key > 10;
                    }
                };
                qry.setInitialQuery(new ScanQuery<>(predicate));
                qry.setLocalListener(new CQListener());
                qry.setRemoteFilterFactory(new CQFactory());

                cache.query(qry);
            }
        });
        t.start();
    }

    public static class CQListener implements CacheEntryUpdatedListener<Integer, String> {
        @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends String>> events) throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends Integer, ? extends String> e: events)
                System.out.println("Updated: " + e);
        }
    }

    public static class CQFactory implements Factory<CacheEntryEventFilter<Integer, String>> {
        @Override public CacheEntryEventFilter<Integer, String> create() {
            return new CacheEntryEventFilter<Integer, String>() {
                @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends String> cacheEntryEvent) throws CacheEntryListenerException {
                    return cacheEntryEvent.getKey() > 15;
                }
            };
        }
    }
}
