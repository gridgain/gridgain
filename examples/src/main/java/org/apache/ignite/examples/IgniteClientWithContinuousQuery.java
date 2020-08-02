package org.apache.ignite.examples;

import java.net.InetSocketAddress;
import java.util.Collections;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

public class IgniteClientWithContinuousQuery {
    private static final String CACHE_NAME = "testcache";

    public static void main(String[] args) {
        Thread t1 = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(30_000L);
                    System.out.println("is alive");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        t1.start();

        TcpDiscoveryIpFinder finder = new TcpDiscoveryMulticastIpFinder();
        finder.registerAddresses(Collections.singletonList(new InetSocketAddress("localhost", 47501)));

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(true);
        cfg.setPeerClassLoadingEnabled(true);
        cfg.setPluginConfigurations(new PluginConfiguration() {
        });

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(finder);

        cfg.setDiscoverySpi(spi);

        Ignite ignite = Ignition.start(cfg);
        IgniteLogger log = ignite.log();

        System.out.println();
        log.info(">>> Cache continuous query example started.");

        // Auto-close cache at the end of the example.
        try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME)) {
            int keyCnt = 20;

            // These entries will be queried by initial predicate.
            for (int i = 0; i < keyCnt; i++)
                cache.put(i, Integer.toString(i));

            // Create new continuous query.
            ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();

            qry.setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<Integer, String>() {
                @Override
                public boolean apply(Integer key, String val) {
                    return key > 10;
                }
            }));

            // Callback that is called locally when update notifications are received.
            qry.setLocalListener(new CacheEntryUpdatedListener<Integer, String>() {
                @Override
                public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends String>> evts) {
                    for (CacheEntryEvent<? extends Integer, ? extends String> e : evts)
                        System.out.println("Updated entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');
                }
            });

            // This filter will be evaluated remotely on all nodes.
            // Entry that pass this filter will be sent to the caller.
            qry.setRemoteFilterFactory(new Factory<CacheEntryEventFilter<Integer, String>>() {
                @Override
                public CacheEntryEventFilter<Integer, String> create() {
                    return new CacheEntryEventFilter<Integer, String>() {
                        @Override
                        public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends String> e) {
                            return e.getKey() > 10;
                        }
                    };
                }
            });

            // Execute query.
            try (QueryCursor<Cache.Entry<Integer, String>> cur = cache.query(qry)) {
                // Iterate through existing data.
                for (Cache.Entry<Integer, String> e : cur)
                    System.out.println("Queried existing entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');

                // Add a few more keys and watch more query notifications.
                for (int i = keyCnt; i < keyCnt + 10; i++)
                    cache.put(i, Integer.toString(i));

                // Wait for a while while callback is notified about remaining puts.
                Thread.sleep(200_000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
