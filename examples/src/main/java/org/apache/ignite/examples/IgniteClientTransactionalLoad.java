package org.apache.ignite.examples;

import java.net.InetSocketAddress;
import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.transactions.Transaction;

public class IgniteClientTransactionalLoad {
    private static final String CACHE_NAME = "testcache";

    public static void main(String[] args) {
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

        IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME);


        IgniteTransactions transactions = ignite.transactions();

        for (int i = 1000; i <= 1_000_000; i++) {
            try (Transaction tx = transactions.txStart()) {
                cache.put(i, "Hello " + i);

                tx.commit();
            }
        }
    }
}
