package org.apache.ignite.p2p;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;

public class GridP2PCustomSqlFunctionsConfigurationTest extends GridCommonAbstractTest {
    /** Test class loader 1. */
    private static final ClassLoader TEST_CLASS_LOADER_1;

    /** */
    private static final String CUSTOM_FUNCTIONS_CLASS_NAME = "org.apache.ignite.tests.p2p.cache.CustomSqlFunctionsClass";

    static {
        try {
            TEST_CLASS_LOADER_1 = new URLClassLoader(
                new URL[]{new URL(GridTestProperties.getProperty("p2p.uri.cls.second"))},
                GridP2PScanQueryWithTransformerTest.class.getClassLoader());
        } catch (MalformedURLException e) {
            throw new RuntimeException("Define property p2p.uri.cls", e);
        }
    }

    /** */
    private ClassLoader clsLoader;

    /** */
    private Integer localDiscoPort;

    /** */
    private Integer remoteDiscoPort;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        if (localDiscoPort != null) {
            TcpDiscoverySpi disco = (TcpDiscoverySpi)cfg.getDiscoverySpi();
            disco.setLocalPort(localDiscoPort);
        }

        if (remoteDiscoPort != null) {
            TcpDiscoverySpi disco = (TcpDiscoverySpi)cfg.getDiscoverySpi();
            TcpDiscoveryVmIpFinder finder = (TcpDiscoveryVmIpFinder)disco.getIpFinder();

            finder.setAddresses(Collections.singleton("127.0.0.1:" + remoteDiscoPort));
        }

        cfg.setClassLoader(clsLoader);

        return cfg;
    }

    @Test
    public void testStartSimpleGrid() throws Exception {
        localDiscoPort = 47500;
        IgniteEx ig0 = startGrid(0);

        clsLoader = TEST_CLASS_LOADER_1;

        localDiscoPort = null;
        remoteDiscoPort = 47500;
        IgniteEx cl0 = startClientGrid(1);
        remoteDiscoPort = null;

        cl0.getOrCreateCache(new CacheConfiguration<>("CACHE").setSqlFunctionClasses(clsLoader.loadClass(CUSTOM_FUNCTIONS_CLASS_NAME)));
    }
}
