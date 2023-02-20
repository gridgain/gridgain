/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
