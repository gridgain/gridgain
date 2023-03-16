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

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;

/**
 * Class contains tests for a set of situations when cache/cache template configuration contains
 * a class available on classpath of a client node but not available to server.
 * Different tests cover various scenarios: dynamic creation of a cache/template, static configuration,
 * different APIs used to perform operations.
 */
public class P2PCustomSqlFunctionsConfigurationTest extends GridCommonAbstractTest {
    /** Test class loader. */
    private static final ClassLoader CONFIGURATION_CLASS_LOADER;

    /** */
    public static final String STATIC_CACHE_NAME = "STATIC_CACHE";

    /** */
    public static final String STATIC_CACHE_TEMPLATE = "STATIC_CACHE_TMPL_*";

    /** */
    public static final String DYNAMIC_CACHE_NAME = "DYNAMIC_CACHE";

    /** */
    public static final String DYNAMIC_CACHE_TEMPLATE = "DYNAMIC_CACHE_TMPL_*";

    /** */
    private static final String UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME =
        "org.apache.ignite.tests.p2p.cache.UnavailableToServerCustomSqlFunctionsClass";

    static {
        try {
            CONFIGURATION_CLASS_LOADER = new URLClassLoader(
                new URL[]{new URL(GridTestProperties.getProperty("p2p.uri.cls.second"))},
                P2PCustomSqlFunctionsConfigurationTest.class.getClassLoader());
        } catch (MalformedURLException e) {
            throw new RuntimeException("Define property p2p.uri.cls.second", e);
        }
    }

    /** */
    private ClassLoader clsLoader;

    /** */
    private CacheConfiguration<?, ?> staticCacheCfg;

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

        if (clsLoader != null)
            cfg.setClassLoader(clsLoader);

        if (staticCacheCfg != null)
            cfg.setCacheConfiguration(staticCacheCfg);

        return cfg;
    }

    /**
     * Test verifies that if a class is partially available to the servers (some servers have it on classpath,
     * others don't) a request to start a cache with that class in configuration fails with proper error,
     * no nodes fail or hang.
     *
     * @throws Exception If test fails to set up necessary environment.
     */
    @Test
    @Ignore("https://ggsystems.atlassian.net/browse/GG-36341")
    public void testClientStartsDynamicCacheWithPartiallyUnavailableClass() throws Exception {
        clsLoader = CONFIGURATION_CLASS_LOADER;

        //classloader with a specific class is available to one server...
        startGrid(0);

        IgniteEx cl0 = startClientGrid(1);

        clsLoader = null;

        //... but not to the other
        startGrid(2);

        try {
            cl0.getOrCreateCache(new CacheConfiguration<>(DYNAMIC_CACHE_NAME)
                .setSqlFunctionClasses(CONFIGURATION_CLASS_LOADER.loadClass(UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME)));
        }
        catch (Exception e) {
            String errorMsg = e.getMessage();

            assertTrue("Exception during cache creation is expected to be about a class not found "
                    + "during creation process but was: '" + errorMsg + "'",
                errorMsg.contains("ClassNotFoundException"));

            assertTrue("Exception during cache creation is expected to be about a particular class: "
                    + UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME
                    + ", but was: '" + errorMsg + "'",
                errorMsg.contains(UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME));
        }

        checkTopology(3);
    }

    /**
     * Test verifies that dynamic cache request issued by a client node with a class unavailable on server side
     * fails with a proper exception.
     * However, no client nor server nodes fail as a result.
     *
     * @throws Exception If test fails to set up necessary environment.
     */
    @Test
    public void testClientStartsDynamicCacheWithUnavailableClass() throws Exception {
        startGrid(0);

        clsLoader = CONFIGURATION_CLASS_LOADER;

        IgniteEx cl0 = startClientGrid(1);

        try {
            cl0.getOrCreateCache(new CacheConfiguration<>(DYNAMIC_CACHE_NAME)
                .setSqlFunctionClasses(
                    CONFIGURATION_CLASS_LOADER.loadClass(UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME)));
        }
        catch (Exception e) {
//            String errorMsg = e.getMessage();
//
//            assertTrue("Exception during cache creation is expected to be about a class not found "
//                    + "during creation process but was: '" + errorMsg + "'",
//                errorMsg.contains("ClassNotFoundException"));
//
//            assertTrue("Exception during cache creation is expected to be about a particular class: "
//                    + UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME
//                    + ", but was: '" + errorMsg + "'",
//                errorMsg.contains(UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME));
        }

        checkTopology(2);
    }

    /**
     * Test verifies that explicit cache template (a template with asterisk in its name
     * added via addConfiguration cache API) dynamically created from a client node
     * with a class unavailable on server classpath fails with a proper exception.
     * However, no client nor server nodes fail as a result.
     *
     * @throws Exception If test fails to set up necessary environment.
     */
    @Test
    public void testClientAddsExplicitCacheTemplateWithUnavailableClass() throws Exception {
        startGrid(0);

        clsLoader = CONFIGURATION_CLASS_LOADER;

        IgniteEx cl0 = startClientGrid(1);

        try {
            cl0.addCacheConfiguration(new CacheConfiguration<>(DYNAMIC_CACHE_TEMPLATE)
                .setSqlFunctionClasses(clsLoader.loadClass(UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME)));
        }
        catch (Exception e) {
            String errorMsg = e.getMessage();

            assertTrue("Exception during cache template creation is expected to be about a class not found "
                    + "during creation process but was: '" + errorMsg + "'",
                errorMsg.contains("ClassNotFoundException"));

            assertTrue("Exception during explicit cache template creation is expected to contain information "
                    + "about template, but actual message was: '" + errorMsg + "'",
                errorMsg.contains("template"));

            assertTrue("Exception during cache template creation is expected to be about a particular class: "
                    + UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME
                    + ", but was: '" + errorMsg + "'",
                errorMsg.contains(UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME));
        }

        checkTopology(2);
    }

    /**
     * Test verifies that implicit cache template (a template with asterisk in its name
     * added via standard create cache API) dynamically created from a client node
     * with a class unavailable on server classpath fails with a proper exception.
     * However, no client nor server nodes fail as a result.
     *
     * @throws Exception If test fails to set up necessary environment.
     */
    @Test
    public void testClientAddsImplicitCacheTemplateWithUnavailableClass() throws Exception {
        startGrid(0);

        clsLoader = CONFIGURATION_CLASS_LOADER;

        IgniteEx cl0 = startClientGrid(1);

        try {
            cl0.createCache(new CacheConfiguration<>(DYNAMIC_CACHE_TEMPLATE)
                .setSqlFunctionClasses(clsLoader.loadClass(UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME)));
        }
        catch (Exception e) {
            String errorMsg = e.getMessage();

            assertTrue("Exception during cache template creation is expected to be about a class not found "
                + "during creation process but was: '" + errorMsg + "'",
                errorMsg.contains("ClassNotFoundException"));

            assertTrue("Exception during cache template creation is expected to be about a particular class: "
                + UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME
                + ", but was: '" + errorMsg + "'",
                errorMsg.contains(UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME));
        }

        checkTopology(2);
    }

    /**
     * Test verifies that server node fails to start when it encounters a class missing on its classpath
     * in static cache configuration.
     *
     * @throws Exception If test fails to set up necessary environment.
     */
    @Test
    public void testStaticCacheWithUnavailableClassInServerConfig() throws Exception {
        staticCacheCfg = new CacheConfiguration<>(STATIC_CACHE_NAME)
            .setSqlFunctionClasses(CONFIGURATION_CLASS_LOADER.loadClass(UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME));

        try {
            startGrid(0);

            fail("Server node is expected to fail when "
                + "handling configuration with unavailable class in cache configuration: "
                + UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME);
        }
        catch (Exception e) {
            assertTrue("Server expected to fail on ClassNotFoundException on loading "
                    + UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME + " class",
                X.hasCause(e, ClassNotFoundException.class));

            ClassNotFoundException cnfE = X.cause(e, ClassNotFoundException.class);

            assertTrue("ClassNotFoundException was thrown with the unexpected class: " + cnfE.getMessage(),
                cnfE.getMessage().contains(UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME));
        }

        checkTopology(0);
    }

    /**
     * Test verifies that server node fails to start when it encounters a class missing on its classpath
     * in static cache template configuration.
     *
     * @throws Exception If test fails to set up necessary environment.
     */
    @Test
    public void testStaticCacheTemplateWithUnavailableClassInServerConfig() throws Exception {
        staticCacheCfg = new CacheConfiguration<>(STATIC_CACHE_TEMPLATE).setBackups(5)
            .setSqlFunctionClasses(CONFIGURATION_CLASS_LOADER.loadClass(UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME));

        try {
            startGrid(0);

            fail("Server node is expected to fail when "
                + "handling configuration with unavailable class in cache template configuration: "
                + UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME);
        }
        catch (Exception e) {
            assertTrue("Server expected to fail on ClassNotFoundException on loading "
                    + UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME + " class",
                X.hasCause(e, ClassNotFoundException.class));

            ClassNotFoundException cnfE = X.cause(e, ClassNotFoundException.class);

            assertTrue("ClassNotFoundException was thrown with the unexpected class: " + cnfE.getMessage(),
                cnfE.getMessage().contains(UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME));
        }

        checkTopology(0);
    }

    /**
     * Test verifies that a client with a class in its configuration that is unavialable to a server
     * fails to join to the cluster but doesn't fail any server node.
     *
     * @throws Exception If test fails to set up necessary environment.
     */
    @Test
    public void testStaticCacheWithUnavailableClassInClientConfig() throws Exception {
        startGrid(0);

        clsLoader = CONFIGURATION_CLASS_LOADER;

        staticCacheCfg = new CacheConfiguration<>(STATIC_CACHE_NAME)
            .setSqlFunctionClasses(CONFIGURATION_CLASS_LOADER.loadClass(UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME));

        try {
            startClientGrid(1);

            fail("Client node is expected to fail when joining to a cluster that doesn't have on its classpath "
                + "some classes from client's configuration: "
                + UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME
            );
        }
        catch (Exception e) {
            assertTrue(X.hasCause(e, IgniteSpiException.class));

            IgniteSpiException spiE = X.cause(e, IgniteSpiException.class);

            /*assertTrue(spiE.getMessage().contains(UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME));*/
        }

        checkTopology(1);
    }

    @Test
    public void testStaticCacheWithAffinityAndNonAffinityNodesInTopology() throws Exception {
        startGrid("affNode");

        clsLoader = CONFIGURATION_CLASS_LOADER;

        startGrid("nonAffNode");

        staticCacheCfg = new CacheConfiguration<>(STATIC_CACHE_NAME)
            .setSqlFunctionClasses(CONFIGURATION_CLASS_LOADER.loadClass(UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME))
            .setNodeFilter(new AttributeFilter("affNode"));

        try {
            startClientGrid(1);
        }
        catch (Exception e) {
            fail("client node is expected to survive");
        }
    }

    private static class AttributeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private String instanceName;

        /** */
        private AttributeFilter(String instanceName) {
            this.instanceName = instanceName;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            String igniteInstanceName = node.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME);

            return instanceName.contains(igniteInstanceName);
        }
    }
}
