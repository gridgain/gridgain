package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;

/**
 * Test class implements scenarios for integration testing of how indexing code works
 * with classes containing custom SQL functions.
 */
public class P2PCustomClassesAvailabilityTest extends GridCommonAbstractTest {
    /** Test class loader. */
    private static final ClassLoader CONFIGURATION_CLASS_LOADER;

    /** */
    public static final String CACHE_NAME = "PUBLIC_CACHE";

    /** */
    private static final String UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME =
        "org.apache.ignite.tests.p2p.cache.UnavailableToServerCustomSqlFunctionsClass";

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

    static {
        try {
            CONFIGURATION_CLASS_LOADER = new URLClassLoader(
                new URL[]{new URL(GridTestProperties.getProperty("p2p.uri.cls"))},
                P2PCustomClassesAvailabilityTest.class.getClassLoader());
        } catch (MalformedURLException e) {
            throw new RuntimeException("Define property p2p.uri.cls", e);
        }
    }

    /** */
    private ClassLoader clsLoader;

    /** */
    private CacheConfiguration<?, ?> staticCacheCfg;

    /**
     * Test verifies the case when a class with custom SQL functions is available only on a coordinator.
     *
     * This allows a client node with the class available on its classpath to pass validation and join topology, but
     * later on the other nodes in the topology fail to process client's requests because of class locally unavailable.
     *
     * @throws Exception If failed.
     */
    @Test
    @Ignore("https://ggsystems.atlassian.net/browse/GG-36586")
    public void testSqlFunctionsClassAvailableOnCoordinatorOnly() throws Exception {
        clsLoader = CONFIGURATION_CLASS_LOADER;

        startGrid(0);

        clsLoader = null;

        startGrid(1);

        clsLoader = CONFIGURATION_CLASS_LOADER;

        staticCacheCfg = new CacheConfiguration<>(CACHE_NAME)
            .setSqlSchema("PUBLIC")
            .setQueryEntities(Collections.singleton(
                new QueryEntity(Integer.class, Long.class).setTableName(CACHE_NAME)))
            .setSqlFunctionClasses(CONFIGURATION_CLASS_LOADER.loadClass(UNAVAILABLE_TO_SERVER_SQL_FUNCTIONS_CLASS_NAME));

        try {
            startClientGrid(2);
        }
        catch (Throwable t) {
            fail("Unexpected exception was thrown: " + t);
        }
    }
}
