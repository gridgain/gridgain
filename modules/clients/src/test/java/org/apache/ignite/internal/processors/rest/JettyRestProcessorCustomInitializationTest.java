/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.rest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.xml.XmlConfiguration;
import org.junit.Test;
import org.xml.sax.SAXException;

/**
 *
 */
public class JettyRestProcessorCustomInitializationTest extends GridCommonAbstractTest {
    /** Path to Jetty configuration file. */
    private static final String JETTY_CFG_PATH = "modules/clients/src/test/resources/jetty/rest-jetty.xml";
    private static final int IGNITE_CUSTOM_JETTY_PORT = 8095;

    /** Local host. */
    protected static final String LOC_HOST = "127.0.0.1";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setLocalHost(LOC_HOST);

        assert cfg.getConnectorConfiguration() == null;

        ConnectorConfiguration clientCfg = new ConnectorConfiguration();

        clientCfg.setJettyServerFactory(new JettyServerFactory());

        cfg.setConnectorConfiguration(clientCfg);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        return cfg;
    }

    private static class JettyServerFactory implements Factory<Server> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public Server create() {
            log.warning(">>>>> Using custom Jetty server initialization.");

            URL cfgUrl = U.resolveIgniteUrl(JETTY_CFG_PATH);

            XmlConfiguration cfg;

            try {
                cfg = new XmlConfiguration(Resource.newResource(cfgUrl));
            }
            catch (FileNotFoundException e) {
                throw new IgniteSpiException("Failed to find configuration file: " + cfgUrl, e);
            }
            catch (SAXException e) {
                throw new IgniteSpiException("Failed to parse configuration file: " + cfgUrl, e);
            }
            catch (IOException e) {
                throw new IgniteSpiException("Failed to load configuration file: " + cfgUrl, e);
            }
            catch (Exception e) {
                throw new IgniteSpiException("Failed to start HTTP server with configuration file: " + cfgUrl, e);
            }

            try {
                return (Server)cfg.configure();
            }
            catch (Exception e) {
                throw new IgniteException("Failed to start Jetty HTTP server.", e);
            }
        }
    }

    @Test
    @WithSystemProperty(key = "IGNITE_JETTY_PORT", value = "" + IGNITE_CUSTOM_JETTY_PORT)
    public void testCustomFactory() throws Exception {
        startGrid(0);

        String addr = "http://" + LOC_HOST + ":" + IGNITE_CUSTOM_JETTY_PORT + "/ignite?cacheName=default&cmd=top";

        URL url = new URL(addr);

        URLConnection conn = url.openConnection();

        conn.connect();

        assertEquals(200, ((HttpURLConnection)conn).getResponseCode());
    }
}
