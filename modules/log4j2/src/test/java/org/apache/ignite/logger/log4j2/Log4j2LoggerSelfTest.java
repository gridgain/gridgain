/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.logger.log4j2;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.LoggerNodeIdAndApplicationAware;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Grid Log4j2 SPI test.
 */
public class Log4j2LoggerSelfTest {
    /** */
    private static final String LOG_PATH_TEST = "modules/core/src/test/config/log4j2-test.xml";

    /** */
    private static final String LOG_PATH_MAIN = "config/ignite-log4j2.xml";

    /** */
    private static final String LOG_MESSAGE = "Text that should be present in logs";

    /** */
    private static final String LOG_EXCLUDED_MESSAGE = "Logs from org.springframework package should be excluded from logs";

    /** */
    @Before
    public void setUp() {
        Log4J2Logger.cleanup();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFileConstructor() throws Exception {
        File xml = GridTestUtils.resolveIgnitePath(LOG_PATH_TEST);

        assert xml != null;
        assert xml.exists();

        IgniteLogger log = new Log4J2Logger(xml).getLogger(getClass());

        System.out.println(log.toString());

        assertTrue(log.toString().contains("Log4J2Logger"));
        assertTrue(log.toString().contains(xml.getPath()));

        ((LoggerNodeIdAndApplicationAware)log).setApplicationAndNode(null, UUID.randomUUID());

        checkLog(log);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUrlConstructor() throws Exception {
        File xml = GridTestUtils.resolveIgnitePath(LOG_PATH_TEST);

        assert xml != null;
        assert xml.exists();

        URL url = xml.toURI().toURL();
        IgniteLogger log = new Log4J2Logger(url).getLogger(getClass());

        System.out.println(log.toString());

        assertTrue(log.toString().contains("Log4J2Logger"));
        assertTrue(log.toString().contains(url.getPath()));

        ((LoggerNodeIdAndApplicationAware)log).setApplicationAndNode(null, UUID.randomUUID());

        checkLog(log);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPathConstructor() throws Exception {
        IgniteLogger log = new Log4J2Logger(LOG_PATH_TEST).getLogger(getClass());

        System.out.println(log.toString());

        assertTrue(log.toString().contains("Log4J2Logger"));
        assertTrue(log.toString().contains(LOG_PATH_TEST));

        ((LoggerNodeIdAndApplicationAware)log).setApplicationAndNode(null, UUID.randomUUID());

        checkLog(log);
    }

    /**
     * Tests log4j logging SPI.
     */
    private void checkLog(IgniteLogger log) {
        assert !log.isDebugEnabled();
        assert log.isInfoEnabled();

        log.debug("This is 'debug' message.");
        log.info("This is 'info' message.");
        log.warning("This is 'warning' message.");
        log.warning("This is 'warning' message.", new Exception("It's a test warning exception"));
        log.error("This is 'error' message.");
        log.error("This is 'error' message.", new Exception("It's a test error exception"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSystemNodeId() throws Exception {
        UUID id = UUID.randomUUID();

        new Log4J2Logger(LOG_PATH_TEST).setApplicationAndNode(null, id);

        assertEquals(U.id8(id), System.getProperty("nodeId"));
        assertEquals("ignite", System.getProperty("appId"));

        new Log4J2Logger(LOG_PATH_TEST).setApplicationAndNode("other-app", id);

        assertEquals(U.id8(id), System.getProperty("nodeId"));
        assertEquals("other-app", System.getProperty("appId"));
    }

    /**
     * Tests correct behaviour in case 2 local nodes are started.
     *
     * @throws Exception If error occurs.
     */
    @Test
    public void testLogFilesTwoNodes() throws Exception {
        checkOneNode(0);
        checkOneNode(1);
    }

    /**
     * Check that JUL is redirected to Log4j2 .
     *
     * Start the local node and check presence of log file.
     * Check that this is really a log of a started node.
     * Log something using JUL logging.
     * Check that logs present in log file.
     * Log something in INFO level from package that is blocked in log4j2 configuration using JUL logging.
     * Check that logs aren’t present in log file.
     *
     * @throws Exception If error occurs.
     */
    @Test
    public void testJULIsRedirectedToLog4j2() throws Exception {
        File logFile = checkOneNode(0);

        Logger.getLogger(this.getClass().getName()).info(LOG_MESSAGE);
        Logger.getLogger("org.springframework.context.ApplicationContext").info(LOG_EXCLUDED_MESSAGE);

        String logs = U.readFileToString(logFile.getAbsolutePath(), "UTF-8");

        assertTrue("Logs from JUL logger should be present in log file", logs.contains(LOG_MESSAGE));
        assertFalse(
            "JUL INFO logs for org.springframework package are present in log file",
            logs.contains(LOG_EXCLUDED_MESSAGE)
        );
    }

    /**
     * Check that Apache Commons Logging is redirected to Log4j2.
     *
     * Start the local node and check for presence of log file.
     * Check that this is really a log of a started node.
     * Log something using Apache Commons Logging.
     * Check that logs present in log file.
     * Log something in INFO level from package that is blocked in log4j2 configuration using Apache Commons Logging.
     * Check that logs aren’t present in log file.
     *
     * @throws Exception If error occurs.
     */
    @Test
    public void testJCLIsRedirectedToLog4j2() throws Exception {
        File logFile = checkOneNode(0);

        LogFactory.getLog(this.getClass()).info(LOG_MESSAGE);
        LogFactory.getLog("org.springframework.context.ApplicationContext").info(LOG_EXCLUDED_MESSAGE);

        String logs = U.readFileToString(logFile.getAbsolutePath(), "UTF-8");

        assertTrue("Logs from JCL logger should be present in log file", logs.contains(LOG_MESSAGE));
        assertFalse(
            "JCL INFO logs for org.springframework package are present in log file",
            logs.contains(LOG_EXCLUDED_MESSAGE)
        );
    }

    /**
     * Starts the local node and checks for presence of log file.
     * Also checks that this is really a log of a started node.
     *
     * @param id Test-local node ID.
     * @return Log file.
     * @throws Exception If error occurred.
     */
    private File checkOneNode(int id) throws Exception {
        String id8;
        File logFile;

        try (Ignite ignite = G.start(getConfiguration("grid" + id, LOG_PATH_MAIN))) {
            id8 = U.id8(ignite.cluster().localNode().id());

            String logPath = "work/log/ignite-" + id8 + ".log";

            logFile = U.resolveIgnitePath(logPath);
            assertNotNull("Failed to resolve path: " + logPath, logFile);
            assertTrue("Log file does not exist: " + logFile, logFile.exists());

            assertEquals(logFile.getAbsolutePath(), ignite.log().fileName());
        }

        String logContent = U.readFileToString(logFile.getAbsolutePath(), "UTF-8");

        assertTrue("Log file does not contain it's node ID: " + logFile,
            logContent.contains(">>> Local node [ID=" + id8.toUpperCase()));

        return logFile;
    }

    /**
     * Creates grid configuration.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param logPath Logger configuration path.
     * @return Grid configuration.
     * @throws Exception If error occurred.
     */
    private static IgniteConfiguration getConfiguration(String igniteInstanceName, String logPath)
        throws Exception {
        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(false) {{
            setAddresses(Collections.singleton("127.0.0.1:47500..47509"));
        }});

        return new IgniteConfiguration()
            .setIgniteInstanceName(igniteInstanceName)
            .setGridLogger(new Log4J2Logger(logPath))
            .setConnectorConfiguration(null)
            .setDiscoverySpi(disco);
    }
}
