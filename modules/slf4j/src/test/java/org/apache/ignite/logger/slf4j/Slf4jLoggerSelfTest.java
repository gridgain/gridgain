/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.logger.slf4j;

import java.io.File;
import java.util.Collections;
import java.util.Objects;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.logging.log4j.LogManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Grid Slf4j SPI test.
 */
public class Slf4jLoggerSelfTest {
    /** Path to full log. */
    private static final String LOG_ALL = "work/log/all.log";

    /** Path to filtered log. */
    private static final String LOG_FILTERED = "work/log/filtered.log";

    /** */
    private static final String LOG_MESSAGE = "Text that should be present in logs";

    /** */
    private static final String LOG_EXCLUDED_MESSAGE = "Logs from org.springframework package should be excluded from logs";

    /** */
    @Before
    public void setUp() {
        deleteLogs();

        LogManager.shutdown();
    }

    /** */
    @After
    public void tearDown() {
        deleteLogs();
    }

    /**
     * Check that JUL is redirected to Slf4j .
     *
     * Start the local node and check presence of log file.
     * Check that this is really a log of a started node.
     * Log something using JUL logging.
     * Check that logs present in log file.
     * Log something in INFO level from package that is blocked in slf4j configuration using JUL logging.
     * Check that logs aren’t present in log file.
     *
     * @throws Exception If error occurs.
     */
    @Test
    public void testJULIsRedirectedToSlf4j() throws Exception {
        File logFile = checkOneNode();

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
     * Check that Apache Commons Logging is redirected to Slf4j.
     *
     * Start the local node and check for presence of log file.
     * Check that this is really a log of a started node.
     * Log something using Apache Commons Logging.
     * Check that logs present in log file.
     * Log something in INFO level from package that is blocked in slf4j configuration using Apache Commons Logging.
     * Check that logs aren’t present in log file.
     *
     * @throws Exception If error occurs.
     */
    @Test
    public void testJCLIsRedirectedToSlf4j() throws Exception {
        File logFile = checkOneNode();

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
     * Creates grid configuration.
     *
     * @return Grid configuration.
     */
    private static IgniteConfiguration getConfiguration() {
        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(false) {{
            setAddresses(Collections.singleton("127.0.0.1:47500..47509"));
        }});

        return new IgniteConfiguration()
            .setGridLogger(new Slf4jLogger(LoggerFactory.getLogger(Slf4jLoggerSelfTest.class)))
            .setConnectorConfiguration(null)
            .setDiscoverySpi(disco);
    }

    /**
     * Starts the local node and checks for presence of log file.
     * Also checks that this is really a log of a started node.
     *
     * @return Log file.
     * @throws Exception If error occurred.
     */
    private File checkOneNode() throws Exception {
        String id8;
        File logFile;

        try (Ignite ignite = G.start(getConfiguration())) {
            id8 = U.id8(ignite.cluster().localNode().id());

            logFile = U.resolveIgnitePath(LOG_ALL);
            assertNotNull("Failed to resolve path: " + LOG_ALL, logFile);
            assertTrue("Log file does not exist: " + LOG_ALL, logFile.exists());
        }

        String logContent = U.readFileToString(logFile.getAbsolutePath(), "UTF-8");

        assertTrue("Log file does not contain it's node ID: " + logFile,
            logContent.contains(">>> Local node [ID=" + id8.toUpperCase()));

        return logFile;
    }

    /** Delete existing logs, if any */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void deleteLogs() {
        Stream.of(LOG_ALL, LOG_FILTERED)
            .map(U::resolveIgnitePath)
            .filter(Objects::nonNull)
            .forEach(File::delete);
    }
}
