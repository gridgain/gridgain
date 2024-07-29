/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.commandline;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests Command Handler logging configuration.
 */
public class CommandHandlerLoggingTest {

    private static final String DEFAULT_LOGGER_NAME = "org.apache.ignite.internal.commandline.CommandHandlerLog";

    private static final String CUSTOM_LOGGER_NAME = "org.apache.ignite.internal.commandline.CommandHandler";

    private static final String ROOT_LOGGER_NAME = "";

    /**
     * Verifies that custom logging configuration is provided and is not overriden.
     */
    @Test
    @WithSystemProperty(key = "java.util.logging.config.file", value = "control-utility-logging.properties")
    public void testExternalConfigIsNotOverridden() {
        String resourceName = "control-utility-logging.properties";
        String resourcePath = getClass().getClassLoader().getResource(resourceName).getPath();

        System.setProperty("java.util.logging.config.file", resourcePath);

        CommandHandler cmd = new CommandHandler();

        ArrayList<String> loggers = Collections.list(LogManager.getLogManager().getLoggerNames());
        assertTrue(loggers.contains(CUSTOM_LOGGER_NAME));

        // check that custom logger has no handlers
        Logger customLogger = LogManager.getLogManager().getLogger(CUSTOM_LOGGER_NAME);
        assertEquals(0, customLogger.getHandlers().length);

        // check that root logger has exactly one file handler
        Logger rootLogger = LogManager.getLogManager().getLogger(ROOT_LOGGER_NAME);
        assertEquals(1, rootLogger.getHandlers().length);
        assertEquals(FileHandler.class, rootLogger.getHandlers()[0].getClass());
    }

    /**
     * Verifies default logging configuration.
     */
    @Test
    public void testLoggingDefaultConfig() {
        new CommandHandler();

        ArrayList<String> loggers = Collections.list(LogManager.getLogManager().getLoggerNames());
        assertTrue(loggers.contains(DEFAULT_LOGGER_NAME));

        Logger logger = LogManager.getLogManager().getLogger(DEFAULT_LOGGER_NAME);

        Handler[] handlers = logger.getHandlers();

        FileHandler fileHandler = Arrays.stream(handlers)
            .filter(h -> h instanceof FileHandler)
            .map(h -> (FileHandler) h)
            .findFirst()
            .orElseThrow(() -> new AssertionError("FileHandler not found"));

        StreamHandler consoleHandler = Arrays.stream(handlers)
            .filter(h -> h instanceof StreamHandler)
            .map(h -> (StreamHandler) h)
            .findFirst()
            .orElseThrow(() -> new AssertionError("StreamHandler not found"));
    }

    @After
    public void tearDown() {
        System.clearProperty("java.util.logging.config.file");
    }
}
