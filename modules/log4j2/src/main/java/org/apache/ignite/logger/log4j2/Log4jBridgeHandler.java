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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Handler;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.spi.ExtendedLogger;

import static java.util.Arrays.stream;

/**
 * Bridge from JUL to log4j2.<br> This is an alternative to log4j.jul.LogManager (running as complete JUL replacement),
 * especially useful for webapps running on a container for which the LogManager cannot or should not be used.<br><br>
 *
 * Installation/usage:<ul>
 * <li> Declaratively inside JUL's <code>logging.properties</code>:<br>
 * <code>handlers = org.apache.logging.log4j.jul.Log4jBridgeHandler</code><br>
 * <li> Programmatically by calling <code>install()</code> method,
 * e.g. inside ServletContextListener static-class-init. or contextInitialized()
 * </ul>
 */
public class Log4jBridgeHandler extends java.util.logging.Handler {
    /** The caller of the logging is java.util.logging.Logger (for location info). */
    private static final String FQCN = java.util.logging.Logger.class.getName();

    /** Unknown logger name. */
    private static final String UNKNOWN_LOGGER_NAME = "unknown.jul.logger";

    /** JUL formatter. */
    private static final java.util.logging.Formatter julFormatter = new java.util.logging.SimpleFormatter();

    /**
     * Custom Log4j level corresponding to the {@link java.util.logging.Level#FINEST} logging level. This maps to a
     * level more specific than {@link org.apache.logging.log4j.Level#TRACE}.
     */
    private static final Level FINEST = Level.forName("FINEST", Level.TRACE.intLevel() + 100);

    /**
     * Custom Log4j level corresponding to the {@link java.util.logging.Level#CONFIG} logging level. This maps to a
     * level in between {@link org.apache.logging.log4j.Level#INFO} and {@link org.apache.logging.log4j.Level#DEBUG}.
     */
    private static final Level CONFIG = Level.forName("CONFIG", Level.INFO.intLevel() + 50);

    /** Mapping between JUL log levels and Log4j log levels. */
    private final ConcurrentMap<java.util.logging.Level, Level> julToLog4j = new ConcurrentHashMap<>(U.capacity(9));

    /** Sorted JUL levels. */
    private final List<java.util.logging.Level> sortedJulLevels = new ArrayList<>(9);

    /**
     * Default constructor.
     */
    public Log4jBridgeHandler() {
        julToLog4j.put(java.util.logging.Level.ALL, Level.ALL);
        julToLog4j.put(java.util.logging.Level.FINEST, FINEST);
        julToLog4j.put(java.util.logging.Level.FINER, Level.TRACE);
        julToLog4j.put(java.util.logging.Level.FINE, Level.DEBUG);
        julToLog4j.put(java.util.logging.Level.CONFIG, CONFIG);
        julToLog4j.put(java.util.logging.Level.INFO, Level.INFO);
        julToLog4j.put(java.util.logging.Level.WARNING, Level.WARN);
        julToLog4j.put(java.util.logging.Level.SEVERE, Level.ERROR);
        julToLog4j.put(java.util.logging.Level.OFF, Level.OFF);

        sortedJulLevels.addAll(julToLog4j.keySet());
        sortedJulLevels.sort(JulLevelComparator.INSTANCE);
    }

    /**
     * Adds a new Log4jBridgeHandler instance to JUL's root logger.
     */
    public static void install() {
        if (Log4jBridgeHandler.isInstalled())
            return;

        java.util.logging.Logger rootLog = getJulRootLogger();

        for (Handler handler : rootLog.getHandlers())
            if (handler instanceof java.util.logging.ConsoleHandler)
                rootLog.removeHandler(handler);

        rootLog.addHandler(new Log4jBridgeHandler());
    }

    /**
     * Returns true if Log4jBridgeHandler has been previously installed, returns false otherwise.
     *
     * @return {@code True} if Log4jBridgeHandler is already installed, {@code false} other wise
     */
    public static boolean isInstalled() {
        return stream(getJulRootLogger().getHandlers()).anyMatch(handler -> handler instanceof Log4jBridgeHandler);
    }

    /** {@inheritDoc} */
    @Override public void publish(java.util.logging.LogRecord record) {
        // Silently ignore null records.
        if (record == null)
            return;

        Logger log4jLog = getLog4jLogger(record);

        Level level = toLevel(record.getLevel());
        String msg = julFormatter.formatMessage(record); // use JUL's implementation to get real msg
        Throwable thrown = record.getThrown();

        if (log4jLog instanceof ExtendedLogger) {
            try {
                ((ExtendedLogger)log4jLog).logIfEnabled(FQCN, level, null, msg, thrown);
            }
            catch (NoClassDefFoundError e) {
                // Sometimes there are problems with log4j.ExtendedStackTraceElement, so try a workaround.
                log4jLog.warn("Log4jBridgeHandler: ignored exception when calling 'ExtendedLogger'", e);
                log4jLog.log(level, msg, thrown);
            }
        }
        else
            log4jLog.log(level, msg, thrown);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void flush() {
        // No-op.
    }

    /**
     * Return the root logger instance.
     */
    private static java.util.logging.Logger getJulRootLogger() {
        return java.util.logging.LogManager.getLogManager().getLogger("");
    }

    /**
     * Return the log4j-Logger instance that will be used for logging.
     */
    private Logger getLog4jLogger(java.util.logging.LogRecord record) {
        String name = record.getLoggerName();
        if (name == null)
            name = UNKNOWN_LOGGER_NAME;

        return org.apache.logging.log4j.LogManager.getLogger(name);
    }

    /**
     * Converts a JDK logging Level to a Log4j logging Level.
     *
     * @param julLevel JUL Level to convert, may be null per the JUL specification.
     * @return converted Level or {@code null} if the given level could not be converted.
     */
    private Level toLevel(java.util.logging.Level julLevel) {
        if (julLevel == null)
            return null;

        final Level level = julToLog4j.get(julLevel);

        if (level != null)
            return level;

        Level log4jLevel = nearestLevel(julLevel);

        julToLog4j.put(julLevel, log4jLevel);

        return log4jLevel;
    }

    /**
     * @param julLevel JUL level.
     */
    private Level nearestLevel(java.util.logging.Level julLevel) {
        for (java.util.logging.Level level : sortedJulLevels) {
            if (JulLevelComparator.INSTANCE.compare(julLevel, level) <= 0)
                return julToLog4j.get(level);
        }

        return Level.OFF;
    }

    /**
     * JUL level order comparator.
     */
    private static final class JulLevelComparator implements Comparator<java.util.logging.Level> {
        /** Instance. */
        public static final Comparator<java.util.logging.Level> INSTANCE = new JulLevelComparator();

        /** {@inheritDoc} */
        @Override public int compare(java.util.logging.Level level1, java.util.logging.Level level2) {
            return Integer.compare(level1.intValue(), level2.intValue());
        }
    }
}
