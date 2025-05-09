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

package org.apache.ignite;

import org.apache.ignite.internal.util.tostring.GridToStringExclude;

/**
 * This interface defines basic logging functionality used throughout the system. We had to
 * abstract it out so that we can use whatever logging is used by the hosting environment.
 * Currently, <a target=_new href="http://logging.apache.org/log4j/1.2/">log4j</a>,
 * <a target=_new href="https://docs.jboss.org/hibernate/orm/5.4/topical/html_single/logging/Logging.html">JBoss</a>,
 * <a target=_new href="http://jakarta.apache.org/commons/logging/">JCL</a> and
 * console logging are provided as supported implementations.
 * <p>
 * Ignite logger could be configured either from code (for example log4j logger):
 * <pre name="code" class="java">
 *      IgniteConfiguration cfg = new IgniteConfiguration();
 *      ...
 *      URL xml = U.resolveIgniteUrl("config/custom-log4j.xml");
 *      IgniteLogger log = new Log4JLogger(xml);
 *      ...
 *      cfg.setGridLogger(log);
 * </pre>
 * or in grid configuration file (see JCL logger example below):
 * <pre name="code" class="xml">
 *      ...
 *      &lt;property name="gridLogger"&gt;
 *          &lt;bean class="org.apache.ignite.logger.jcl.JclLogger"&gt;
 *              &lt;constructor-arg type="org.apache.commons.logging.Log"&gt;
 *                  &lt;bean class="org.apache.commons.logging.impl.Log4JLogger"&gt;
 *                      &lt;constructor-arg type="java.lang.String" value="config/ignite-log4j.xml"/&gt;
 *                  &lt;/bean&gt;
 *              &lt;/constructor-arg&gt;
 *          &lt;/bean&gt;
 *      &lt;/property&gt;
 *      ...
 * </pre>
 * It's recommended to use Ignite's logger injection instead of using/instantiating
 * logger in your task/job code. See {@link org.apache.ignite.resources.LoggerResource} annotation about logger
 * injection.
 * <h1 class="header">Quiet Mode</h1>
 * By default Ignite starts in un-suppressed logging mode. If system property {@code IGNITE_QUIET} is set to
 * {@code true} than Ignition will suppress {@code INFO} and {@code DEBUG} log output. Note that all output in "quiet"
 * mode is done through standard output (STDOUT).
 * <p>
 * Note that Ignite's standard startup scripts <tt>$IGNITE_HOME/bin/ignite.{sh|bat}</tt> start
 * by default in "verbose" mode. Both scripts accept {@code -q} arguments to turn on "quiet" mode.
 */
@GridToStringExclude
public interface IgniteLogger {
    /**
     * Marker for log messages that are useful in development environments, but not in production.
     */
    String DEV_ONLY = "DEV_ONLY";

    /**
     * Creates new logger with given category based off the current instance.
     *
     * @param ctgr Category for new logger.
     * @return New logger with given category.
     */
    public IgniteLogger getLogger(Object ctgr);

    /**
     * Logs out trace message.
     *
     * @param msg Trace message.
     */
    public void trace(String msg);

    /**
     * Logs out trace message.
     * The default implementation calls {@code this.trace(msg)}.
     *
     * @param marker Name of the marker to be associated with the message.
     * @param msg Trace message.
     */
    public default void trace(String marker, String msg) {
        trace(msg);
    }

    /**
     * Logs out debug message.
     *
     * @param msg Debug message.
     */
    public void debug(String msg);

    /**
     * Logs out debug message.
     * The default implementation calls {@code this.debug(msg)}.
     *
     * @param marker Name of the marker to be associated with the message.
     * @param msg Debug message.
     */
    public default void debug(String marker, String msg) {
        debug(msg);
    }

    /**
     * Logs out information message.
     *
     * @param msg Information message.
     */
    public void info(String msg);

    /**
     * Logs out information message.
     * The default implementation calls {@code this.info(msg)}.
     *
     * @param marker Name of the marker to be associated with the message.
     * @param msg Information message.
     */
    public default void info(String marker, String msg) {
        info(msg);
    }

    /**
     * Logs out warning message.
     *
     * @param msg Warning message.
     */
    public default void warning(String msg) {
        warning(msg, null);
    }

    /**
     * Logs out warning message with optional exception.
     *
     * @param msg Warning message.
     * @param e Optional exception (can be {@code null}).
     */
    public void warning(String msg, Throwable e);

    /**
     * Logs out warning message with optional exception.
     * The default implementation calls {@code this.warning(msg)}.
     *
     * @param marker Name of the marker to be associated with the message.
     * @param msg Warning message.
     * @param e Optional exception (can be {@code null}).
     */
    public default void warning(String marker, String msg, Throwable e) {
        warning(msg, e);
    }

    /**
     * Logs out error message.
     *
     * @param msg Error message.
     */
    public default void error(String msg) {
        error(null, msg, null);
    }

    /**
     * Logs error message with optional exception.
     *
     * @param msg Error message.
     * @param e Optional exception (can be {@code null}).
     */
    public void error(String msg, Throwable e);

    /**
     * Logs error message with optional exception.
     * The default implementation calls {@code this.error(msg)}.
     *
     * @param marker Name of the marker to be associated with the message.
     * @param msg Error message.
     * @param e Optional exception (can be {@code null}).
     */
    public default void error(String marker, String msg, Throwable e) {
        error(msg, e);
    }

    /**
     * Tests whether {@code trace} level is enabled.
     *
     * @return {@code true} in case when {@code trace} level is enabled, {@code false} otherwise.
     */
    public boolean isTraceEnabled();

    /**
     * Tests whether {@code debug} level is enabled.
     *
     * @return {@code true} in case when {@code debug} level is enabled, {@code false} otherwise.
     */
     public boolean isDebugEnabled();

    /**
     * Tests whether {@code info} level is enabled.
     *
     * @return {@code true} in case when {@code info} level is enabled, {@code false} otherwise.
     */
    public boolean isInfoEnabled();

    /**
     * Tests whether Logger is in "Quiet mode".
     *
     * @return {@code true} "Quiet mode" is enabled, {@code false} otherwise
     */
    public boolean isQuiet();

    /**
     * Gets name of the file being logged to if one is configured or {@code null} otherwise.
     *
     * @return Name of the file being logged to if one is configured or {@code null} otherwise.
     */
    public String fileName();
}
