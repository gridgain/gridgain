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

package org.apache.ignite.logger.java;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Formatter for JUL logger.
 */
public class JavaLoggerFormatter extends Formatter {
    /** Name for anonymous loggers. */
    static final String ANONYMOUS_LOGGER_NAME = "UNKNOWN";

    /** Default date-time format. */
    static final DateTimeFormatter DEFAULT_DATE_FMT = IgniteUtils.ISO_DATE_FMT;

    /** Log message date-time formatter. */
    static final DateTimeFormatter DATE_FMT = resolveDateFormat();

    /** Property for overriding default dat-time format. */
    static final String IGNITE_JUL_DATE_FORMAT_PROPERTY = "IGNITE_JAVA_LOGGER_DATE_FORMAT";

    /** {@inheritDoc} */
    @Override public String format(LogRecord record) {
        String threadName = Thread.currentThread().getName();

        String logName = record.getLoggerName();

        if (logName == null || logName.equals(""))
            logName = ANONYMOUS_LOGGER_NAME;
        else if (logName.contains("."))
            logName = logName.substring(logName.lastIndexOf('.') + 1);

        return "[" + DATE_FMT.format(Instant.ofEpochMilli(record.getMillis())) + "][" +
            record.getLevel() + "][" +
            threadName + "][" +
            logName + "] " +
            formatMessage(record) +
            (record.getThrown() == null ? '\n' : formatStackTrace(record.getThrown()));
    }

    private static String formatStackTrace(@NotNull Throwable t) {
        StringWriter sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw)) {
            pw.println();
            t.printStackTrace(new PrintWriter(sw));
        }

        return sw.toString();
    }

    @NotNull
    private static DateTimeFormatter resolveDateFormat() {
        String customDateFormat = IgniteSystemProperties.getString(IGNITE_JUL_DATE_FORMAT_PROPERTY);
        if (customDateFormat != null) {
            try {
                return DateTimeFormatter.ofPattern(customDateFormat).withZone(ZoneId.systemDefault());
            } catch (Exception e) {
                System.err.println("Incorrect JavaLogger date-time format: " + customDateFormat);
            }
        }

        return DEFAULT_DATE_FMT;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JavaLoggerFormatter.class, this);
    }
}
