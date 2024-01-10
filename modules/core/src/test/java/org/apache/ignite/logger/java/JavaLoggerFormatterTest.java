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

package org.apache.ignite.logger.java;

import java.time.Instant;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

/**
 * Java log formatter test.
 */
@GridCommonTest(group = "Logger")
public class JavaLoggerFormatterTest extends GridCommonAbstractTest {

    private static final JavaLoggerFormatter formatter = new JavaLoggerFormatter();

    /**
     * Null category should fall back to anonymous.
     */
    @Test
    public void testNullCategory() {
        assertFormattedOutput(null, JavaLoggerFormatter.ANONYMOUS_LOGGER_NAME);
    }

    /**
     * Empty (zero-length) category should fall back to anonymous.
     */
    @Test
    public void testEmptyCategory() {
        assertFormattedOutput("", JavaLoggerFormatter.ANONYMOUS_LOGGER_NAME);
    }

    /**
     * Should use specified literal as category.
     */
    @Test
    public void testSimpleCategory() {
        assertFormattedOutput("foo", "foo");
    }

    /**
     * Should use classname as category (cut off package name).
     */
    @Test
    public void testClassCategory() {
        assertFormattedOutput(JavaLoggerFormatterTest.class.getName(), "JavaLoggerFormatterTest");
    }

    /**
     * Should append stack trace to log message.
     */
    @Test
    public void testThrowable() {
        // given
        final String msg = "Greed is good";
        final String category = "probe";
        final String exceptionMsg = "Not enough minerals";

        LogRecord record = buildLogRecord(msg, category);
        record.setThrown(new RuntimeException(exceptionMsg));

        // when
        String result = formatter.format(record);

        // then
        final String expected = buildExpectedString(msg, category) + "\njava.lang.RuntimeException: " + exceptionMsg;
        assertStartsWith(expected, result);
    }

    private static void assertFormattedOutput(String msg, String category, String expectedCategory) {
        // given
        LogRecord record = buildLogRecord(msg, category);

        // when
        String result = formatter.format(record);

        // then
        assertStartsWith(buildExpectedString(msg, expectedCategory), result);
    }

    private static void assertFormattedOutput(String category, String expectedCategory) {
        assertFormattedOutput("Power overwhelming", category, expectedCategory);
    }

    private static final String BASE_DATE = "2000-01-01T01:01:01.001";

    private static LogRecord buildLogRecord(@NotNull String message, @Nullable String category) {
        LogRecord record = new LogRecord(Level.INFO, message);
        record.setMillis(BASE_INSTANT.toEpochMilli());
        record.setThreadID(Math.toIntExact(Thread.currentThread().getId()));
        record.setLoggerName(category);

        return record;
    }

    private static String buildExpectedString(@NotNull String message, @Nullable String category) {
        return "[" +
            BASE_DATE + "][" +
            Level.INFO + "][" +
            Thread.currentThread().getName() + "][" +
            category + "] " +
            message;
    }

    private static final Instant BASE_INSTANT = Instant.from(IgniteUtils.ISO_DATE_FMT.parse(BASE_DATE));

    private static void assertStartsWith(String expected, String actual) {
        assertThat(actual, startsWith(expected));
    }
}
