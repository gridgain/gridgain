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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Java log formatter test.
 */
@GridCommonTest(group = "Logger")
public class JavaLoggerFormatterTest {

    private static JavaLoggerFormatter formatter;

    @BeforeClass
    public static void beforeClass() throws Exception {
        formatter = new JavaLoggerFormatter();
    }

    @Test
    public void testNullCategory() {
        assertFormattedOutput(null, JavaLoggerFormatter.ANONYMOUS_LOGGER_NAME);
    }

    @Test
    public void testEmptyCategory() {
        assertFormattedOutput("", JavaLoggerFormatter.ANONYMOUS_LOGGER_NAME);
    }

    @Test
    public void testSimpleCategory() {
        assertFormattedOutput("foo", "foo");
    }

    @Test
    public void testClassCategory() {
        assertFormattedOutput(JavaLoggerFormatterTest.class.getName(), "JavaLoggerFormatterTest");
    }

    @Test
    public void testThrowable() {
        // given
        final String msg = "Greed is good";
        final String category = "probe";
        final String exceptionMsg = "Not enough minerals";

        LogRecord record = buildLogRecord(msg, category);
        try {
            throw new RuntimeException(exceptionMsg);
        }
        catch (Throwable t) {
            record.setThrown(t);
        }

        // when
        String result = formatter.format(record);

        // then
        final String expected = buildExpectedString(msg, category) + "\njava.lang.RuntimeException: " + exceptionMsg;
        assertSimilar(expected, result);
    }

    @Test
    public void testCorrectCustomFormatProperty() {
        // given
        System.setProperty("IGNITE_JAVA_LOGGER_DATE_FORMAT", "yyyy/MM/dd HH:mm:ss");

        // when
        DateTimeFormatter fmt = JavaLoggerFormatter.resolveDateFormat();

        // then
        assertEquals("2000/01/01 01:01:01", fmt.format(Instant.ofEpochMilli(BASE_MILLIS)));
    }

    @Test
    public void testIncorrectCustomFormatProperty() {
        // given
        System.setProperty("IGNITE_JAVA_LOGGER_DATE_FORMAT", "foobar");

        // when
        DateTimeFormatter fmt = JavaLoggerFormatter.resolveDateFormat();

        // then
        assertEquals(BASE_DATE, fmt.format(Instant.ofEpochMilli(BASE_MILLIS)));
    }

    void assertFormattedOutput(String msg, String category, String expectedCategory) {
        // given
        LogRecord record = buildLogRecord(msg, category);

        // when
        String result = formatter.format(record);

        // then
        assertSimilar(buildExpectedString(msg, expectedCategory), result);
    }

    void assertFormattedOutput(String category, String expectedCategory) {
        assertFormattedOutput("Power overwhelming", category, expectedCategory);
    }

    static final DateTimeFormatter ISO_DATE_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss,SSS").withZone(ZoneId.systemDefault());

    static final String BASE_DATE = "2000-01-01T01:01:01,001";

    static final long BASE_MILLIS = Instant.from(ISO_DATE_FORMAT.parse(BASE_DATE)).toEpochMilli();

    static LogRecord buildLogRecord(@NotNull String message, @Nullable String category) {
        LogRecord record = new LogRecord(Level.INFO, message);
        record.setMillis(BASE_MILLIS);
        record.setThreadID(Math.toIntExact(Thread.currentThread().getId()));
        record.setLoggerName(category);

        return record;
    }

    static String buildExpectedString(@NotNull String message, @Nullable String category) {
        return "[" +
            BASE_DATE + "][" +
            Level.INFO + "][" +
            Thread.currentThread().getName() + "][" +
            category + "] " +
            message;
    }

    static void assertSimilar(String expected, String actual) {
        assertTrue("\nExpected: " + expected + "\nActual:   " + actual, actual.startsWith(expected));
    }

}