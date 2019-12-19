/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.failure;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableSet;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.failure.TestFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.failure.FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_BLOCKED;
import static org.apache.ignite.internal.util.IgniteUtils.THREAD_DUMP_MSG;

/**
 * Tests for throttling thread dumps during handling failures.
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, value = "true")
public class FailureProcessorThreadDumpThrottlingTest extends GridCommonAbstractTest {
    /** Test logger. */
    private static TestLogger testLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration(igniteInstanceName);

        TestFailureHandler hnd = new TestFailureHandler(false);

        hnd.setIgnoredFailureTypes(ImmutableSet.of(FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT, SYSTEM_WORKER_BLOCKED));

        cfg.setFailureHandler(hnd);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /**
     * Tests that thread dumps will not get if {@code IGNITE_DUMP_THREADS_ON_FAILURE == false}.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE_THROTTLING_TIMEOUT, value = "0")
    public void testNoThreadDumps() throws Exception {
        IgniteEx ignite = null;

        try {
            testLog = new TestLogger(log, Collections.singleton(s -> s.contains(THREAD_DUMP_MSG)));

            ignite = (IgniteEx)startGrid();

            FailureContext failureCtx =
                    new FailureContext(SYSTEM_WORKER_BLOCKED, new Throwable("Failure context error"));

            for (int i = 0; i < 3; i++)
                ignite.context().failure().process(failureCtx);

            assertTrue(testLog.filteredRecords().isEmpty());
        }
        finally {
            if (ignite != null)
                stopAllGrids();
        }
    }

    /**
     * Tests that thread dumps will get for every failure for disabled throttling.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, value = "true")
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE_THROTTLING_TIMEOUT, value = "0")
    public void testNoThrottling() throws Exception {
        IgniteEx ignite = null;

        try {
            testLog = new TestLogger(log, Collections.singleton(s -> s.contains(THREAD_DUMP_MSG)));

            ignite = (IgniteEx)startGrid();

            FailureContext failureCtx =
                    new FailureContext(SYSTEM_WORKER_BLOCKED, new Throwable("Failure context error"));

            for (int i = 0; i < 3; i++)
                ignite.context().failure().process(failureCtx);

            assertEquals(3, testLog.filteredRecords().size());
        }
        finally {
            if (ignite != null)
                stopAllGrids();
        }
    }

    /**
     * Tests that thread dumps will be throttled and will be generated again after timeout exceeded.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, value = "true")
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE_THROTTLING_TIMEOUT, value = "1000")
    public void testThrottling() throws Exception {
        IgniteEx ignite = null;

        try {
            Predicate<String> dumpFilter = s -> s.contains(THREAD_DUMP_MSG);
            Predicate<String> throttledFilter = s -> s.contains("Thread dump is hidden");

            testLog = new TestLogger(log, Arrays.asList(dumpFilter, throttledFilter));

            ignite = (IgniteEx)startGrid();

            FailureContext failureCtx =
                    new FailureContext(SYSTEM_WORKER_BLOCKED, new Throwable("Failure context error"));

            for (int i = 0; i < 3; i++)
                ignite.context().failure().process(failureCtx);

            U.sleep(1000);

            for (int i = 0; i < 3; i++)
                ignite.context().failure().process(failureCtx);

            assertEquals(2, (int)testLog.filteredRecords().stream().filter(dumpFilter).count());
            assertEquals(4, (int)testLog.filteredRecords().stream().filter(throttledFilter).count());
        }
        finally {
            if (ignite != null)
                stopAllGrids();
        }
    }

    /**
     * Tests that thread dumps will be throttled per failure type and will be generated again after timeout exceeded.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, value = "true")
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE_THROTTLING_TIMEOUT, value = "1000")
    public void testThrottlingPerFailureType() throws Exception {
        IgniteEx ignite = null;

        try {
            Predicate<String> dumpFilter = s -> s.contains(THREAD_DUMP_MSG);
            Predicate<String> throttledFilter = s -> s.contains("Thread dump is hidden");

            testLog = new TestLogger(log, Arrays.asList(dumpFilter, throttledFilter));

            ignite = (IgniteEx)startGrid();

            FailureContext workerBlockedFailureCtx =
                    new FailureContext(SYSTEM_WORKER_BLOCKED, new Throwable("Failure context error"));

            FailureContext opTimeoutFailureCtx =
                    new FailureContext(SYSTEM_CRITICAL_OPERATION_TIMEOUT, new Throwable("Failure context error"));

            for (int i = 0; i < 3; i++) {
                ignite.context().failure().process(workerBlockedFailureCtx);

                ignite.context().failure().process(opTimeoutFailureCtx);
            }

            U.sleep(1000);

            for (int i = 0; i < 3; i++) {
                ignite.context().failure().process(workerBlockedFailureCtx);

                ignite.context().failure().process(opTimeoutFailureCtx);
            }

            assertEquals(4, (int)testLog.filteredRecords().stream().filter(dumpFilter).count());
            assertEquals(8, (int)testLog.filteredRecords().stream().filter(throttledFilter).count());
        }
        finally {
            if (ignite != null)
                stopAllGrids();
        }
    }

    /**
     * Tests that default thread dump trhottling timeout equals failure detection timeout.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, value = "true")
    public void testDefaultThrottlingTimeout() throws Exception {
        IgniteEx ignite = null;

        try {
            ignite = (IgniteEx)startGrid();

            assertEquals(ignite.context().failure().dumpThreadsTrottlingTimeout, ignite.configuration().getFailureDetectionTimeout().longValue());
        }
        finally {
            if (ignite != null)
                stopAllGrids();
        }
    }

    /**
     * Custom logger for checking if specified message and thread dump appeared at WARN and ERROR logging level.
     */
    private static class TestLogger extends ListeningTestLogger {
        /** Filtered log records. */
        private final List<String> filteredRecs = new ArrayList<>();

        /** Filters. */
        private final Collection<Predicate<String>> filters;

        /**
         * @param echo Echo.
         * @param filters Filters.
         */
        public TestLogger(@Nullable IgniteLogger echo, Collection<Predicate<String>> filters) {
            super(false, echo);

            this.filters = filters;
        }

        /**
         * @return Filtered records.
         */
        public List<String> filteredRecords() {
            return filteredRecs;
        }

        /** {@inheritDoc} */
        @Override public void warning(String msg, @Nullable Throwable t) {
            super.warning(msg, t);

            filter(msg);
        }

        /** {@inheritDoc} */
        @Override public void error(String msg, @Nullable Throwable t) {
            super.error(msg, t);

            filter(msg);
        }

        /** {@inheritDoc} */
        @Override public void info(String msg) {
            super.info(msg);

            filter(msg);
        }

        /**
         * Tests all log records with given filters.
         * @param msg Message.
         */
        private void filter(String msg) {
            for (Predicate<String> filter : filters) {
                if (filter.test(msg))
                    filteredRecs.add(msg);
            }
        }
    }
}
