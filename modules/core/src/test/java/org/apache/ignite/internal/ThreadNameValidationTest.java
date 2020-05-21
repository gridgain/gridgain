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

package org.apache.ignite.internal;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

/**
 * Check threads for default names in single and thread pool instances.
 */
public class ThreadNameValidationTest extends GridCacheAbstractSelfTest {

    /** {@link Executors.DefaultThreadFactory} count before test. */
    private static transient int defaultThreadFactoryCountBeforeTest;

    /** {@link Thread#threadInitNumber} count before test. */
    private static transient int anonymousThreadCountBeforeTest;


    /** */
    private static final TestRule beforeAllTestRule = (base, description) -> new Statement() {
        @Override public void evaluate() throws Throwable {
            defaultThreadFactoryCountBeforeTest = getDefaultPoolCount();
            base.evaluate();
        }
    };

    /** Manages before first test execution. */
    @ClassRule public static transient RuleChain firstLastTestRule
        = RuleChain.outerRule(beforeAllTestRule).around(GridAbstractTest.firstLastTestRule);

    /** */
    private static final String FIELD = "user-name";

    /** */
    private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();


    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        anonymousThreadCountBeforeTest = getAnonymousThreadCount();
        super.beforeTest();
    }


    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        assertEquals("Executors.DefaultThreadFactory usage detected, IgniteThreadPoolExecutor is preferred",
            defaultThreadFactoryCountBeforeTest, getDefaultPoolCount());
        assertEquals("Thread without specific name detected",
            anonymousThreadCountBeforeTest, getAnonymousThreadCount());
        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(igniteInstanceName);

        ccfg.setStoreKeepBinary(true);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testThreadsWithDefaultNames() throws Exception {
        validateThreadNames();

        IgniteCache<Integer, BinaryObject> cache = grid(0).cache(DEFAULT_CACHE_NAME).withKeepBinary();

        final int ENTRY_CNT = 10;

        for (int i = 0; i < ENTRY_CNT; i++)
            cache.put(i, userObject("user-" + i));

        validateThreadNames();

        cache.removeAll();

        validateThreadNames();
    }

    /**
     * @param userName User name.
     * @return Binary object.
     */
    private BinaryObject userObject(String userName) {
        return grid(0).binary().builder("orders").setField(FIELD, userName).build();
    }

    /**
     * Validates current existed thread names.
     */
    private void validateThreadNames() {
        Arrays.stream(threadMXBean.dumpAllThreads(false, false))
            .filter(t -> t.getThreadName().startsWith("Thread-")).forEach(threadInfo -> {
            StringBuilder sb = new StringBuilder();
            sb.append("Thread with default name detected. StackTrace: ");
            for (StackTraceElement element : threadInfo.getStackTrace()) {
                sb.append(System.lineSeparator())
                    .append(element.toString());
            }
            assertTrue(sb.toString(), false);
        });
    }

    /**
     * Gets pools count with {@link Executors.DefaultThreadFactory}.
     * @return count
     */
    private static int getDefaultPoolCount() throws ReflectiveOperationException {
        Class<?> defaultThreadFacktory = Class.forName("java.util.concurrent.Executors$DefaultThreadFactory");
        Field poolNumber = defaultThreadFacktory.getDeclaredField("poolNumber");
        poolNumber.setAccessible(true);
        AtomicInteger counter = (AtomicInteger)poolNumber.get(null);
        return counter.get();
    }

    /**
     * Gets anonymous threads count since JVM start.
     * @return count
     */
    private static int getAnonymousThreadCount() throws ReflectiveOperationException {
        Field threadInitNumberField = Thread.class.getDeclaredField("threadInitNumber");
        threadInitNumberField.setAccessible(true);
        return threadInitNumberField.getInt(null);
    }
}