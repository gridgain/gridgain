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

package org.apache.ignite.spi.metric.jmx;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 *
 */
public class JmxMetricExporterSpiTest extends GridCommonAbstractTest {
    /**
     *
     */
    @Test
    public void testConcurrentRegistration() throws IgniteCheckedException {
        JmxMetricExporterSpi spi = new JmxMetricExporterSpi();

        new IgniteTestResources(new DummyMBeanServer()).inject(spi);

        TestMetricsManager testMgr = new TestMetricsManager();

        spi.setMetricRegistry(testMgr);

        spi.spiStart("testInstance");

        testMgr.runRegistersConcurrent();
        testMgr.runUnregisters();
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    private static class TestMetricsManager implements ReadOnlyMetricRegistry {
        /** */
        private final List<Consumer<MetricRegistry>> creation = new ArrayList<>();

        /** */
        private final List<Consumer<MetricRegistry>> rmv = new ArrayList<>();

        /** {@inheritDoc} */
        @Override public void addMetricRegistryCreationListener(Consumer<MetricRegistry> lsnr) {
            creation.add(lsnr);
        }

        /** {@inheritDoc} */
        @Override public void addMetricRegistryRemoveListener(Consumer<MetricRegistry> lsnr) {
            rmv.add(lsnr);
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<MetricRegistry> iterator() {
            return EmptyIterator.INSTANCE;
        }

        /**
         *
         */
        public void runRegistersConcurrent() {
            final AtomicInteger cntr = new AtomicInteger();

            GridTestUtils.runMultiThreadedAsync(() -> {
                for (int i = 0; i < 20; i++) {
                    for (Consumer<MetricRegistry> lsnr : creation)
                        lsnr.accept(new MetricRegistry("type", "stub-" + cntr.getAndIncrement(), log));
                }
            }, Runtime.getRuntime().availableProcessors() * 2, "runner-");

        }

        /**
         *
         */
        public void runUnregisters() {
            for (int i = 0; i < Runtime.getRuntime().availableProcessors() * 2 * 20; i++) {
                for (Consumer<MetricRegistry> lsnr : creation)
                    lsnr.accept(new MetricRegistry("type", "stub-" + i, log));
            }
        }
    }
}
