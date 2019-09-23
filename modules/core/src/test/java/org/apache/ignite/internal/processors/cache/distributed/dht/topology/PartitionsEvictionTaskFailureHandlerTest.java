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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 *
 */
public class PartitionsEvictionTaskFailureHandlerTest extends GridCommonAbstractTest {
    /** Failure. */
    private AtomicBoolean failure = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));


        cfg.setFailureHandler(new AbstractFailureHandler() {
            /** {@inheritDoc} */
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                failure.set(true);

                // Do not invalidate a node context.
                return false;
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
        failure.set(false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    @Test
    public void testEvictionTaskShouldCallFailureHandler() throws Exception {
        IgniteEx node1 = startGrid(0);

        node1.cluster().baselineAutoAdjustEnabled(false);

        CountDownLatch latch = new CountDownLatch(1);

        subscribeEvictionQueueAtLatch(node1, latch);

        node1.cluster().active(true);

        IgniteCache<Object, Object> cache = node1.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setGroupName("test-grp"));

        for (int i = 0; i < 100_000; i++)
            cache.put(i, i);

        IgniteEx node2 = startGrid(1);

        awaitPartitionMapExchange();

        node1.cluster().setBaselineTopology(node2.cluster().topologyVersion());

        awaitEvictionQueueForFilling(node1, 20_000);

        latch.countDown();

        awaitEvictionQueueIsEmpty(node1, 200_000);

        assertTrue(GridTestUtils.waitForCondition(() -> failure.get(), 10_000));
    }

    /**
     * @param node Node.
     * @param ms Milliseconds.
     */
    private void awaitEvictionQueueIsEmpty(IgniteEx node, int ms) throws IgniteInterruptedCheckedException {
        Object evictionQueue = U.field(node.context().cache().context().evict(), "evictionQueue");

        assertTrue(GridTestUtils.waitForCondition(() -> {
            try {
                return (Integer)(U.invoke(null, evictionQueue, "size")) == 0;
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
                failure.set(true);

                return false;
            }
        }, ms));
    }

    /**
     * @param node Node.
     * @param ms Milliseconds.
     */
    private void awaitEvictionQueueForFilling(IgniteEx node, int ms) throws IgniteInterruptedCheckedException {
        Object evictionQueue = U.field(node.context().cache().context().evict(), "evictionQueue");

        assertTrue(GridTestUtils.waitForCondition(() -> {
            try {
                return (Integer)(U.invoke(null, evictionQueue, "size")) != 0;
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
                failure.set(true);

                return false;
            }
        }, ms));
    }

    /**
     * @param node Node.
     * @param latch Latch.
     */
    private void subscribeEvictionQueueAtLatch(IgniteEx node, CountDownLatch latch) {
        Object evictionQueue = U.field(node.context().cache().context().evict(), "evictionQueue");
        Queue[] buckets = U.field(evictionQueue, "buckets");

        for (int i = 0; i < buckets.length; i++)
            buckets[i] = new WaitingQueue(latch);
    }

    /**
     *
     */
    private class WaitingQueue extends LinkedBlockingQueue {
        /** Latch. */
        private final CountDownLatch latch;

        /**
         * @param latch Latch.
         */
        public WaitingQueue(CountDownLatch latch) {
            this.latch = latch;
        }

        /** {@inheritDoc} */
        @Override public Object poll() {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                //Noop
            }

            Object obj = super.poll();

            if(obj != null) {
                try {

                    Field field = U.findField(PartitionsEvictManager.PartitionEvictionTask.class, "finishFut");

                    field.setAccessible(true);

                    Field modifiersField = Field.class.getDeclaredField("modifiers");
                    modifiersField.setAccessible(true);
                    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

                    field.set(obj, new GridFutureAdapter<Object>() {
                        @Override
                        protected boolean onDone(@Nullable Object res, @Nullable Throwable err, boolean cancel) {
                            if (err == null)
                                throw new RuntimeException("TEST");

                            return super.onDone(res, err, cancel);
                        }
                    });
                }
                catch (Exception e) {
                    fail();
                }
            }

            return obj;
        }
    }
}