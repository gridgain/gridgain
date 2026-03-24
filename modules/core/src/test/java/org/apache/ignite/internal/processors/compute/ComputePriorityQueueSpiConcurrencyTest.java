/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.compute;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.CompletableFuture.allOf;

/**
 * Tests related to how priority queue collision SPI handles high concurrency cases.
 */
public class ComputePriorityQueueSpiConcurrencyTest extends GridCommonAbstractTest {
    private static final int GRID_CNT = 1;

    private final PriorityQueueCollisionSpi spi = new PriorityQueueCollisionSpi();

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);

        spi.setParallelJobsNumber(20);
        spi.setPriorityAttributeKey("grid.task.priority");
        spi.setJobPriorityAttributeKey("grid.job.priority");
        spi.setDefaultPriority(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setCollisionSpi(spi)
            .setMetricsUpdateFrequency(Long.MAX_VALUE)
            .setClientFailureDetectionTimeout(Long.MAX_VALUE);
    }

    @Test
    public void testSubmitManyJobs() {
        Ignite ignite = grid(0);

        // When we submit new task we wait for collision SPI to start it if there are available slots.
        // Tasks are short-lived, it happens often, so many threads are used to submit enough tasks fast.
        ExecutorService executor = Executors.newFixedThreadPool(100);

        try {
            int taskCount = 100000;

            CompletableFuture<?>[] futures = new CompletableFuture<?>[taskCount];
            for (int i = 0; i < taskCount; i++) {
                CompletableFuture<Object> future = new CompletableFuture<>();

                futures[i] = future;

                executor.submit(() -> {
                    ignite.compute().executeAsync(new RandomPriorityTask(), "");
                    future.complete(null);
                });
            }

            allOf(futures).join();
        } finally {
            executor.shutdown();
        }
    }

    @Override protected long getTestTimeout() {
        return 30_000L;
    }

    @ComputeTaskSessionFullSupport
    public static class RandomPriorityTask extends ComputeTaskSplitAdapter<Object, Object> {
        @TaskSessionResource
        private ComputeTaskSession taskSes = null;

        @Override protected Collection<ComputeJob> split(int gridSize, Object arg) {
            taskSes.setAttribute("grid.task.priority", new Random().nextInt(10));

            List<ComputeJob> jobs = new ArrayList<>(gridSize);

            jobs.add(new ComputeJobAdapter() {
                @Override public Object execute() throws IgniteException {
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    return null;
                }
            });

            return jobs;
        }

        @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }
}