/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * Tests related to how priority queue collision SPI handles high concurrency cases.
 */
public class ComputePriorityQueueSpiConcurrencyTest extends GridCommonAbstractTest {
    private static final int GRID_CNT = 1;

    private static final int PARALLEL_JOBS_COUNT = 100;

    private static final String JOB_PRIORITY_ATTRIBUTE = "grid.job.priority";

    private static final String TASK_PRIORITY_ATTRIBUTE = "grid.task.priority";

    private static final PriorityQueueCollisionSpiEx spi = new PriorityQueueCollisionSpiEx();

    /**
     * {@inheritDoc}
     */
    @Override
    protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);

        spi.setParallelJobsNumber(PARALLEL_JOBS_COUNT);
        spi.setPriorityAttributeKey(TASK_PRIORITY_ATTRIBUTE);
        spi.setJobPriorityAttributeKey(JOB_PRIORITY_ATTRIBUTE);
        spi.setDefaultPriority(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCollisionSpi(spi);
    }

    @Test
    public void testSubmitManyJobs() {
        Ignite ignite = grid(0);

        int taskCount = 200_000;

        long started = System.currentTimeMillis();
        long timeout = 20_000L;

        for (int i = 0; i < taskCount; i++) {
            ignite.compute().executeAsync(new RandomPriorityTask(), "");
            assertThat(System.currentTimeMillis() - started, lessThan(timeout));
        }

        assertThat(spi.collisionCounter.get(), greaterThan(0L));
    }

    @Override protected long getTestTimeout() {
        return 30_000L;
    }

    @ComputeTaskSessionFullSupport
    public static class RandomPriorityTask extends ComputeTaskSplitAdapter<Object, Object> {
        @TaskSessionResource
        private ComputeTaskSession taskSes = null;

        @Override protected Collection<ComputeJob> split(int gridSize, Object arg) {
            taskSes.setAttribute(TASK_PRIORITY_ATTRIBUTE, ThreadLocalRandom.current().nextInt(10));

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

    private static class PriorityQueueCollisionSpiEx extends PriorityQueueCollisionSpi {
        private AtomicLong collisionCounter = new AtomicLong();

        @Override
        public void onCollision(CollisionContext ctx) {
            collisionCounter.incrementAndGet();
        }
    }
}
