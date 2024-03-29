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

package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.CollisionExternalListener;
import org.apache.ignite.spi.collision.CollisionJobContext;
import org.apache.ignite.spi.collision.CollisionSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Cancelled jobs metrics self test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridCancelledJobsMetricsSelfTest extends GridCommonAbstractTest {
    /** */
    private static GridCancelCollisionSpi colSpi = new GridCancelCollisionSpi();

    /** */
    public GridCancelledJobsMetricsSelfTest() {
        super(true);
    }


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        cfg.setCollisionSpi(colSpi);

        DiscoverySpi discoSpi = cfg.getDiscoverySpi();

        assert discoSpi instanceof TcpDiscoverySpi;

        cfg.setMetricsUpdateFrequency(500);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCancelledJobs() throws Exception {
        final Ignite ignite = G.ignite(getTestIgniteInstanceName());

        // We change it because compute jobs will go to sleep.
        assertTrue(computeJobWorkerInterruptTimeout(ignite).propagate(10L));

        Collection<ComputeTaskFuture<?>> futs = new ArrayList<>();

        for (int i = 1; i <= 10; i++)
            futs.add(ignite.compute().executeAsync(CancelledTask.class, null));

        // Wait to be sure that metrics were updated.
        waitForCondition(() -> ignite.cluster().localNode().metrics().getTotalCancelledJobs() > 0, 5000);

        colSpi.externalCollision();

        for (ComputeTaskFuture<?> fut : futs) {
            try {
                fut.get(getTestTimeout());

                fail("Job was not interrupted.");
            }
            catch (IgniteException e) {
                if (e.hasCause(InterruptedException.class))
                    throw new IgniteCheckedException("Test run has been interrupted.", e);

                info("Caught expected exception: " + e.getMessage());
            }
        }

        // Job was cancelled and now we need to calculate metrics.
        assertThat(ignite.cluster().localNode().metrics().getTotalCancelledJobs(), equalTo(10));
    }

    /**
     *
     */
    private static final class CancelledTask extends ComputeTaskSplitAdapter<String, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, String arg) {
            return Arrays.asList(new GridCancelledJob());
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            assertFalse(results.get(0).isCancelled());

            return null;
        }
    }

    /**
     *
     */
    private static final class GridCancelledJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @Override public String execute() {
            X.println("Executing job.");

            try {
                Thread.sleep(Long.MAX_VALUE);
            }
            catch (InterruptedException ignored) {
                throw new IgniteException("Job got interrupted while waiting for cancellation.");
            }
            finally {
                X.println("Finished job.");
            }

            return null;
        }
    }

    /**
     *
     */
    @IgniteSpiMultipleInstancesSupport(true)
    private static class GridCancelCollisionSpi extends IgniteSpiAdapter
        implements CollisionSpi {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private CollisionExternalListener lsnr;

        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {
            Collection<CollisionJobContext> activeJobs = ctx.activeJobs();
            Collection<CollisionJobContext> waitJobs = ctx.waitingJobs();

            for (CollisionJobContext job : waitJobs)
                job.activate();

            for (CollisionJobContext job : activeJobs) {
                log.info("Cancelling job : " + job.getJob());

                job.cancel();
            }
        }

        /** {@inheritDoc} */
        @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
            // Start SPI start stopwatch.
            startStopwatch();

            // Ack start.
            if (log.isInfoEnabled())
                log.info(startInfo());
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // Ack stop.
            if (log.isInfoEnabled())
                log.info(stopInfo());
        }

        /** {@inheritDoc} */
        @Override public void setExternalCollisionListener(CollisionExternalListener lsnr) {
            this.lsnr = lsnr;
        }

        /** */
        public void externalCollision() {
            CollisionExternalListener tmp = lsnr;

            if (tmp != null)
                tmp.onExternalCollision();
        }
    }
}
