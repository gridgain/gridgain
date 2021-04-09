/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.p2p;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;

/**
 *
 */
public class ClassLoadingProblemExtendedLoggingTest extends GridCommonAbstractTest {
    /** */
    private ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPeerClassLoadingEnabled(true)
            .setDeploymentMode(SHARED)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setGridLogger(listeningLog)
            .setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            ));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void test() throws Exception {
        IgniteEx ignite = startGrid();

        IgniteEx client = startClientGrid(1);

        client.compute(client.cluster().forRemotes()).execute(new RemoteTask(), null);

        TestRecordingCommunicationSpi clientSpi = spi(client);

        clientSpi.closure((node, msg) -> {});
    }

    /** */
    @Test
    public void test1() throws Exception {
        IgniteEx ignite = startGrids(3);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, 32)));

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        stopGrid(2);

        ignite.cluster().setBaselineTopology(asList(new ClusterNode[] {grid(0).cluster().localNode(), grid(1).cluster().localNode()}));

        startGrid(2);

        awaitPartitionMapExchange();

        doSleep(60_000);

    }
    /**
     *
     */
    public static class RemoteTask extends ComputeTaskSplitAdapter {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws IgniteException {
            return range(0, gridSize).mapToObj(i -> new RemoteJob(arg)).collect(toList());
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List list) throws IgniteException {
            return null;
        }
    }

    /**
     *
     */
    public static class RemoteJob extends ComputeJobAdapter {
        /** */
        private final Object arg;

        /** */
        public RemoteJob(Object arg) {
            this.arg = arg;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            return null;
        }
    }
}
