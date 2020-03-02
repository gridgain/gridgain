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
package org.apache.ignite.p2p;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.managers.deployment.GridDeploymentManager;
import org.apache.ignite.internal.managers.deployment.GridDeploymentMetadata;
import org.apache.ignite.internal.managers.deployment.GridDeploymentStore;
import org.apache.ignite.internal.managers.deployment.GridTestDeployment;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.internal.processors.cache.IgnitePeerToPeerClassLoadingException;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequest;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 *
 */
public class ClassLoaderClusterFailTest extends GridCommonAbstractTest implements Serializable {
    /** */
    private static final String PREDICATE_NAME = "org.apache.ignite.tests.p2p.P2PTestPredicate";

    /** */
    private final IgniteUuid incorrectClsLdrId = IgniteUuid.fromUuid(UUID.randomUUID());

    /** */
    private boolean spoilMessageOnSend = false;

    /** */
    private boolean spoilDeploymentBeforePrepare = false;

    /** */
    private static AtomicReference<Throwable> exceptionThrown = new AtomicReference<>(null);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg
            .setClientMode(igniteInstanceName.startsWith("client"))
            .setPeerClassLoadingEnabled(true)
            .setDeploymentMode(DeploymentMode.SHARED)
            .setCommunicationSpi(new TestCommunicationSpi())
            .setMarshaller(new OptimizedMarshaller());

        return cfg;
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
    private void doTest(
        boolean spoilDeploymentBeforePrepare,
        boolean spoilMessageOnSend,
        Class<? extends Throwable> exceptionExpected
    ) throws Exception {
        this.spoilDeploymentBeforePrepare = spoilDeploymentBeforePrepare;
        this.spoilMessageOnSend = spoilMessageOnSend;
        this.exceptionThrown.set(null);

        IgniteEx crd = (IgniteEx)startGridsMultiThreaded(1);

        IgniteCache<Integer, Integer> cache = crd.getOrCreateCache("cache");

        IgniteEx client1 = (IgniteEx)startGrid("client1");
        IgniteEx client2 = (IgniteEx)startGrid("client2");

        awaitPartitionMapExchange();

        GridDeploymentManager deploymentManager = client2.context().deploy();

        GridDeploymentStore store = GridTestUtils.getFieldValue(deploymentManager, "locStore");

        GridTestUtils.setFieldValue(deploymentManager, "locStore", new GridDeploymentTestStore(store));

        client1.compute(client1.cluster().forRemotes()).execute(new P2PDeploymentLongRunningTask(), null);

        assertTrue(
            "Wrong exception: " + exceptionThrown.get(),
            exceptionThrown.get() == null && exceptionExpected == null
                || X.hasCause(exceptionThrown.get(), exceptionExpected)
        );

        doSleep(4000);
    }

    /** */
    @Test
    public void testDontSpoil() throws Exception {
        doTest(false, false, null);
    }

    /** */
    @Test
    public void testSpoilBeforePrepare() throws Exception {
        doTest(true, false, IgnitePeerToPeerClassLoadingException.class);
    }

    /** */
    @Test
    public void testSpoilOnSend() throws Exception {
        doTest(false, true, IgnitePeerToPeerClassLoadingException.class);
    }

    /** */
    @Test
    public void testSpoilBoth() throws Exception {
        doTest(true, true, IgnitePeerToPeerClassLoadingException.class);
    }

    /**
     *
     */
    private class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            if (spoilMessageOnSend && msg instanceof GridIoMessage) {
                GridIoMessage ioMsg = (GridIoMessage)msg;

                Message m = ioMsg.message();

                if (m instanceof GridCacheQueryRequest) {
                    GridCacheQueryRequest queryRequest = (GridCacheQueryRequest)m;

                    if (queryRequest.deployInfo() != null) {
                        queryRequest.prepare(
                            new GridDeploymentInfoBean(
                                IgniteUuid.fromUuid(UUID.randomUUID()),
                                queryRequest.deployInfo().userVersion(),
                                queryRequest.deployInfo().deployMode(),
                                null
                            )
                        );
                    }
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }

    /**
     *
     */
    private class GridDeploymentTestStore implements GridDeploymentStore {
        /** */
        private final GridDeploymentStore store;

        /** */
        public GridDeploymentTestStore(GridDeploymentStore store) {
            this.store = store;
        }

        /** {@inheritDoc} */
        @Override public void start() throws IgniteCheckedException {
            store.start();
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            store.stop();
        }

        /** {@inheritDoc} */
        @Override public void onKernalStart() throws IgniteCheckedException {
            store.onKernalStart();
        }

        /** {@inheritDoc} */
        @Override public void onKernalStop() {
            store.onKernalStop();
        }

        /** {@inheritDoc} */
        @Override public @Nullable GridDeployment getDeployment(GridDeploymentMetadata meta) {
            return store.getDeployment(meta);
        }

        /** {@inheritDoc} */
        @Override public @Nullable GridDeployment searchDeploymentCache(GridDeploymentMetadata meta) {
            return store.searchDeploymentCache(meta);
        }

        /** {@inheritDoc} */
        @Override public @Nullable GridDeployment getDeployment(IgniteUuid ldrId) {
            return store.getDeployment(ldrId);
        }

        /** {@inheritDoc} */
        @Override public Collection<GridDeployment> getDeployments() {
            return store.getDeployments();
        }

        /** {@inheritDoc} */
        @Override public GridDeployment explicitDeploy(Class<?> cls, ClassLoader clsLdr) throws IgniteCheckedException {
            if (spoilDeploymentBeforePrepare)
                return new GridTestDeployment(DeploymentMode.SHARED, this.getClass().getClassLoader(), incorrectClsLdrId, "0", PREDICATE_NAME, false);
            else
                return store.explicitDeploy(cls, clsLdr);
        }

        /** {@inheritDoc} */
        @Override public void explicitUndeploy(@Nullable UUID nodeId, String rsrcName) {
            store.explicitUndeploy(nodeId, rsrcName);
        }

        /** {@inheritDoc} */
        @Override
        public void addParticipants(Map<UUID, IgniteUuid> allParticipants, Map<UUID, IgniteUuid> addedParticipants) {
            store.addParticipants(allParticipants, addedParticipants);
        }
    }

    /**
     *
     */
    public class P2PDeploymentLongRunningTask extends ComputeTaskSplitAdapter<Object, Object> implements Serializable {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws IgniteException {
            IntFunction<ComputeJobAdapter> f = i -> new ComputeJobAdapter() {
                /** */
                @IgniteInstanceResource
                private transient IgniteEx ignite;

                /** {@inheritDoc} */
                @Override public Object execute() {
                    try {
                        Thread.sleep(3000);

                        if (ignite.configuration().getIgniteInstanceName().equals("client2")) {
                            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache("cache");

                            Class<IgniteBiPredicate> cls = (Class<IgniteBiPredicate>)getExternalClassLoader().loadClass(PREDICATE_NAME);

                            cache.query(new ScanQuery<IgniteBiPredicate, Integer>(cls.newInstance())).getAll();
                        }
                    }
                    catch (Throwable e) {
                        exceptionThrown.set(e);
                    }

                    return null;
                }
            };

            return IntStream.range(0, gridSize).mapToObj(f).collect(Collectors.toList());
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }
}
