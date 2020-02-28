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
import java.util.Objects;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequest;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

public class ClassLoaderClusterFailTest extends GridCommonAbstractTest implements Serializable {
    private static final String TASK_NAME = "org.apache.ignite.tests.p2p.P2PDeploymentLongRunningTask";
    private static final String PREDICATE_NAME = "org.apache.ignite.tests.p2p.P2PTestPredicate";

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(igniteInstanceName.startsWith("client"));

        cfg
            .setPeerClassLoadingEnabled(true)
            .setDeploymentMode(DeploymentMode.SHARED)
            .setDeploymentSpi(new TestDeploymentSpi())
            .setCommunicationSpi(new TestCommunicationSpi())
            .setMarshaller(new OptimizedMarshaller())
            .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true));

        return cfg;
    }
    @Test
    public void test() throws Exception {
        Ignite crd = startGridsMultiThreaded(2);

        IgniteCache<Integer, Integer> cache = crd.getOrCreateCache("cache");

        //cache.query(new SqlFieldsQuery("create table t(id integer primary key, f integer"));

        for (int i = 0; i < 1; i++)
            //cache.query(new SqlFieldsQuery("insert into t (id, f) values (" + i + ", " + i + ")"));
            cache.put(i, i);

        IgniteEx client1 = (IgniteEx)startGrid("client1");
        Ignite client2 = startGrid("client2");

        awaitPartitionMapExchange();

        //Class<ComputeTask> cls = (Class<ComputeTask>)getExternalClassLoader().loadClass(TASK_NAME);

        client1.compute(client1.cluster().forRemotes()).execute(new P2PDeploymentLongRunningTask(), null);

        stopGrid("client1");

        startGrid("client1");

        doSleep(4000);

        //stopGrid("client2");

        doSleep(10000);
    }

    private class TestDeploymentSpi extends LocalDeploymentSpi {
        @Override public boolean register(ClassLoader ldr, Class rsrc) throws IgniteSpiException {
            log.info("!!!! " + rsrc.getSimpleName());

            return super.register(ldr, rsrc);
        }
    }

    private class TestCommunicationSpi extends TcpCommunicationSpi {
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
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

    public class P2PDeploymentLongRunningTask extends ComputeTaskSplitAdapter<Object, Object> implements Serializable {
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws IgniteException {
            IntFunction<ComputeJobAdapter> f = i -> new ComputeJobAdapter() {
                @IgniteInstanceResource
                private transient IgniteEx ignite;

                @Override public Object execute() {
                    X.println("!!! Executing P2PDeploymentLongRunningTask job on " + ignite.configuration().getIgniteInstanceName());

                    try {
                        Thread.sleep(3000);

                        //if (ignite.configuration().getIgniteInstanceName().equals("client2")) {
                            X.println("!!! Executing P2PDeploymentLongRunningTask on client2 after waiting.");

                            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache("cache");

                            Class<IgniteBiPredicate> cls = (Class<IgniteBiPredicate>)getExternalClassLoader().loadClass(PREDICATE_NAME);

                            QueryCursor cursor = cache.query(new ScanQuery<IgniteBiPredicate, Integer>(cls.newInstance()));

                            List a = cursor.getAll();

                        /*for (ClusterNode node : ignite.cluster().forServers().nodes()) {
                            ignite.context().cache().context().io().sendOrderedMessage(
                                node,
                                new Object(),
                                new P2PTestMessage(),
                                QUERY_POOL,
                                Long.MAX_VALUE
                            );
                        }*/

                            X.println("!!! P2P scan query sent.");
                        //}
                    }
                    catch (Exception e) {
                        X.println("!!! Exception:" + e.getMessage());
                        e.printStackTrace();
                    }

                    return null;
                }
            };

            return IntStream.range(0, gridSize).mapToObj(f).collect(Collectors.toList());
        }

        @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }

    public static class Cls implements Serializable {
        String A;
        String B;
        String C;

        public Cls(String a, String b, String c) {
            A = a;
            B = b;
            C = c;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Cls cls = (Cls)o;
            return Objects.equals(A, cls.A) &&
                Objects.equals(B, cls.B) &&
                Objects.equals(C, cls.C);
        }

        @Override public int hashCode() {
            return Objects.hash(A, B, C);
        }
    }
}
