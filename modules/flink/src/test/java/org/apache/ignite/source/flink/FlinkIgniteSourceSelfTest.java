/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.source.flink;

import java.util.UUID;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link IgniteSource}.
 */
public class FlinkIgniteSourceSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String TEST_CACHE = "testCache";

    /** Flink source context. */
    private SourceFunction.SourceContext<CacheEvent> ctx;

    /** Ignite instance. */
    private Ignite ignite;

    /** Cluster Group */
    private ClusterGroup clsGrp;

    /** Ignite Source instance */
    private IgniteSource igniteSrc;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Before
    public void setUpTest() throws Exception {
        ctx = mock(SourceFunction.SourceContext.class);
        ignite = mock(Ignite.class);
        clsGrp = mock(ClusterGroup.class);

        IgniteEvents igniteEvts = mock(IgniteEvents.class);
        IgniteCluster igniteCluster = mock(IgniteCluster.class);
        TaskRemoteFilter taskRemoteFilter = mock(TaskRemoteFilter.class);

        when(ctx.getCheckpointLock()).thenReturn(new Object());
        when(ignite.events(clsGrp)).thenReturn(igniteEvts);
        when(ignite.cluster()).thenReturn(igniteCluster);

        igniteSrc = new IgniteSource(TEST_CACHE);
        igniteSrc.setIgnite(ignite);
        igniteSrc.setEvtBatchSize(1);
        igniteSrc.setEvtBufTimeout(1);
        igniteSrc.setRuntimeContext(createRuntimeContext());

        IgniteBiPredicate locLsnr = igniteSrc.getLocLsnr();

        when(igniteEvts.remoteListen(locLsnr, taskRemoteFilter, EventType.EVT_CACHE_OBJECT_PUT ))
            .thenReturn(UUID.randomUUID());

        when(igniteCluster.forCacheNodes(TEST_CACHE)).thenReturn(clsGrp);
    }

    /**  */
    @After
    public void tearDownTest() {
        igniteSrc.cancel();
    }

    /** Creates streaming runtime context */
    private RuntimeContext createRuntimeContext() {
        StreamingRuntimeContext runtimeCtx = mock(StreamingRuntimeContext.class);

        when(runtimeCtx.isCheckpointingEnabled()).thenReturn(true);

        return runtimeCtx;
    }

    /**
     * Tests Ignite source start operation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIgniteSourceStart() throws Exception {
        igniteSrc.start(null, EventType.EVT_CACHE_OBJECT_PUT);

        verify(ignite.events(clsGrp), times(1));
    }

    /**
     * Tests Ignite source run operation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIgniteSourceRun() throws Exception {
        IgniteInternalFuture f = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    igniteSrc.start(null, EventType.EVT_CACHE_OBJECT_PUT);

                    igniteSrc.run(ctx);
                }
                catch (Throwable e) {
                    igniteSrc.cancel();

                   throw new AssertionError("Unexpected failure.", e);
                }
            }
        });

        long endTime = System.currentTimeMillis() + 2000;

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return f.isDone() || System.currentTimeMillis() > endTime;
            }
        }, 3000);

        igniteSrc.cancel();

        f.get(3000);
    }
}
