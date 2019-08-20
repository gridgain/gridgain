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

package org.gridgain.service;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;
import com.google.common.collect.Lists;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.processors.metric.export.MetricRequest;
import org.apache.ignite.internal.processors.metric.export.MetricResponse;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.gridgain.agent.StompDestinationsUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.apache.ignite.internal.GridTopic.TOPIC_METRICS;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Metric service test.
 */
public class MetricsServiceTest extends AbstractServiceTest {
    /** Io manager. */
    private GridIoManager io = mock(GridIoManager.class);

    /**
     * Should send metrics by web socket from coordinator.
     */
    @Test
    public void onNodeMetrics() {
        MetricsService srvc = new MetricsService(getMockContext(), mgr);

        MetricResponse metricRes = getMetricResponse();
        srvc.onNodeMetrics(UUID.randomUUID(), metricRes, SYSTEM_POOL);

        ArgumentCaptor<String> destCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(mgr, times(1)).send(destCaptor.capture(), payloadCaptor.capture());

        Assert.assertEquals(StompDestinationsUtils.buildMetricsDest(metricRes.clusterId()), destCaptor.getValue());
        Assert.assertArrayEquals(new byte[]{1, 2, 3, 4}, payloadCaptor.getValue());
    }

    /**
     * Should send metric request in metric topic.
     */
    @Test
    public void broadcastPullMetrics() throws IgniteCheckedException {
        MetricsService srvc = new MetricsService(getMockContext(), getMockWebSocketManager());

        srvc.broadcastPullMetrics();

        ArgumentCaptor<Collection> nodesCaptor = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<GridTopic> topicCaptor = ArgumentCaptor.forClass(GridTopic.class);
        ArgumentCaptor<Message> msgCaptor = ArgumentCaptor.forClass(Message.class);
        ArgumentCaptor<Byte> plcCaptor = ArgumentCaptor.forClass(Byte.class);
        verify(io, times(1)).sendToGridTopic(nodesCaptor.capture(), topicCaptor.capture(), msgCaptor.capture(), plcCaptor.capture());

        Assert.assertEquals(1, nodesCaptor.getValue().size());
        Assert.assertEquals(TOPIC_METRICS, topicCaptor.getValue());
        Assert.assertTrue(msgCaptor.getValue() instanceof MetricRequest);
        Assert.assertEquals(Integer.valueOf(-1), ((MetricRequest) msgCaptor.getValue()).schemaVersion());
        Assert.assertEquals(Byte.valueOf(SYSTEM_POOL), plcCaptor.getValue());
    }

    /** {@inheritDoc} */
    @Override protected GridKernalContext getMockContext() {
        GridKernalContext ctx = super.getMockContext();
        when(ctx.io()).thenReturn(io);

        IgniteClusterEx cluster = ctx.grid().cluster();
        ClusterGroup grp = mock(ClusterGroup.class);
        when(cluster.forServers()).thenReturn(grp);
        when(grp.nodes()).thenReturn(Lists.newArrayList(new TcpDiscoveryNode()));

        return ctx;
    }

    /**
     * @return Metric response.
     */
    private MetricResponse getMetricResponse() {
        MessageReader reader = mock(MessageReader.class);
        when(reader.readByteArray(anyString())).thenReturn(new byte[]{1, 2, 3, 4});
        when(reader.beforeMessageRead()).thenReturn(true);

        MetricResponse metricRes = new MetricResponse();
        metricRes.readFrom(ByteBuffer.wrap(ByteBuffer.allocate(128).array()), reader);

        return metricRes;
    }
}
