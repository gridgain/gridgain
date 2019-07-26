package org.apache.ignite.opencensus.spi.tracing;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.SpanId;
import io.opencensus.trace.TraceComponent;
import io.opencensus.trace.export.SpanData;
import io.opencensus.trace.export.SpanExporter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.tracing.TraceTags;
import org.apache.ignite.internal.processors.tracing.Traces;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class OpenCensusTracingSpiTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Shared trace component. */
    private static TraceComponent traceComponent;

    /** Exporter to check reported spans. */
    private TraceTestExporter exporter;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setTracingSpi(new OpenCensusTracingSpi(traceComponent));

        return cfg;
    }

    @BeforeClass
    public static void beforeTests() {
        /**
         * There is no ability to create several OpenCensus trace components within one JVM.
         * This hack with shared component resolvesa problem
         * with TracingSpi creation in case of multiple nodes in same JVM.
         */
        traceComponent = OpenCensusTracingSpi.createTraceComponent(OpenCensusTracingSpi.class.getClassLoader());
    }

    @Before
    public void before() throws Exception {
        exporter = new TraceTestExporter();

        exporter.start(traceComponent, "all");

        startGrids(GRID_CNT);
    }

    @After
    public void after() {
        stopAllGrids();

        traceComponent.getExportComponent().shutdown();
    }

    @Test
    public void testNodeJoinTracing() throws Exception {
        IgniteEx joinedNode = startGrid(GRID_CNT);

        awaitPartitionMapExchange();

        String joinedNodeId = joinedNode.localNode().id().toString();

        // Check Traces.Discovery.NODE_JOIN_REQUEST span existence:
        List<SpanData> nodeJoinReqSpans = exporter.handler.allSpans()
            .filter(span -> Traces.Discovery.NODE_JOIN_REQUEST.equals(span.getName()))
            .filter(span -> AttributeValue.stringAttributeValue(joinedNodeId).equals(
                span.getAttributes().getAttributeMap().get(TraceTags.tag(TraceTags.EVENT_NODE, TraceTags.ID))))
            .collect(Collectors.toList());

        Assert.assertEquals(
            Traces.Discovery.NODE_JOIN_REQUEST
                + " span is not found (or it's more than 1), spans=" + nodeJoinReqSpans,
            1,
            nodeJoinReqSpans.size()
        );

        // Check Traces.Discovery.NODE_JOIN_ADD spans on all nodes:
        for (int i = 0; i <= GRID_CNT; i++) {
            List<SpanData> nodeJoinAddSpans = exporter.handler.spansReportedByNode(getTestIgniteInstanceName(i))
                .filter(span -> Traces.Discovery.NODE_JOIN_ADD.equals(span.getName()))
                .filter(span -> AttributeValue.stringAttributeValue(joinedNodeId).equals(
                    span.getAttributes().getAttributeMap().get(TraceTags.tag(TraceTags.EVENT_NODE, TraceTags.ID))))
                .collect(Collectors.toList());

            Assert.assertEquals(
                String.format("%s span not found (or it's more than 1), spans=%s, nodeId=%d",
                    Traces.Discovery.NODE_JOIN_ADD, nodeJoinReqSpans, i),
                1,
                nodeJoinReqSpans.size()
            );

            nodeJoinAddSpans.forEach(spanData -> {
                SpanData parentSpan = exporter.handler.spanById(spanData.getParentSpanId());

                Assert.assertNotNull(
                    "Parent span doesn't exist for " + spanData,
                    parentSpan
                );
                Assert.assertEquals(
                    "Parent span name is invalid " + parentSpan,
                    Traces.Discovery.NODE_JOIN_REQUEST,
                    parentSpan.getName()
                );
                Assert.assertEquals(
                    "Parent span is not related to joined node " + parentSpan,
                    AttributeValue.stringAttributeValue(joinedNodeId),
                    parentSpan.getAttributes().getAttributeMap().get(TraceTags.tag(TraceTags.EVENT_NODE, TraceTags.ID))
                );
            });
        }

        // Check Traces.Discovery.NODE_JOIN_FINISH spans on all nodes:
        for (int i = 0; i <= GRID_CNT; i++) {
            List<SpanData> nodeJoinAddSpans = exporter.handler.spansReportedByNode(getTestIgniteInstanceName(i))
                .filter(span -> Traces.Discovery.NODE_JOIN_FINISH.equals(span.getName()))
                .filter(span -> AttributeValue.stringAttributeValue(joinedNodeId).equals(
                    span.getAttributes().getAttributeMap().get(TraceTags.tag(TraceTags.EVENT_NODE, TraceTags.ID))))
                .collect(Collectors.toList());

            Assert.assertEquals(
                String.format("%s span not found (or it's more than 1), spans=%s, nodeId=%d",
                    Traces.Discovery.NODE_JOIN_FINISH, nodeJoinReqSpans, i),
                1,
                nodeJoinReqSpans.size()
            );

            nodeJoinAddSpans.forEach(spanData -> {
                SpanData parentSpan = exporter.handler.spanById(spanData.getParentSpanId());

                Assert.assertNotNull(
                    "Parent span doesn't exist for " + spanData,
                    parentSpan
                );
                Assert.assertEquals(
                    "Parent span name is invalid " + parentSpan,
                    Traces.Discovery.NODE_JOIN_ADD,
                    parentSpan.getName()
                );
                Assert.assertEquals(
                    "Parent span is not related to joined node " + parentSpan,
                    AttributeValue.stringAttributeValue(joinedNodeId),
                    parentSpan.getAttributes().getAttributeMap().get(TraceTags.tag(TraceTags.EVENT_NODE, TraceTags.ID))
                );
            });
        }
    }

    @Test
    public void testNodeLeftTracing() throws Exception {
        IgniteEx leftNode = grid(GRID_CNT - 1);

        String leftNodeId = leftNode.localNode().id().toString();

        stopGrid(GRID_CNT - 1);

        awaitPartitionMapExchange();

        U.sleep(5000);

        // Check Traces.Discovery.NODE_LEFT span existence:
        List<SpanData> nodeLeftSpans = exporter.handler.allSpans()
            .filter(span -> Traces.Discovery.NODE_LEFT.equals(span.getName()))
            .filter(span -> AttributeValue.stringAttributeValue(leftNodeId).equals(
                span.getAttributes().getAttributeMap().get(TraceTags.tag(TraceTags.EVENT_NODE, TraceTags.ID))))
            .collect(Collectors.toList());

        int k = 2;
    }

    @Test
    public void testPartitionsMapExchangeTracing() throws Exception {

    }

    /**
     *
     */
    static class TraceTestExporter implements OpenCensusTraceExporter {
        private static final String HANDLER_NAME = "test";

        private final TraceExporterTestHandler handler = new TraceExporterTestHandler();

        @Override public void start(TraceComponent traceComponent, String igniteInstanceName) {
            traceComponent.getExportComponent().getSpanExporter().registerHandler(HANDLER_NAME, handler);
        }

        @Override public void stop(TraceComponent traceComponent) {
            traceComponent.getExportComponent().getSpanExporter().unregisterHandler(HANDLER_NAME);
        }
    }

    /**
     *
     */
    static class TraceExporterTestHandler extends SpanExporter.Handler {
        private final Map<SpanId, SpanData> collectedSpans = new ConcurrentHashMap<>();

        @Override public void export(Collection<SpanData> spanDataList) {
            for (SpanData data : spanDataList)
                collectedSpans.put(data.getContext().getSpanId(), data);
        }

        public Stream<SpanData> allSpans() {
            return collectedSpans.values().stream();
        }

        public SpanData spanById(SpanId id) {
            return collectedSpans.get(id);
        }

        public Stream<SpanData> spansReportedByNode(String igniteInstanceName) {
            return collectedSpans.values().stream()
                    .filter(spanData -> AttributeValue.stringAttributeValue(igniteInstanceName)
                        .equals(spanData.getAttributes().getAttributeMap().get("node.name")));
        }
    }
}
