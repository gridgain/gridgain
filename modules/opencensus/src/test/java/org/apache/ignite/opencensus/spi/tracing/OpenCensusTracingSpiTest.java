package org.apache.ignite.opencensus.spi.tracing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.TraceComponent;
import io.opencensus.trace.export.SpanData;
import io.opencensus.trace.export.SpanExporter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class OpenCensusTracingSpiTest extends GridCommonAbstractTest {
    private static final int GRID_CNT = 3;

    /** Shared trace component. */
    private static TraceComponent traceComponent;

    private TraceTestExporter exporter;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setTracingSpi(new OpenCensusTracingSpi(traceComponent));

        return cfg;
    }

    @BeforeClass
    public static void beforeTests() {
        // There is no ability to create several opencensus trace components within one JVM.
        // This hack with shared component resolves the problem.
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

        String joinedNodeId = joinedNode.localNode().id().toString();

        // Check node.join.request on coordinator:

        // Check node.join traces:
        for (int i = 0; i <= GRID_CNT; i++) {
            List<SpanData> nodeJoinTraces = exporter.handler.spansReportedByNode(getTestIgniteInstanceName(i))
                .filter(span -> "node.join".equals(span.getName()))
                .collect(Collectors.toList());


        }

        int k = 2;
    }

    @Test
    public void testPartitionsMapExchangeTracing() {

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
        private final List<SpanData> collectedSpans = Collections.synchronizedList(new ArrayList<>());

        @Override public void export(Collection<SpanData> spanDataList) {
            for (SpanData data : spanDataList)
                collectedSpans.add(data);
        }

        public Stream<SpanData> spansReportedByNode(String igniteInstanceName) {
            return collectedSpans.stream()
                    .filter(spanData -> AttributeValue.stringAttributeValue(igniteInstanceName)
                        .equals(spanData.getAttributes().getAttributeMap().get("node.name")));
        }
    }
}
