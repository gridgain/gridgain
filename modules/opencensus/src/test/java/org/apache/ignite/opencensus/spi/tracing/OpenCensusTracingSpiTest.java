package org.apache.ignite.opencensus.spi.tracing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setTracingSpi(new OpenCensusTracingSpi(traceComponent).withExporter(new TraceTestExporter()));

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
        startGrids(GRID_CNT);
    }

    @After
    public void after() {
        stopAllGrids();
    }

    @Test
    public void testNodeJoinTracing() throws Exception {
        IgniteEx joiningNode = startGrid(GRID_CNT);

        int k = 2;
    }

    @Test
    public void testPartitionsMapExchangeTracing() {

    }

    private List<SpanData> spansReportedByNode(int nodeId) {
        OpenCensusTracingSpi spi = (OpenCensusTracingSpi) grid(nodeId).configuration().getTracingSpi();
        TraceTestExporter exporter = (TraceTestExporter) spi.getExporter();
        return exporter.handler.collectedSpans;
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
    }
}
