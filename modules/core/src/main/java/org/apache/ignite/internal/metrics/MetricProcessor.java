package org.apache.ignite.internal.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;

public class MetricProcessor extends GridProcessorAdapter {
    private final Map<String, MetricRegistry> metrics = new TreeMap<>();

    /**
     * @param ctx Kernal context.
     */
    public MetricProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        synchronized (metrics) {
            metrics.clear();
        }
    }

    public void register(String key, MetricRegistry m) {
        synchronized (metrics) {
            MetricRegistry old = metrics.putIfAbsent(key, m);

            //assert old == null : "Metrics key " + key + " already exists.";
        }
    }

    public void unregister(String key) {
        synchronized (metrics) {
            MetricRegistry old = metrics.remove(key);

            //assert old != null : "Metrics key " + key + " doesn't exist.";
        }
    }

    public Map<String, MetricRegistry> metrics() {
        synchronized (metrics) {
            Map<String, MetricRegistry> res = new TreeMap<>(metrics);

            return res;
        }
    }
}
