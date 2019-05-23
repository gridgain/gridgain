package org.apache.ignite.internal.processors.tracing.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.tracing.Status;

public class StatusMatchTable {
    private static final Map<Status, io.opencensus.trace.Status> table = new ConcurrentHashMap<>();

    static {
        table.put(Status.OK, io.opencensus.trace.Status.OK);
        table.put(Status.CANCELLED, io.opencensus.trace.Status.CANCELLED);
        table.put(Status.ABORTED, io.opencensus.trace.Status.ABORTED);
    }

    private StatusMatchTable() {
        //
    }

    public static io.opencensus.trace.Status match(Status status) {
        io.opencensus.trace.Status result = table.get(status);

        if (result == null)
            throw new IgniteException("Unknown status (no matching with opencensus): " + status);

        return result;
    }
}
