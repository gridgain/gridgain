package org.apache.ignite.internal.processors.tracing;

/**
 * List of tags that can be used to decorate spans.
 */
public class TraceTags {
    public static final String NODE_ID = "node.id";
    public static final String NODE_CONSISTENT_ID = "node.consistent.id";
    public static final String EVENT_NODE_ID = "event.node.id";

    private TraceTags() {};
}
