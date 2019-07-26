package org.apache.ignite.internal.processors.tracing;

/**
 * List of trace names used in appropriate sub-systems.
 */
public class Traces {
    public static class Discovery {
        private Discovery() {}

        public static final String NODE_JOIN_REQUEST = "discovery.node.join.request";
        public static final String NODE_JOIN_ADD = "discovery.node.join.add";
        public static final String NODE_JOIN_FINISH = "discovery.node.join.finish";
        public static final String NODE_FAILED = "discovery.node.failed";
        public static final String NODE_LEFT = "discovery.node.left";
        public static final String CUSTOM_EVENT = "discovery.custom.event";
    }

    private Traces() {}
}
