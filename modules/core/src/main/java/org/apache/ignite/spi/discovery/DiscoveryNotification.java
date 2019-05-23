package org.apache.ignite.spi.discovery;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.tracing.messages.Trace;
import org.jetbrains.annotations.Nullable;

public class DiscoveryNotification {
    private final int eventType;
    private final long topVer;
    private final ClusterNode node;
    private final Collection<ClusterNode> topSnapshot;

    private @Nullable Map<Long, Collection<ClusterNode>> topHist;
    private @Nullable DiscoverySpiCustomMessage customMsgData;
    private Trace trace;

    public DiscoveryNotification(int eventType, long topVer, ClusterNode node, Collection<ClusterNode> topSnapshot) {
        this.eventType = eventType;
        this.topVer = topVer;
        this.node = node;
        this.topSnapshot = topSnapshot;
    }

    public DiscoveryNotification(
        int eventType,
        long topVer,
        ClusterNode node,
        Collection<ClusterNode> topSnapshot,
        @Nullable Map<Long, Collection<ClusterNode>> topHist,
        @Nullable DiscoverySpiCustomMessage customMsgData,
        Trace trace
    ) {
        this.eventType = eventType;
        this.topVer = topVer;
        this.node = node;
        this.topSnapshot = topSnapshot;
        this.topHist = topHist;
        this.customMsgData = customMsgData;
        this.trace = trace;
    }

    public int type() {
        return eventType;
    }

    public long getTopVer() {
        return topVer;
    }

    public ClusterNode getNode() {
        return node;
    }

    public Collection<ClusterNode> getTopSnapshot() {
        return topSnapshot;
    }

    public Map<Long, Collection<ClusterNode>> getTopHist() {
        return topHist;
    }

    public void setTopHist(Map<Long, Collection<ClusterNode>> topHist) {
        this.topHist = topHist;
    }

    public DiscoverySpiCustomMessage getCustomMsgData() {
        return customMsgData;
    }

    public void setCustomMsgData(DiscoverySpiCustomMessage customMsgData) {
        this.customMsgData = customMsgData;
    }

    public Trace getTrace() {
        return trace;
    }

    public void setTrace(Trace trace) {
        this.trace = trace;
    }
}
