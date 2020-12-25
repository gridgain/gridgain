package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.plugin.extensions.communication.Message;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Router to track and handle any requests, related to statistics gathering process.
 * Router know about requests types and call back statistics manager to process failed requests.
 */
public interface StatisticsGatheringRequestRouter {
    public void removeGathering(UUID gatId);

    /**
     * Just add tracking of statistics gathering process, i.e. allow to require .
     * @param gatId
     */
    public void addGathering(UUID gatId);

    /**
     * Send specified requests.
     *
     * @param getId Gathering id.
     * @param keys Keys to collect statistics by.
     */
    public void sendGatheringRequestsAsync(UUID gatId, Collection<StatisticsKeyMessage> keys);

    /**
     *
     * @param tbl
     * @param objStats
     */
    public void sendPartitionStatisticsToBackupNodesAsync(GridH2Table tbl, Collection<ObjectPartitionStatisticsImpl> objStats);

}
