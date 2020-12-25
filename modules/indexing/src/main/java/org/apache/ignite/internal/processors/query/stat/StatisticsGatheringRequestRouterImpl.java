package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.messages.CancelStatisticsGatheringRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsClearRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsGatheringRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsGatheringResponse;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsGetRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsGetResponse;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsObjectData;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsPropagationMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StatisticsGatheringRequestRouterImpl implements StatisticsGatheringRequestRouter, GridLocalEventListener,
        GridMessageListener {
    /** Statistics related messages topic name. */
    private static final Object TOPIC = GridTopic.TOPIC_CACHE.topic("statistics");

    /** Logger. */
    private final IgniteLogger log;

    /** Local node id. */
    private final UUID locNodeId;

    /** Statistics manager. */
    private final IgniteStatisticsManagerImpl statMgr;

    /** Event manager. */
    private final GridEventStorageManager eventMgr;

    /** IO manager. */
    private final GridIoManager ioMgr;

    /** Message management pool */
    private final IgniteThreadPoolExecutor msgMgmtPool;

    /** Ignite statistics helper. */
    private final IgniteStatisticsHelper helper;

    /** Sended, but not received requests map reqId -> Request. */
    private final ConcurrentMap<UUID, StatisticsAddrRequest<StatisticsGatheringRequest>> remainingRequests =
            new ConcurrentHashMap<>();

    /** Gathering counter. */
    private final Counters gc = new Counters();

    public StatisticsGatheringRequestRouterImpl(
        UUID locNodeId,
        IgniteStatisticsManagerImpl statMgr,
        GridEventStorageManager eventMgr,
        GridIoManager ioMgr,
        IgniteStatisticsHelper helper,
        IgniteThreadPoolExecutor msgMgmtPool,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.log = logSupplier.apply(StatisticsGatheringRequestRouterImpl.class);
        this.locNodeId = locNodeId;
        this.statMgr = statMgr;
        this.eventMgr = eventMgr;
        this.ioMgr = ioMgr;
        this.helper = helper;
        this.msgMgmtPool = msgMgmtPool;

        eventMgr.addLocalEventListener(this, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);
        ioMgr.addMessageListener(TOPIC, this);
    }

    public void removeGathering(UUID gatId){
        gc.removeCounter(gatId);
        // TODO remove remaining
    }

    /**
     *
     * @param gatId
     * @param keys
     * @param failedPartitions
     * @return Map nodeId to addressed gathering request.
     * @throws IgniteCheckedException
     */
    /*private Map<UUID, StatisticsAddrRequest<StatisticsGatheringRequest>> generateCollectionRequestsMap(
        UUID gatId,
        Collection<StatisticsKeyMessage> keys,
        Map<StatisticsKeyMessage, int[]> failedPartitions
    ) throws IgniteCheckedException {
        return helper.generateCollectionRequests(gatId, keys, failedPartitions).stream().collect(
                Collectors.toMap(StatisticsAddrRequest::nodeId, r -> r));
    }*/

    private Map<UUID, StatisticsAddrRequest<StatisticsGatheringRequest>> toReqIdMap(
        Collection<StatisticsAddrRequest<StatisticsGatheringRequest>> reqs) {
        Map<UUID, StatisticsAddrRequest<StatisticsGatheringRequest>> res = new HashMap<>();
        reqs.forEach(r -> res.put(r.req().reqId(), r -> r));
        return res;
    }

    /**
     * Send gathering requests by specified keys and gathering id:
     * 1) Generate requests by keys and failed partitions.
     * 2) Put generated request into remaining map and increment gathering counter.
     * 2) "Send" or schedule local request execution (if exists) - can't fail to send local one.
     * 3) Send remove requests
     *
     *
     * @param gatId Gathering id.
     * @param keys Keys to generate and send requests by.
     * @param failedPartitions Collection of failed partitions to resend or
     *     {@code null} if it need to send request to all partitions.
     */
    private void sendGatheringRequests(
        UUID gatId,
        Collection<StatisticsKeyMessage> keys,
        Map<StatisticsKeyMessage, int[]> failedPartitions
    ) {
        Collection<StatisticsAddrRequest<StatisticsGatheringRequest>> reqs = null;
        int cnt = 0;
        Collection<StatisticsAddrRequest<StatisticsGatheringRequest>> failedMsgs;
        do {
            try {
                reqs = helper.generateCollectionRequests(gatId, keys, failedPartitions);
            }
            catch (IgniteCheckedException e) {
                // TODO: maybe shell call some special method in case of error.
                statMgr.cancelObjectStatisticsGathering(gatId);

                return;
            }

            Map<UUID, StatisticsAddrRequest<StatisticsGatheringRequest>> reqsMap = toReqIdMap(reqs);

            remainingRequests.putAll(reqsMap);
            gc.incrementCounter(gatId, reqs.size());

            // Process local request
            StatisticsAddrRequest<StatisticsGatheringRequest> locReq = reqs.stream().filter(
                    r -> locNodeId.equals(r.nodeId())).findAny().orElse(null);
            if (locReq != null) {
                statMgr.gatherLocalObjectStatisticsAsync(gatId, locReq.req().reqId(), locReq.req().keys());

                reqsMap.remove(locReq.req().reqId());
            }

            // Process remote requests
            failedMsgs = sendRequests(reqs);

            if (F.isEmpty(failedMsgs))
                failedPartitions = null;
            else {
                failedMsgs.forEach(r -> remainingRequests.remove(r.req().reqId()));
                //!
                gc.decrementCounter(gatId, failedMsgs.size());

                Set<UUID> failedIds = failedMsgs.stream().map(r -> r.req().reqId()).collect(Collectors.toSet());
                failedPartitions = helper.extractFailed(failedMsgs.stream().map(StatisticsAddrRequest::req)
                        .toArray(StatisticsGatheringRequest[]::new));
            }


            if (cnt++ > 10) {
                /*StatisticsKeyMessage key = status.keys().iterator().next();

                throw new IgniteCheckedException(String.format(
                        "Unable to send all messages to collect statistics by key %s.%s and the others %d",
                        key.schema(), key.obj(), status.keys().size() - 1));*/
                // TODO: cancel gathering
            }
        }
        while (failedPartitions != null);
    }

    @Override public void sendGatheringRequestsAsync(UUID gatId, Collection<StatisticsKeyMessage> keys) {
        msgMgmtPool.submit(() -> sendGatheringRequests(gatId, keys));
    }

    /**
     * Send requests to target nodes (except of local one).
     *
     * @param reqs Collection of addressed requests to send.
     * @return Collection of addressed requests that has errors while sending or {@code null} if all requests was send
     * successfully.
     */
    private <T extends Message> Collection<StatisticsAddrRequest<T>> sendRequests(
        Collection<StatisticsAddrRequest<T>> reqs
    ) {
        Collection<StatisticsAddrRequest<T>> res = null;

        for (StatisticsAddrRequest<T> req : reqs) {
            if (locNodeId.equals(req.nodeId()))
                continue;

            try {
                ioMgr.sendToCustomTopic(req.nodeId(), TOPIC, req.req(), GridIoPolicy.QUERY_POOL);
            }
            catch (IgniteCheckedException e) {
                if (res == null)
                    res = new ArrayList<>();

                res.add(req);
            }
        }

        return res;
    }


    public void replyGatheringRequest(UUID reqId, Collection<ObjectStatisticsImpl> data) {
        StatisticsAddrRequest<StatisticsGatheringRequest> request = remainingRequests.get(reqId);
        if (locNodeId.equals(reqId.node())) {
            receiveLocalStatistics();
        }
    }

    public void replyGatheringRequestAsync(UUID reqId, Collection<ObjectStatisticsImpl> data) {
        msgMgmtPool.submit(() -> replyGatheringRequest(reqId, data));
    }


    /** {@inheritDoc} */
    @Override public void onEvent(Event evt) {
        assert evt.type() == EventType.EVT_NODE_FAILED || evt.type() == EventType.EVT_NODE_LEFT;

        final UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

        // TODO: implement me
        //ctx.closure().runLocalSafe(() -> onNodeLeft(nodeId), GridIoPolicy.QUERY_POOL);
    }
    /**
     * Receive and handle statistics propagation message as response for collection request.
     *
     * @param nodeId Sender node id.
     * @param msg Statistics propagation message with partitions statistics to handle.
     * @throws IgniteCheckedException In case of errors.
     */
    private void receiveLocalStatistics(UUID nodeId, StatisticsGatheringResponse msg) {
        assert msg.data().keySet().stream().noneMatch(pd -> pd.type() == StatisticsType.PARTITION)
                : "Got partition statistics by request " + msg.reqId();

        StatisticsAddrRequest<StatisticsGatheringRequest> request = remainingRequests.get(msg.reqId());

        assert request.nodeId().equals(nodeId);

        if (request == null) {
            if (log.isDebugEnabled())
                log.debug(String.format(
                        "Ignoring outdated local statistics collection response from node %s to col %s req %s",
                        nodeId, msg.gatId(), msg.reqId()));

            return;
        }

        Map<StatisticsKeyMessage, int[]> failedParts = IgniteStatisticsHelper.extractFailed(request.req(), msg);
        statMgr.registerLocalResult(msg.gatId(), msg.data().keySet());
        if (gc.decrementCounter(msg.gatId(), 1))
            finishGatheringAsync(msg.gatId());
    }

    private void finishGatheringAsync(UUID gatId) {
        msgMgmtPool.submit(() -> statMgr.finishStatisticsCollection(gatId));
    }

    /**
     * Receive and handle statistics propagation message with partitions statistics.
     *
     * @param nodeId Sender node id.
     * @param msg Statistics propagation message with partitions statistics to handle.
     */
    private void receivePartitionsStatisticsAsync(UUID nodeId, StatisticsPropagationMessage msg) {
        msgMgmtPool.submit(() -> statMgr.receivePartitionsStatistics(msg.data()));
    }

    /**
     * Send statistics by request.
     *
     * @param nodeId Node to send statistics to.
     * @param msg Statistics request to process.
     * @throws IgniteCheckedException In case of errors.
     */
    private void supplyStatistics(UUID nodeId, StatisticsGetRequest msg) {

        List<StatisticsObjectData> data = new ArrayList<>(msg.keys().size());
        for (StatisticsKeyMessage keyMsg : msg.keys()) {
            ObjectStatisticsImpl objStats = statMgr.getGlobalStatistics(keyMsg);
            if (objStats != null)
                try {
                    data.add(StatisticsUtils.toObjectData(keyMsg, StatisticsType.GLOBAL, objStats));
                } catch (IgniteCheckedException e) {
                    if (log.isDebugEnabled())
                        log.debug(String.format("Unable to build object statistics message by key %s due to %s",
                                keyMsg, e.getMessage()));
                    // Send statistics without current key.
                }
        }

        safeSend(nodeId, new StatisticsGetResponse(msg.reqId(), data));


    }

    /**
     * Send some message until node left the topology or message successfully sended.
     *
     * @param nodeId Target node id.
     * @param msg Message to send.
     */
    private void safeSend(UUID nodeId, Message msg) {
        while (true) {
            try {
                ioMgr.sendToCustomTopic(nodeId, TOPIC, msg, GridIoPolicy.QUERY_POOL);
            } catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug(String.format("Failed to send statistics message to node %d due to %s, retrying",
                            nodeId, e.getMessage()));
            }

            return;
        }
    }

    /**
     * Schedule statistics gathering by specified request.
     *
     * @param nodeId request origin node.
     * @param msg request message.
     */
    private void handleGatheringRequest(UUID nodeId, StatisticsGatheringRequest msg) {
        assert msg.reqId() != null : "Got statistics collection request without request id";

        statMgr.gatherLocalObjectStatisticsAsync(nodeId, msg.gatId(), msg.reqId(), msg.keys().keySet());
        helper.addActiveCollectionStatus(msg.reqId(), new StatisticsGatheringContext(, null));

        statMgmtPool.submit(() -> processLocal(nodeId, msg));
    }

    /**
     * Handle statistics clear request.
     *
     * @param nodeId UUID of request sender node.
     * @param msg Clear request message.
     */
    private void clearObjectStatistics(UUID nodeId, StatisticsClearRequest msg) {
        statMgr.clearObjectStatisticsLocal(msg.keys());
    }


    /** {@inheritDoc} */
    @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
        try {
            if (msg instanceof StatisticsPropagationMessage)
                receivePartitionsStatisticsAsync(nodeId, (StatisticsPropagationMessage) msg);
            else if (msg instanceof StatisticsGatheringResponse)
                receiveLocalStatistics(nodeId, (StatisticsGatheringResponse) msg);
            else if (msg instanceof StatisticsGetRequest)
                msgMgmtPool.submit(() -> supplyStatistics(nodeId, (StatisticsGetRequest) msg));
            else if (msg instanceof StatisticsGatheringRequest)
                handleGatheringRequest(nodeId, (StatisticsGatheringRequest)msg);
            else if (msg instanceof CancelStatisticsGatheringRequest)
                cancelStatisticsCollection(nodeId, (CancelStatisticsGatheringRequest) msg);
            else if (msg instanceof StatisticsClearRequest)
                msgMgmtPool.submit(() -> clearObjectStatistics(nodeId, (StatisticsClearRequest)msg));
            else
                log.warning("Unknown msg " + msg +  " in statistics topic " + TOPIC + " from node " + nodeId);
        } catch (IgniteCheckedException e) {
            log.warning("Statistic msg from node " + nodeId + " processing failed", e);
        }
    }

    /**
     * Cancel local statistics collection task.
     *
     * @param nodeId Sender node id.
     * @param msg Cancel request.
     */
    private void cancelStatisticsCollection(UUID nodeId, CancelStatisticsGatheringRequest msg) {
        Arrays.stream(msg.reqIds()).forEach(reqId -> {
            StatisticsAddrRequest<StatisticsGatheringRequest> removed = remainingRequests.remove(reqId);

            assert removed == null || (removed.nodeId().equals(nodeId) && removed.req().gatId().equals(msg.gatId()));
        });
        gc.removeCounter(msg.gatId());
        statMgr.cancelLocalStatisticsGathering(msg.gatId());
    }

    /**
     * Thread safe counters by ids.
     */
    private static class Counters {

        /** Id to counter map. */
        private final ConcurrentMap<UUID, Integer> counters = new ConcurrentHashMap<>();

        /**
         * Increment counter.
         *
         * @param id Counter id.
         * @param increment Increment value.
         */
        public void incrementCounter(UUID id, int increment) {
            counters.compute(id, (k, v) -> (v == null) ? increment : v + increment);
        }

        /**
         * Remove specified counter.
         *
         * @param id Counter id.
         * @return Last counter value or {@code null} if there are no counter with specified id.
         */
        public Integer removeCounter(UUID id) {
            return counters.remove(id);
        }

        /**
         * Subtract decrement from counter and return {@code true} if counter became zero.
         *
         * @param id Counter id.
         * @param decrement Decrement.
         * @return {@code true} if counter became zero, {@code false} otherwise.
         */
        public boolean decrementCounter(UUID id, int decrement) {
            boolean[] res = new boolean[1];
            counters.compute(id, (k, v) -> {
                if (v == null) {
                    //TODO: log
                    res[0] = true;
                    return null;
                }
                v -= decrement;
                res[0] = v == 0;
                if (v == 0)
                    return v;
                return null;
            });
            return res[0];
        }

        /**
         * Get counter current value.
         *
         * @param id Counter id.
         * @return Current value of counter or {@code null} if there are no such counter
         */
        public Integer getCounter(UUID id) {
            return counters.get(id);
        }
    }

    /** {@inheritDoc} */
    @Override public void sendPartitionStatisticsToBackupNodesAsync(
            GridH2Table tbl,
            Collection<ObjectPartitionStatisticsImpl> objStats
    ) {
        msgMgmtPool.submit(() -> sendPartitionStatisticsToBackupNodes(tbl, objStats));
    }

    /**
     * Send statistics propagation messages with partition statistics to all backups node.
     *
     * @param tbl Table to which statistics should be send.
     * @param objStats Collection of partition statistics to send.
     */
    private void sendPartitionStatisticsToBackupNodes(
            GridH2Table tbl,
            Collection<ObjectPartitionStatisticsImpl> objStats
    ) {
        StatisticsKeyMessage keyMsg = new StatisticsKeyMessage(tbl.identifier().schema(), tbl.identifier().table(), null);
        GridDhtPartitionTopology topology = tbl.cacheContext().topology();
        Map<UUID, List<StatisticsObjectData>> statsByNodes = new HashMap<>();
        for (ObjectPartitionStatisticsImpl stat : objStats) {
            StatisticsObjectData statData;
            try {
                statData = StatisticsUtils.toObjectData(keyMsg, StatisticsType.PARTITION, stat);
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug(String.format("Unable to convert statistics by obj %s to message due to %s",
                        keyMsg, e.getMessage()));
                continue;
            }

            for (ClusterNode partNode : topology.nodes(stat.partId(), topology.readyTopologyVersion())) {
                if (locNodeId.equals(partNode.id()))
                    continue;

                statsByNodes.compute(partNode.id(), (k, v) -> {
                    if (v == null)
                        v = new ArrayList<>();

                    v.add(statData);

                    return v;
                });
            }
        }

        for (Map.Entry <UUID, List<StatisticsObjectData>> statToNode : statsByNodes.entrySet()) {
            StatisticsPropagationMessage nodeMsg = new StatisticsPropagationMessage(statToNode.getValue());
            safeSend(statToNode.getKey(), nodeMsg);
        }
    }

    /**
     * Handle node left event:
     * 1) Cancel all collection tasks which expect specified node statistics result.
     * 2) Cancel collection task requested by node left.
     *
     * @param nodeId leaved node id.
     */
    private void onNodeLeft(UUID nodeId) {
        Map<UUID, Collection<StatisticsGatheringRequest>> failedGatherings = new HashMap<>();
        remainingRequests.forEach((k,v) -> {
            if (!nodeId.equals(v.nodeId()))
                return;

            StatisticsAddrRequest<StatisticsGatheringRequest> failedRequest = remainingRequests.remove(k);

            if (failedRequest == null)
                return;

            failedGatherings.compute(failedRequest.req().gatId(), (k, v) -> {
                if (v == null)
                    v = new ArrayList<>();

                v.add(failedRequest.req());

                return v;
            });
        });

        for (Map.Entry<UUID, Collection<StatisticsGatheringRequest>> failedGathering : failedGatherings.entrySet()) {
            Map<StatisticsKeyMessage, int[]> failedPartitions = IgniteStatisticsHelper.extractFailed(
                    failedGathering.getValue().toArray(new StatisticsGatheringRequest[0]));

            gc.decrementCounter(failedGathering.getKey(), failedGathering.getValue().size());
            // TODO reschedule sending... add request counter BEFORE actually send it.

        }
    }
}
