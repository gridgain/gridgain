package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
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
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implementation of Statistics gathering request crawler with addition event and message listeners handling.
 */
public class StatisticsGatheringRequestCrawlerImpl implements StatisticsGatheringRequestCrawler, GridLocalEventListener,
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

    /** Remaining requests map reqId -> Request. Contains incoming requests too. */
    private final ConcurrentMap<UUID, StatisticsAddrRequest<StatisticsGatheringRequest>> remainingRequests =
            new ConcurrentHashMap<>();


    /**
     * Constructor.
     *
     * @param locNodeId Local node id.
     * @param statMgr Statistics manager.
     * @param eventMgr Event storage manager.
     * @param ioMgr Io manager.
     * @param helper Statistics helper.
     * @param msgMgmtPool Message processing thread pool.
     * @param logSupplier Log supplier.
     */
    public StatisticsGatheringRequestCrawlerImpl(
        UUID locNodeId,
        IgniteStatisticsManagerImpl statMgr,
        GridEventStorageManager eventMgr,
        GridIoManager ioMgr,
        IgniteStatisticsHelper helper,
        IgniteThreadPoolExecutor msgMgmtPool,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.log = logSupplier.apply(StatisticsGatheringRequestCrawlerImpl.class);
        this.locNodeId = locNodeId;
        this.statMgr = statMgr;
        this.eventMgr = eventMgr;
        this.ioMgr = ioMgr;
        this.helper = helper;
        this.msgMgmtPool = msgMgmtPool;

        eventMgr.addLocalEventListener(this, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);
        ioMgr.addMessageListener(TOPIC, this);
    }

    /**
     * Stop request crawler manager.
     */
    public void stop() {
        if (msgMgmtPool != null) {
            List<Runnable> unfinishedTasks = msgMgmtPool.shutdownNow();
            if (!unfinishedTasks.isEmpty())
                log.warning(String.format("%d statistics collection request cancelled.", unfinishedTasks.size()));
        }
    }

    /**
     * Convert collection of addressed gathering request to map reqId to addressed request.
     *
     * @param reqs Collection of request to convert.
     * @return Map request id to request.
     */
    private Map<UUID, StatisticsAddrRequest<StatisticsGatheringRequest>> toReqIdMap(
        Collection<StatisticsAddrRequest<StatisticsGatheringRequest>> reqs
    ) {
        Map<UUID, StatisticsAddrRequest<StatisticsGatheringRequest>> res = new HashMap<>();

        reqs.forEach(r -> res.put(r.req().reqId(), r));

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
                reqs = helper.generateCollectionRequests(gatId, locNodeId, keys, failedPartitions);
            }
            catch (IgniteCheckedException e) {
                statMgr.cancelObjectStatisticsGathering(gatId);

                return;
            }

            Map<UUID, StatisticsAddrRequest<StatisticsGatheringRequest>> reqsMap = toReqIdMap(reqs);

            remainingRequests.putAll(reqsMap);

            // Process local request
            StatisticsAddrRequest<StatisticsGatheringRequest> locReq = reqs.stream().filter(
                    r -> locNodeId.equals(r.targetNodeId())).findAny().orElse(null);
            if (locReq != null)
                statMgr.gatherLocalObjectStatisticsAsync(gatId, locReq.req().reqId(), locReq.req().keys());

            // Process remote requests
            failedMsgs = sendRequests(reqs);

            if (F.isEmpty(failedMsgs))
                failedPartitions = null;
            else {
                failedMsgs.forEach(r -> remainingRequests.remove(r.req().reqId()));

                failedPartitions = helper.extractFailed(failedMsgs.stream().map(StatisticsAddrRequest::req)
                        .toArray(StatisticsGatheringRequest[]::new));
            }


            if (cnt++ > 10) {
                if (log.isInfoEnabled())
                    log.info(String.format("Unable to send gathering requests for 10 times, cancel gathering %s", gatId));

                statMgr.cancelObjectStatisticsGathering(gatId);
            }
        }
        while (!F.isEmpty(failedPartitions));
    }

    /** {@inheritDoc} */
    @Override public void sendGatheringRequestsAsync(
        UUID gatId,
        Collection<StatisticsKeyMessage> keys,
        Map<StatisticsKeyMessage, int[]> failedParts
    ) {
        msgMgmtPool.submit(() -> sendGatheringRequests(gatId, keys, failedParts));
    }

    /**
     * Send response to given request.
     *
     * @param reqId Request id to response to.
     * @param statistics Collected statistics.
     */
    private void sendGatheringResponse(
        UUID reqId,
        Map<IgniteBiTuple<StatisticsKeyMessage, ObjectStatisticsImpl>, int[]> statistics
    ) {
        StatisticsAddrRequest<StatisticsGatheringRequest> req =  remainingRequests.remove(reqId);
        if (req == null) {
            if (log.isDebugEnabled())
                log.debug(String.format("Dropping results to cancelled collection request %s", reqId));

            return;
        }
        UUID gatId = req.req().gatId();

        Map<StatisticsObjectData, int[]> dataParts = new HashMap<>(statistics.size());
        statistics.forEach((k,v) -> {
            try {
                StatisticsObjectData data = StatisticsUtils.toObjectData(k.getKey(), StatisticsType.LOCAL, k.getValue());

                dataParts.put(data, v);
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug(String.format("Unable to format statistics %s.%s by request=%s gathering=%s",
                            k.getKey().schema(), k.getKey().obj(), reqId, gatId));

            }
        });


        if (locNodeId.equals(req.senderNodeId()))
            statMgr.registerLocalResult(gatId, dataParts);
        else {
            int parts = req.req().keys().values().stream().mapToInt(ar -> ar.length).sum();
            statMgr.onRemoteGatheringSend(gatId, parts);
            StatisticsGatheringResponse resp = new StatisticsGatheringResponse(req.req().gatId(), reqId, dataParts);
            safeSend(req.senderNodeId(), resp);
        }
    }

    /** {@inheritDoc} */
    @Override public void sendGatheringResponseAsync(
        UUID reqId,
        Map<IgniteBiTuple<StatisticsKeyMessage, ObjectStatisticsImpl>, int[]> statistics
    ) {
        msgMgmtPool.submit(() -> sendGatheringResponse(reqId, statistics));
    }

    public void sendCancelGathering(UUID gatId) {
        // TODO: local node
        Map<UUID, Collection<UUID>> nodeReqs = new HashMap<>();
        remainingRequests.forEach((k, v) -> {
            if (!gatId.equals(v.req().gatId()))
                return;

            StatisticsAddrRequest<StatisticsGatheringRequest> reqToCancel = remainingRequests.remove(k);

            if (reqToCancel == null)
                return;
            nodeReqs.compute(reqToCancel.targetNodeId(), (k, v) -> {
                if (v == null)
                    v = new ArrayList<>();

                v.add(reqToCancel.req().reqId());

                return v;
            });
        });

        Collection<StatisticsAddrRequest<CancelStatisticsGatheringRequest>> cancelReqs = nodeReqs.entrySet().stream()
            .map(e -> new StatisticsAddrRequest(new CancelStatisticsGatheringRequest(gatId,
                e.getValue().toArray(new UUID[0])), locNodeId, e.getKey())).collect(Collectors.toList());
        Collection<StatisticsAddrRequest<CancelStatisticsGatheringRequest>> failedReqs = sendRequests(cancelReqs);
        if (!F.isEmpty(failedReqs))
            if (log.isDebugEnabled())
                log.debug(String.format("Unable to send %d cancel gathering requests.", failedReqs.size()));
    }

    /** {@inheritDoc} */
    @Override public void sendCancelGatheringAsync(UUID gatId) {
        msgMgmtPool.submit(() -> sendCancelGathering(gatId));
    }

    /**
     * Send clear statistics requests for all nodes by the given keys.
     *
     * @param keys Keys to clear statistics by.
     */
    public void sendClearStatistics(Collection<StatisticsKeyMessage> keys) {
        // TODO: local node

        try {
            Collection<StatisticsAddrRequest<StatisticsClearRequest>> msgs = helper
                .generateClearRequests(locNodeId, keys);
            Collection<StatisticsAddrRequest<StatisticsClearRequest>> failedMsgs = sendRequests(msgs);

            if (!F.isEmpty(failedMsgs))
                if (log.isDebugEnabled())
                    log.debug(String.format("Unable to send %d clear statistics request", failedMsgs.size()));
        } catch (IgniteCheckedException e) {
            if (log.isDebugEnabled())
                log.debug(String.format("Unable to send clear statistics request by keys %s", keys));
        }
    }

    /** {@inheritDoc} */
    @Override public void sendClearStatisticsAsync(Collection<StatisticsKeyMessage> keys) {
        msgMgmtPool.submit(() -> sendClearStatistics(keys));
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
            if (locNodeId.equals(req.targetNodeId()))
                continue;

            try {
                ioMgr.sendToCustomTopic(req.targetNodeId(), TOPIC, req.req(), GridIoPolicy.QUERY_POOL);
            }
            catch (IgniteCheckedException e) {
                if (res == null)
                    res = new ArrayList<>();

                res.add(req);
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void onEvent(Event evt) {
        assert evt.type() == EventType.EVT_NODE_FAILED || evt.type() == EventType.EVT_NODE_LEFT;

        final UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

        // TODO: implement me
        //ctx.closure().runLocalSafe(() -> onNodeLeft(nodeId), GridIoPolicy.QUERY_POOL);
    }

    /**
     * Process StatisticsGetResponse message.
     *
     * @param nodeId Sender node id.
     * @param msg Response to process.
     */
    private void receiveGlobalStatistics(UUID nodeId, StatisticsGetResponse msg) {
        statMgr.saveGlobalStatistics(msg.data());
    }


    /**
     * Receive and handle statistics gathering response message as response for collection request.
     *
     * @param nodeId Sender node id.
     * @param msg Statistics propagation message with partitions statistics to handle.
     * @throws IgniteCheckedException In case of errors.
     */
    private void receiveLocalStatistics(UUID nodeId, StatisticsGatheringResponse msg) {
        assert msg.data().keySet().stream().noneMatch(pd -> pd.type() == StatisticsType.PARTITION)
                : "Got partition statistics by request " + msg.reqId();

        StatisticsAddrRequest<StatisticsGatheringRequest> req = remainingRequests.remove(msg.reqId());

        if (req == null) {
            if (log.isDebugEnabled())
                log.debug(String.format(
                        "Ignoring outdated local statistics collection response from node %s to col %s req %s",
                        nodeId, msg.gatId(), msg.reqId()));

            return;
        }

        assert req.targetNodeId().equals(nodeId);

        statMgr.registerLocalResult(msg.gatId(), msg.data());

        Map<StatisticsKeyMessage, int[]> failedParts = IgniteStatisticsHelper.extractFailed(req.req(), msg);

        if (!F.isEmpty(failedParts))
            sendGatheringRequestsAsync(req.req().gatId(), req.req().keys().keySet(), failedParts);
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
     * @param nodeId Request origin node.
     * @param msg Request message.
     */
    private void handleGatheringRequest(UUID nodeId, StatisticsGatheringRequest msg) {
        remainingRequests.put(msg.reqId(), new StatisticsAddrRequest<>(msg, nodeId, locNodeId));

        statMgr.gatherLocalObjectStatisticsAsync(msg.gatId(), msg.reqId(), msg.keys());
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
        if (msg instanceof StatisticsPropagationMessage)
            receivePartitionsStatisticsAsync(nodeId, (StatisticsPropagationMessage) msg);
        else if (msg instanceof StatisticsGatheringResponse)
            receiveLocalStatistics(nodeId, (StatisticsGatheringResponse) msg);
        else if (msg instanceof StatisticsGetRequest)
            msgMgmtPool.submit(() -> supplyStatistics(nodeId, (StatisticsGetRequest) msg));
        else if (msg instanceof StatisticsGetResponse)
            receiveGlobalStatistics(nodeId, (StatisticsGetResponse)msg);
        else if (msg instanceof StatisticsGatheringRequest)
            handleGatheringRequest(nodeId, (StatisticsGatheringRequest)msg);
        else if (msg instanceof CancelStatisticsGatheringRequest)
            cancelStatisticsCollection(nodeId, (CancelStatisticsGatheringRequest) msg);
        else if (msg instanceof StatisticsClearRequest)
            msgMgmtPool.submit(() -> clearObjectStatistics(nodeId, (StatisticsClearRequest)msg));
        else
            log.warning("Unknown msg " + msg +  " in statistics topic " + TOPIC + " from node " + nodeId);
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

            assert removed == null || (removed.targetNodeId().equals(nodeId) && removed.req().gatId().equals(msg.gatId()));
        });
        statMgr.cancelLocalStatisticsGathering(msg.gatId());
    }

    /** {@inheritDoc} */
    @Override public void sendPartitionStatisticsToBackupNodesAsync(
            StatisticsKeyMessage key,
            Collection<ObjectPartitionStatisticsImpl> objStats
    ) {
        msgMgmtPool.submit(() -> sendPartitionStatisticsToBackupNodes(key, objStats));
    }

    /**
     * Send statistics propagation messages with global statistics.
     *
     * @param globalStats Global statistics to send.
     */
    public void sendGlobalStat(Map<StatisticsKeyMessage, ObjectStatisticsImpl> globalStats) {
        try {
            Collection<StatisticsAddrRequest<StatisticsPropagationMessage>> msgs =
                    helper.generateGlobalPropagationMessages(locNodeId, globalStats);

            Collection<StatisticsAddrRequest<StatisticsPropagationMessage>> failedMsgs =  sendRequests(msgs);

            if (!F.isEmpty(failedMsgs))
                if (log.isDebugEnabled())
                    log.debug(String.format("Failed to send %d global statistics messages.", failedMsgs.size()));
        }
        catch (IgniteCheckedException e) {
            if (log.isDebugEnabled())
                log.debug(String.format("Unable to send global statistics caused by %s", e.getMessage()));
        }
    }

    /** {@inheritDoc} */
    @Override public void sendGlobalStatAsync(Map<StatisticsKeyMessage, ObjectStatisticsImpl> globalStats) {
        msgMgmtPool.submit(() -> sendGlobalStat(globalStats));
    }

    /**
     * Send statistics propagation messages with partition statistics to all backups node.
     * Can send same statistics to some nodes twice in case of topology changes.
     *
     * @param key Statistics key.
     * @param objStats Collection of partition statistics to send.
     */
    private void sendPartitionStatisticsToBackupNodes(
            StatisticsKeyMessage key,
            Collection<ObjectPartitionStatisticsImpl> objStats
    ) {
        Collection<StatisticsAddrRequest<StatisticsPropagationMessage>> failedMsgs = null;
        do {
            Collection<StatisticsAddrRequest<StatisticsPropagationMessage>> reqs;
            try {
                reqs = helper.generatePropagationMessages(locNodeId, key, objStats);
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug(String.format("Unable to send statistics to backups by object %s.%s", key.schema(),
                        key.obj()));

                return;
            }
            failedMsgs = sendRequests(reqs);
        } while (!F.isEmpty(failedMsgs));
    }

    /**
     * Handle node left event:
     * 1) Cancel all collection tasks which expect specified node statistics result.
     * 2) Cancel collection task requested by node left.
     *
     * @param nodeId leaved node id.
     */
    private void onNodeLeft(UUID nodeId) {/
        Map<UUID, Collection<StatisticsGatheringRequest>> failedGatherings = new HashMap<>();
        remainingRequests.forEach((k,v) -> {
            if (!nodeId.equals(v.targetNodeId()))
                return;

            StatisticsAddrRequest<StatisticsGatheringRequest> failedRequest = remainingRequests.remove(k);

            if (failedRequest == null)
                return;

            failedGatherings.compute(failedRequest.req().gatId(), (k1, v1) -> {
                if (v1 == null)
                    v1 = new ArrayList<>();

                v1.add(failedRequest.req());

                return v1;
            });
        });

        for (Map.Entry<UUID, Collection<StatisticsGatheringRequest>> failedGathering : failedGatherings.entrySet()) {
            Map<StatisticsKeyMessage, int[]> failedPartitions = IgniteStatisticsHelper.extractFailed(
                    failedGathering.getValue().toArray(new StatisticsGatheringRequest[0]));

            sendGatheringRequests(failedGathering.getKey(),  failedPartitions.keySet(), failedPartitions);
        }
    }
}
