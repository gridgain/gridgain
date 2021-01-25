/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
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
import org.apache.ignite.internal.util.GridArrays;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import static org.apache.ignite.internal.GridTopic.TOPIC_STATISTICS;

/**
 * Implementation of Statistics gathering request crawler with addition event and message listeners handling.
 */
public class StatisticsGatheringRequestCrawlerImpl implements StatisticsGatheringRequestCrawler, GridLocalEventListener,
        GridMessageListener {
    /** Maximum number of attempts to send statistics gathering requests. */
    public static final int MAX_SEND_RETRIES = 10;

    /** Logger. */
    private final IgniteLogger log;

    /** Local node id. */
    private final UUID locNodeId;

    /** Statistics manager. */
    private final IgniteStatisticsManagerImpl statMgr;

    /** Event manager. */
    private final GridEventStorageManager evtMgr;

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
     * @param evtMgr Event storage manager.
     * @param ioMgr Io manager.
     * @param helper Statistics helper.
     * @param msgMgmtPool Message processing thread pool.
     * @param logSupplier Log supplier.
     */
    public StatisticsGatheringRequestCrawlerImpl(
        UUID locNodeId,
        IgniteStatisticsManagerImpl statMgr,
        GridEventStorageManager evtMgr,
        GridIoManager ioMgr,
        IgniteStatisticsHelper helper,
        IgniteThreadPoolExecutor msgMgmtPool,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.log = logSupplier.apply(StatisticsGatheringRequestCrawlerImpl.class);
        this.locNodeId = locNodeId;
        this.statMgr = statMgr;
        this.evtMgr = evtMgr;
        this.ioMgr = ioMgr;
        this.helper = helper;
        this.msgMgmtPool = msgMgmtPool;

        evtMgr.addLocalEventListener(this, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);
        ioMgr.addMessageListener(TOPIC_STATISTICS, this);
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
        evtMgr.removeLocalEventListener(this, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);
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
        Collection<Integer> failedPartitions
    ) {
        Collection<StatisticsAddrRequest<StatisticsGatheringRequest>> reqs = null;
        int cnt = 0;
        Collection<StatisticsAddrRequest<StatisticsGatheringRequest>> failedMsgs;
        do {
            try {
                reqs = helper.generateCollectionRequests(gatId, keys, failedPartitions);
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug(String.format("Cancelling statistics collection %s caused by %s", gatId, e.getMessage()));

                try {
                    statMgr.cancelObjectStatisticsGathering(gatId);
                }
                catch (IgniteCheckedException e1) {
                    log.warning("Unable to cancel statistics gathering " + gatId + " due to " + e1.getMessage());
                }

                return;
            }

            Map<UUID, StatisticsAddrRequest<StatisticsGatheringRequest>> reqsMap = toReqIdMap(reqs);

            remainingRequests.putAll(reqsMap);

            // Process local request
            StatisticsGatheringRequest locReq = findLocal(reqs);
            if (locReq != null)
                statMgr.gatherLocalObjectStatisticsAsync(gatId, locReq.reqId(), locReq.keys(), locReq.parts());

            // Process remote requests
            failedMsgs = sendRequests(reqs);

            if (F.isEmpty(failedMsgs))
                failedPartitions = null;
            else {
                failedPartitions = new ArrayList<>();
                for (StatisticsAddrRequest<StatisticsGatheringRequest> msg : failedMsgs) {
                    remainingRequests.remove(msg.req().reqId());

                    failedPartitions.addAll(GridArrays.list(msg.req().parts()));
                }
            }

            if (cnt++ > MAX_SEND_RETRIES) {
                if (log.isInfoEnabled())
                    log.info(String.format("Unable to send gathering requests for 10 times, cancel gathering %s", gatId));

                try {
                    statMgr.cancelObjectStatisticsGathering(gatId);
                }
                catch (IgniteCheckedException e) {
                    log.warning("Unable to cancel statistics gathering " + gatId + " after max retries due to: "
                        + e.getMessage());
                }
            }
        }
        while (!F.isEmpty(failedPartitions));
    }

    /** {@inheritDoc} */
    @Override public void sendGatheringRequestsAsync(
        UUID gatId,
        Collection<StatisticsKeyMessage> keys,
        Collection<Integer> failedParts
    ) {
        msgMgmtPool.submit(() -> sendGatheringRequests(gatId, keys, failedParts));
    }

    /**
     * Send response to given request.
     *
     * @param reqId Request id to response to.
     * @param statistics Collected statistics.
     * @param parts Partitions from which statistics was collected.
     */
    private void sendGatheringResponse(
        UUID reqId,
        Map<StatisticsKeyMessage, ObjectStatisticsImpl> statistics,
        int[] parts
    ) {
        StatisticsAddrRequest<StatisticsGatheringRequest> req = remainingRequests.remove(reqId);
        if (req == null) {
            if (log.isDebugEnabled())
                log.debug(String.format("Dropping results to cancelled collection request %s", reqId));

            return;
        }
        UUID gatId = req.req().gatId();

        Set<StatisticsObjectData> data = new HashSet<>(statistics.size());
        for (Map.Entry<StatisticsKeyMessage, ObjectStatisticsImpl> stat : statistics.entrySet()) {
            try {
                StatisticsObjectData keyData = StatisticsUtils.toObjectData(stat.getKey(), StatisticsType.LOCAL, stat.getValue());

                data.add(keyData);
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug(String.format("Unable to format statistics %s.%s by request=%s gathering=%s",
                            stat.getKey().schema(), stat.getKey().obj(), reqId, gatId));

            }
        }

        StatisticsGatheringResponse resp = new StatisticsGatheringResponse(req.req().gatId(), reqId, data, parts);

        if (locNodeId.equals(req.sndNodeId()))
            receiveLocalStatistics(locNodeId, resp, req);
        else {
            // Original partitions count to correctly tear down remaining on remote request
            statMgr.onRemoteGatheringSend(gatId, req.req().parts().length);

            try {
                send(req.sndNodeId(), resp);
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug(String.format("Unable to send collected statistics to node %s by request %s by gathering %s",
                        req.sndNodeId(), reqId, resp.gatId()));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void sendGatheringResponseAsync(
        UUID reqId,
        Map<StatisticsKeyMessage, ObjectStatisticsImpl> statistics,
        int[] parts
    ) {
        msgMgmtPool.submit(() -> sendGatheringResponse(reqId, statistics, parts));
    }

    /**
     * Send cancel gathering requests by each remaining request for specified gathering.
     *
     * @param gatId Gathering id to cancel.
     */
    public void sendCancelGathering(UUID gatId) {
        Map<UUID, Collection<UUID>> nodesReqs = new HashMap<>();
        remainingRequests.forEach((reqId, addrReq) -> {
            if (!gatId.equals(addrReq.req().gatId()))
                return;

            StatisticsAddrRequest<StatisticsGatheringRequest> reqToCancel = remainingRequests.remove(reqId);

            if (reqToCancel == null)
                return;

            nodesReqs.computeIfAbsent(reqToCancel.targetNodeId(), nodeId -> new ArrayList<>()).add(reqToCancel.req().reqId());
        });

        Collection<StatisticsAddrRequest<CancelStatisticsGatheringRequest>> cancelReqs = new ArrayList<>(nodesReqs.size());
        for (Map.Entry<UUID, Collection<UUID>> nodeReqs : nodesReqs.entrySet()) {
            CancelStatisticsGatheringRequest req = new CancelStatisticsGatheringRequest(gatId,
                nodeReqs.getValue().toArray(new UUID[0]));
            cancelReqs.add(new StatisticsAddrRequest<>(req, locNodeId, nodeReqs.getKey()));
        }

        CancelStatisticsGatheringRequest locReq = cancelReqs.stream().filter(req -> locNodeId.equals(req.targetNodeId()))
            .map(StatisticsAddrRequest::req).findAny().orElse(null);
        if (locReq != null)
            cancelStatisticsCollection(locNodeId, locReq);

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

        try {
            Collection<StatisticsAddrRequest<StatisticsClearRequest>> msgs = helper.generateClearRequests(keys);
            StatisticsClearRequest localReq = findLocal(msgs);
            if (localReq != null)
                statMgr.clearObjectsStatisticsLocal(localReq.keys());

            Collection<StatisticsAddrRequest<StatisticsClearRequest>> failedMsgs = sendRequests(msgs);

            if (!F.isEmpty(failedMsgs))
                if (log.isDebugEnabled())
                    log.debug(String.format("Unable to send %d clear statistics request", failedMsgs.size()));
        }
        catch (IgniteCheckedException e) {
            if (log.isDebugEnabled())
                log.debug(String.format("Unable to send clear statistics request by keys %s due to %s", keys,
                    e.getMessage()));
        }
    }

    /**
     * Find local request from specified addressed request and return it.
     *
     * @param msgs Collection of addressed messages to find local one.
     * @param <T> Type of messages.
     * @return Local message of {@code null} if there are no message targeted to local node.
     */
    private <T> T findLocal(Collection<StatisticsAddrRequest<T>> msgs) {
        return msgs.stream().filter(
                r -> locNodeId.equals(r.targetNodeId())).map(StatisticsAddrRequest::req).findAny().orElse(null);
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
                send(req.targetNodeId(), req.req());
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

        Map<UUID, IgniteBiTuple<Collection<StatisticsKeyMessage>, Collection<Integer>>> failedGats = new HashMap<>();

        Set<UUID> incomingGatsToCancel = new HashSet<>();

        for (StatisticsAddrRequest<StatisticsGatheringRequest> req : remainingRequests.values()) {
            if (nodeId.equals(req.sndNodeId()))
                incomingGatsToCancel.add(req.req().gatId());

            if (!nodeId.equals(req.targetNodeId()))
                continue;

            StatisticsAddrRequest<StatisticsGatheringRequest> reqToCancel = remainingRequests.remove(req.req().reqId());

            if (reqToCancel == null)
                continue;

            failedGats.computeIfAbsent(reqToCancel.req().gatId(), gatId ->
                new IgniteBiTuple<>(reqToCancel.req().keys(), new ArrayList<>()))
                .getValue().addAll(GridArrays.list(reqToCancel.req().parts()));
        }
        for (UUID gatId : incomingGatsToCancel)
            statMgr.cancelLocalStatisticsGathering(gatId);

        for (Map.Entry<UUID, IgniteBiTuple<Collection<StatisticsKeyMessage>, Collection<Integer>>> failedGat : failedGats.entrySet())
            sendGatheringRequestsAsync(failedGat.getKey(), failedGat.getValue().get1(), failedGat.getValue().get2());
    }

    /**
     * Receive gathering response and match it to request, then process these couple.
     *
     * @param nodeId Sender node id.
     * @param msg Statistics propagation message with partitions statistics to handle.
     * @throws IgniteCheckedException In case of errors.
     */
    private void receiveLocalStatistics(UUID nodeId, StatisticsGatheringResponse msg) {
        assert msg.data().stream().noneMatch(pd -> pd.type() == StatisticsType.PARTITION)
                : "Got partition statistics by request " + msg.reqId();

        StatisticsAddrRequest<StatisticsGatheringRequest> req = remainingRequests.remove(msg.reqId());

        if (req == null) {
            if (log.isDebugEnabled())
                log.debug(String.format(
                        "Ignoring outdated local statistics collection response from node %s to col %s req %s",
                        nodeId, msg.gatId(), msg.reqId()));

            return;
        }

        receiveLocalStatistics(nodeId, msg, req);
    }

    /**
     * Process pair of statistics gathering request and response: save collected statistics and, if there are
     * some skipped partition, reschedule their collection.
     *
     * @param nodeId Sender node id.
     * @param msg Statistics propagation message.
     * @param req Corresponding statistics collection request.
     */
    private void receiveLocalStatistics(
        UUID nodeId,
        StatisticsGatheringResponse msg,
        StatisticsAddrRequest<StatisticsGatheringRequest> req
    ) {
        assert req.targetNodeId().equals(nodeId);
        assert req.sndNodeId().equals(locNodeId);

        statMgr.registerLocalResult(msg.gatId(), msg.data(), msg.parts().length);

        int[] failedParts = GridArrays.subtract(req.req().parts(), msg.parts());

        if (!F.isEmpty(failedParts))
            sendGatheringRequestsAsync(req.req().gatId(), req.req().keys(), GridArrays.list(failedParts));
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
            if (objStats != null) {
                try {
                    data.add(StatisticsUtils.toObjectData(keyMsg, StatisticsType.GLOBAL, objStats));
                }
                catch (IgniteCheckedException e) {
                    if (log.isDebugEnabled()) {
                        log.debug(String.format("Unable to build object statistics message by key %s due to %s",
                                keyMsg, e.getMessage()));
                    }
                }
            }
        }

        try {
            send(nodeId, new StatisticsGetResponse(msg.reqId(), data));
        }
        catch (IgniteCheckedException e) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Unable to send back requested statistics to node %s by request %s",
                    nodeId, msg.reqId()));
            }
        }
    }

    /**
     * Try send message.
     *
     * @param nodeId Target node id.
     * @param msg Message to send.
     */
    private void send(UUID nodeId, Message msg) throws IgniteCheckedException {
        ioMgr.sendToGridTopic(nodeId, TOPIC_STATISTICS, msg, GridIoPolicy.MANAGEMENT_POOL);
    }

    /**
     * Schedule statistics gathering by specified request.
     *
     * @param nodeId Request origin node.
     * @param msg Request message.
     */
    private void handleGatheringRequest(UUID nodeId, StatisticsGatheringRequest msg) {
        remainingRequests.put(msg.reqId(), new StatisticsAddrRequest<>(msg, nodeId, locNodeId));

        statMgr.gatherLocalObjectStatisticsAsync(msg.gatId(), msg.reqId(), msg.keys(), msg.parts());
    }

    /**
     * Handle statistics clear request.
     *
     * @param nodeId UUID of request sender node.
     * @param msg Clear request message.
     */
    private void clearObjectStatistics(UUID nodeId, StatisticsClearRequest msg) {
        statMgr.clearObjectsStatisticsLocal(msg.keys());
    }

    /** {@inheritDoc} */
    @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
        if (msg instanceof StatisticsPropagationMessage) {
            StatisticsPropagationMessage propMsg = (StatisticsPropagationMessage) msg;
            if (propMsg.data().iterator().next().type() == StatisticsType.GLOBAL)
                msgMgmtPool.submit(() -> statMgr.saveGlobalStatistics(propMsg.data()));
            else
                msgMgmtPool.submit(() -> statMgr.receivePartitionsStatistics(propMsg.data()));
        }
        else if (msg instanceof StatisticsGatheringResponse)
            receiveLocalStatistics(nodeId, (StatisticsGatheringResponse) msg);
        else if (msg instanceof StatisticsGetRequest)
            msgMgmtPool.submit(() -> supplyStatistics(nodeId, (StatisticsGetRequest) msg));
        else if (msg instanceof StatisticsGetResponse)
            msgMgmtPool.submit(() -> statMgr.saveGlobalStatistics(((StatisticsGetResponse)msg).data()));
        else if (msg instanceof StatisticsGatheringRequest)
            handleGatheringRequest(nodeId, (StatisticsGatheringRequest)msg);
        else if (msg instanceof CancelStatisticsGatheringRequest)
            cancelStatisticsCollection(nodeId, (CancelStatisticsGatheringRequest) msg);
        else if (msg instanceof StatisticsClearRequest)
            msgMgmtPool.submit(() -> clearObjectStatistics(nodeId, (StatisticsClearRequest)msg));
        else
            log.warning("Unknown msg " + msg + " in statistics topic " + TOPIC_STATISTICS + " from node " + nodeId);
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

            assert removed == null || (removed.sndNodeId().equals(nodeId) && removed.req().gatId().equals(msg.gatId()));
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
                    helper.generateGlobalPropagationMessages(globalStats);

            Collection<StatisticsAddrRequest<StatisticsPropagationMessage>> failedMsgs = sendRequests(msgs);

            if (!F.isEmpty(failedMsgs)) {
                if (log.isDebugEnabled())
                    log.debug(String.format("Failed to send %d global statistics messages.", failedMsgs.size()));
            }
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
                reqs = helper.generatePropagationMessages(key, objStats);
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Unable to send statistics to backups by object %s.%s", key.schema(),
                        key.obj()));
                }

                return;
            }
            failedMsgs = sendRequests(reqs);
        } while (!F.isEmpty(failedMsgs));
    }
}
