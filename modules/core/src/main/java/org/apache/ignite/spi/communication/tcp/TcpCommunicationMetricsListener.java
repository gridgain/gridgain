/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.spi.communication.tcp;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.metric.HistogramMetric;
import org.apache.ignite.internal.util.nio.GridNioMetricsListener;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.IdMessage;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.TimeLoggableMessage;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_COMM_SPI_TIME_HIST_BOUNDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_MESSAGES_TIME_LOGGING;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MESSAGES_INFO_STORE_TIME;

/**
 * Statistics for {@link org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi}.
 */
public class TcpCommunicationMetricsListener implements GridNioMetricsListener{
    /** Default history bounds in milliseconds. */
    public static final long[] DEFAULT_HIST_BOUNDS = new long[]{10, 20, 40, 80, 160, 320, 500, 1000, 2000, 4000};

    /** */
    public static final int NANOS_TO_MILLIS_COEF = 1_000_000;

    /** */
    public static final String BOUNDS_PARAM_DELIMITER = ",";

    /** Counter factory. */
    private static final Callable<LongHolder> HOLDER_FACTORY = new Callable<LongHolder>() {
        @Override public LongHolder call() {
            return new LongHolder();
        }
    };

    /** Received bytes count. */
    private final LongAdder rcvdBytesCnt = new LongAdder();

    /** Sent bytes count.*/
    private final LongAdder sentBytesCnt = new LongAdder();

    /** All registered metrics. */
    private final Set<ThreadMetrics> allMetrics = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /** Thread-local metrics. */
    private final ThreadLocal<ThreadMetrics> threadMetrics = new ThreadLocal<ThreadMetrics>() {
        @Override protected ThreadMetrics initialValue() {
            ThreadMetrics metrics = new ThreadMetrics();

            allMetrics.add(metrics);

            return metrics;
        }
    };

    /** Method to synchronize access to message type map. */
    private final Object msgTypMapMux = new Object();

    /** Message type map. */
    private volatile Map<Short, String> msgTypMap;

    /** {@inheritDoc} */
    @Override public void onBytesSent(int bytesCnt) {
        sentBytesCnt.add(bytesCnt);
    }

    /** {@inheritDoc} */
    @Override public void onBytesReceived(int bytesCnt) {
        rcvdBytesCnt.add(bytesCnt);
    }

    /**
     * Collects statistics for message sent by SPI.
     *
     * @param msg Sent message.
     * @param nodeId Receiver node id.
     */
    public void onMessageSent(Message msg, UUID nodeId) {
        assert msg != null;
        assert nodeId != null;

        if (msg instanceof GridIoMessage) {
            msg = ((GridIoMessage) msg).message();

            updateMessageTypeMap(msg);

            ThreadMetrics metrics = threadMetrics.get();

            metrics.onMessageSent(msg, nodeId);
        }
    }

    /**
     * Collects statistics for message received by SPI.
     *
     * @param msg Received message.
     * @param nodeId Sender node id.
     */
    public void onMessageReceived(Message msg, UUID nodeId) {
        assert msg != null;
        assert nodeId != null;

        if (msg instanceof GridIoMessage) {
            msg = ((GridIoMessage) msg).message();

            updateMessageTypeMap(msg);

            ThreadMetrics metrics = threadMetrics.get();

            metrics.onMessageReceived(msg, nodeId);
        }
    }

    /**
     * Gets sent messages count.
     *
     * @return Sent messages count.
     */
    public int sentMessagesCount() {
        long res = 0;

        for (ThreadMetrics metrics : allMetrics)
            res += metrics.sentMsgsCnt;

        int res0 = (int)res;

        if (res0 < 0)
            res0 = Integer.MAX_VALUE;

        return res0;
    }

    /**
     * Gets sent bytes count.
     *
     * @return Sent bytes count.
     */
    public long sentBytesCount() {
        return sentBytesCnt.longValue();
    }

    /**
     * Gets received messages count.
     *
     * @return Received messages count.
     */
    public int receivedMessagesCount() {
        long res = 0;

        for (ThreadMetrics metrics : allMetrics)
            res += metrics.rcvdMsgsCnt;

        int res0 = (int)res;

        if (res0 < 0)
            res0 = Integer.MAX_VALUE;

        return res0;
    }

    /**
     * Gets received bytes count.
     *
     * @return Received bytes count.
     */
    public long receivedBytesCount() {
        return rcvdBytesCnt.longValue();
    }

    /**
     * Gets received messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public Map<String, Long> receivedMessagesByType() {
        Map<Short, Long> res = new HashMap<>();

        for (ThreadMetrics metrics : allMetrics)
            addMetrics(res, metrics.rcvdMsgsCntByType);

        return convertMessageTypes(res);
    }

    /**
     * Convert message types.
     *
     * @param input Input map.
     * @return Result map.
     */
    private Map<String, Long> convertMessageTypes(Map<Short, Long> input) {
        Map<String, Long> res = new HashMap<>(input.size());

        Map<Short, String> msgTypMap0 = msgTypMap;

        if (msgTypMap0 != null) {
            for (Map.Entry<Short, Long> inputEntry : input.entrySet()) {
                String typeName = msgTypMap0.get(inputEntry.getKey());

                if (typeName != null)
                    res.put(typeName, inputEntry.getValue());
            }
        }

        return res;
    }

    /**
     * Gets received messages counts (grouped by node).
     *
     * @return Map containing sender nodes and respective counts.
     */
    public Map<UUID, Long> receivedMessagesByNode() {
        Map<UUID, Long> res = new HashMap<>();

        for (ThreadMetrics metrics : allMetrics)
            addMetrics(res, metrics.rcvdMsgsCntByNode);

        return res;
    }

    /**
     * Gets sent messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public Map<String, Long> sentMessagesByType() {
        Map<Short, Long> res = new HashMap<>();

        for (ThreadMetrics metrics : allMetrics)
            addMetrics(res, metrics.sentMsgsCntByType);

        return convertMessageTypes(res);
    }

    /**
     * Gets sent messages counts (grouped by node).
     *
     * @return Map containing receiver nodes and respective counts.
     */
    public Map<UUID, Long> sentMessagesByNode() {
        Map<UUID, Long> res = new HashMap<>();

        for (ThreadMetrics metrics : allMetrics)
            addMetrics(res, metrics.sentMsgsCntByNode);

        return res;
    }

    /**
     * @return Map containing histogram metrics for outcomming messages by node by message class.
     */
    public Map<UUID, Map<Class<? extends Message>, HistogramMetric>> outMetricsByNodeByMsgClass() {
        Map<UUID, Map<Class<? extends Message>, HistogramMetric>> res = new HashMap<>();

        for (ThreadMetrics metrics : allMetrics)
            addHistMetrics(res, metrics.outMetricsMap);

        return res;
    }

    /**
     * @return Map containing histogram metrics for incomming messages by node by message class.
     */
    public Map<UUID, Map<Class<? extends Message>, HistogramMetric>> inMetricsByNodeByMsgClass() {
        Map<UUID, Map<Class<? extends Message>, HistogramMetric>> res = new HashMap<>();

        for (ThreadMetrics metrics : allMetrics)
            addHistMetrics(res, metrics.inMetricsMap);

        return res;
    }

    /**
     * Resets metrics for this instance.
     */
    public void resetMetrics() {
        for (ThreadMetrics metrics : allMetrics)
            metrics.reset();
    }

    /**
     * Add single metrics to the total.
     *
     * @param total Total.
     * @param current Current metrics.
     */
    private <T> void addMetrics(Map<T, Long> total, Map<T, LongHolder> current) {
        for (Map.Entry<T, LongHolder> entry : current.entrySet()) {
            T key = entry.getKey();
            long val = entry.getValue().val;

            Long prevVal = total.get(key);

            total.put(key, prevVal == null ? val : prevVal + val);
        }
    }

    /**
     * Adds single metrics to the total.
     * Merges histogram metrics data.
     * @param total Total.
     * @param current Current.
     */
    private void addHistMetrics(Map<UUID, Map<Class<? extends Message>, HistogramMetric>> total,
                                Map<UUID, Map<Class<? extends Message>, HistogramMetric>> current) {
        for (Map.Entry<UUID, Map<Class<? extends Message>, HistogramMetric>> entry: current.entrySet()) {
            UUID nodeId = entry.getKey();
            Map<Class<? extends Message>, HistogramMetric> classMetrics = entry.getValue();


            if (!total.containsKey(nodeId))
                total.put(nodeId, classMetrics);
            else {
                Map<Class<? extends Message>, HistogramMetric> totalClsMetrics = total.get(nodeId);

                for (Map.Entry<Class<? extends Message>, HistogramMetric> entry1: classMetrics.entrySet()) {
                    Class<? extends Message> msgClass = entry1.getKey();
                    HistogramMetric histMetric = entry1.getValue();

                    if (!totalClsMetrics.containsKey(msgClass))
                        totalClsMetrics.put(msgClass, histMetric);
                    else
                        totalClsMetrics.get(msgClass).addValues(histMetric);
                }
            }


        }
    }

    /**
     * Update message type map.
     *
     * @param msg Message.
     */
    private void updateMessageTypeMap(Message msg) {
        short typeId = msg.directType();

        Map<Short, String> msgTypMap0 = msgTypMap;

        if (msgTypMap0 == null || !msgTypMap0.containsKey(typeId)) {
            synchronized (msgTypMapMux) {
                if (msgTypMap == null) {
                    msgTypMap0 = new HashMap<>();

                    msgTypMap0.put(typeId, msg.getClass().getName());

                    msgTypMap = msgTypMap0;
                }
                else {
                    if (!msgTypMap.containsKey(typeId)) {
                        msgTypMap0 = new HashMap<>(msgTypMap);

                        msgTypMap0.put(typeId, msg.getClass().getName());

                        msgTypMap = msgTypMap0;
                    }
                }
            }
        }
    }

    /**
     *
     */
    public static String metricName(UUID nodeId, Class msgClass) {
        return nodeId + "." + msgClass.getSimpleName();
    }

    /**
     * Long value holder.
     */
    private static class LongHolder {
        /** Value. */
        private long val;

        /**
         * Increment value.
         */
        private void increment() {
            val++;
        }
    }

    /**
     * Thread-local metrics.
     */
    private static class ThreadMetrics {
        /** Received messages count. */
        private long rcvdMsgsCnt;

        /** Sent messages count. */
        private long sentMsgsCnt;

        /** Received messages count grouped by message type. */
        private final HashMap<Short, LongHolder> rcvdMsgsCntByType = new HashMap<>();

        /** Received messages count grouped by sender. */
        private final HashMap<UUID, LongHolder> rcvdMsgsCntByNode = new HashMap<>();

        /** Sent messages count grouped by message type. */
        private final HashMap<Short, LongHolder> sentMsgsCntByType = new HashMap<>();

        /** Sent messages count grouped by receiver. */
        private final HashMap<UUID, LongHolder> sentMsgsCntByNode = new HashMap<>();

        /** Stores time latencies for each sent request class and each node */
        private final Map<UUID, Map<Class<? extends Message>, HistogramMetric>> outMetricsMap = Collections.synchronizedMap(new HashMap<>());

        /** Stores sent requests' timestamps that did't get response for each request class and each node */
        private final Map<UUID, Map<Class<? extends Message>, Map<Long, Long>>> outTimestamps = Collections.synchronizedMap(new HashMap<>());

        /** Stores time latencies for each sent request class and each node */
        private final Map<UUID, Map<Class<? extends Message>, HistogramMetric>> inMetricsMap = Collections.synchronizedMap(new HashMap<>());

        /** Stores sent requests' timestamps that did't get response for each request class and each node */
        private final Map<UUID, Map<Class<? extends Message>, Map<Long, Long>>> inTimestamps = Collections.synchronizedMap(new HashMap<>());

        /**
         * Collects statistics for message sent by SPI.
         *
         * @param msg Sent message.
         * @param nodeId Receiver node id.
         */
        private void onMessageSent(Message msg, UUID nodeId) {
            sentMsgsCnt++;

            LongHolder cntByType = F.addIfAbsent(sentMsgsCntByType, msg.directType(), HOLDER_FACTORY);
            LongHolder cntByNode = F.addIfAbsent(sentMsgsCntByNode, nodeId, HOLDER_FACTORY);

            assert cntByType != null;
            assert cntByNode != null;

            cntByType.increment();
            cntByNode.increment();

            addTimestamp(nodeId, msg, true);
        }

        /**
         * Collects statistics for message received by SPI.
         *
         * @param msg Received message.
         * @param nodeId Sender node id.
         */
        private void onMessageReceived(Message msg, UUID nodeId) {
            rcvdMsgsCnt++;

            LongHolder cntByType = F.addIfAbsent(rcvdMsgsCntByType, msg.directType(), HOLDER_FACTORY);
            LongHolder cntByNode = F.addIfAbsent(rcvdMsgsCntByNode, nodeId, HOLDER_FACTORY);

            assert cntByType != null;
            assert cntByNode != null;

            cntByType.increment();
            cntByNode.increment();

            addTimestamp(nodeId, msg, false);
        }

        /**
         * Reset metrics.
         */
        private void reset() {
            rcvdMsgsCnt = 0;
            sentMsgsCnt = 0;

            sentMsgsCntByType.clear();
            sentMsgsCntByNode.clear();

            rcvdMsgsCntByType.clear();
            rcvdMsgsCntByNode.clear();

            inMetricsMap.clear();
            inTimestamps.clear();

            outMetricsMap.clear();
            outTimestamps.clear();
        }

        /**
         * Adds information about message timestamp. Messages from local node are ignored.
         *
         * @param nodeId Node id.
         * @param msg Message.
         */
        private void addTimestamp(UUID nodeId, @NotNull Message msg, boolean outcomming) {
            if (!isEnabled() || !(msg instanceof TimeLoggableMessage))
                return;

            IdMessage idmsg = (IdMessage)msg;

            if (idmsg.messageId() > 0)
                processRequest(nodeId, idmsg, outcomming);
            else
                processResponse(nodeId, idmsg, !outcomming);
        }

        /**
         * Processes request. Request id, node id and request system time are added to {@code timestampMap}
         *
         * @param nodeId Request sender node Id.
         * @param req Request message.
         * @param thisNodeSender {@code true} for outcomming requests and incomming responses.
         *  {@code false} for incomming requests and outcomming responses.
         */
        private void processRequest(UUID nodeId, @NotNull IdMessage req, boolean thisNodeSender) {
            Map<UUID, Map<Class<? extends Message>, Map<Long, Long>>> reqTimestamps = timestampMap(thisNodeSender);
            Map<UUID, Map<Class<? extends Message>, HistogramMetric>> metricsMap    = metricsMap(thisNodeSender);

            if (!reqTimestamps.containsKey(nodeId)) {
                // No requests were sent to or received from nodeId
                reqTimestamps.put(nodeId, new ConcurrentHashMap<>());

                metricsMap.put(nodeId, new ConcurrentHashMap<>());
            }

            Map<Class<? extends Message>, Map<Long, Long>> nodeMap = reqTimestamps.get(nodeId);

            if (!nodeMap.containsKey(req.getClass())) {
                // No message of certain class were sent to or received from nodeId
                nodeMap.put(req.getClass(), Collections.synchronizedMap(new TimestampMap()));

                metricsMap.get(nodeId).put(req.getClass(), new HistogramMetric(metricName(nodeId, req.getClass()),
                                                                         "",
                                                                               obtainHistMetricBounds()));
            }

            Map<Long, Long> reqResTimeMap = nodeMap.get(req.getClass());

            reqResTimeMap.putIfAbsent(req.messageId(), System.nanoTime());
        }

        /**
         * Processes response. If {@code timestampMap} contains corresponding request time difference between response
         * and request is added to metric.
         *
         * @param nodeId Response sender id.
         * @param resp Response message.
         */
        private void processResponse(UUID nodeId, @NotNull IdMessage resp, boolean outcomming) {
            Map<UUID, Map<Class<? extends Message>, Map<Long, Long>>> reqTimestamps = timestampMap(outcomming);
            Map<UUID, Map<Class<? extends Message>, HistogramMetric>> metricsMap    = metricsMap(outcomming);

            if (!reqTimestamps.containsKey(nodeId))
                return;

            Map<Class<? extends Message>, Map<Long, Long>> nodeMap = reqTimestamps.get(nodeId);

            // No message of certain class were sent to or received from nodeId
            for (Map.Entry<Class<? extends Message>, Map<Long, Long>> entry : nodeMap.entrySet()) {
                Map<Long, Long> reqResTimeMap = entry.getValue();

                long reqId = -resp.messageId();

                if (reqResTimeMap.containsKey(reqId)) {
                    Long reqTimestamp = reqResTimeMap.get(reqId);

                    metricsMap.get(nodeId).get(entry.getKey()).value((System.nanoTime() - reqTimestamp) / NANOS_TO_MILLIS_COEF);

                    reqResTimeMap.remove(reqId);

                    break;
                }
            }
        }

        /**
         * @return Metrics bounds in milliseconds.
         */
        private long[] obtainHistMetricBounds() {
            String strBounds = IgniteSystemProperties.getString(IGNITE_COMM_SPI_TIME_HIST_BOUNDS);

            if (strBounds == null)
                return DEFAULT_HIST_BOUNDS;

            try {
                return Arrays.stream(strBounds.split(BOUNDS_PARAM_DELIMITER))
                    .map(String::trim)
                    .mapToLong(Long::valueOf)
                    .toArray();
            } catch (Exception e) {
                return DEFAULT_HIST_BOUNDS;
            }
        }

        /**
         * @return {@code true} if IGNITE_ENABLE_MESSAGES_TIME_LOGGING jvm option is set to {@code true}, {@code false}
         * otherwise.
         */
        private boolean isEnabled() {
            return IgniteSystemProperties.getBoolean(IGNITE_ENABLE_MESSAGES_TIME_LOGGING);
        }

        /**
         *
         */
        private Map<UUID, Map<Class<? extends Message>, HistogramMetric>> metricsMap(boolean isThisNodeSender) {
            return isThisNodeSender ? outMetricsMap : inMetricsMap;
        }

        /**
         *
         */
        private Map<UUID, Map<Class<? extends Message>, Map<Long, Long>>> timestampMap(boolean isThisNodeSender) {
            return isThisNodeSender ? outTimestamps : inTimestamps;
        }
    }

    /**
     * Map with old entries eviction.
     */
    public static class TimestampMap extends LinkedHashMap<Long, Long> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Max timestamp age in nanoseconds */
        private final long maxTimestampAge = IgniteSystemProperties.getLong(IGNITE_MESSAGES_INFO_STORE_TIME, 300) * 1_000_000_000;

        /** {@inheritDoc} */
        @Override public Long putIfAbsent(Long k, Long v) {
            evict();

            return super.putIfAbsent(k, v);
        }

        /** {@inheritDoc} */
        @Override public Long put(Long k, Long v) {
            evict();

            return super.put(k, v);
        }

        /**
         * Evicts messages older then {@code MAX_TIMESTAMP_AGE}
         */
        private void evict() {
            Iterator<Map.Entry<Long, Long>> iter = entrySet().iterator();

            while (iter.hasNext() && System.nanoTime() - iter.next().getValue() > maxTimestampAge)
                iter.remove();
        }
    }

}
