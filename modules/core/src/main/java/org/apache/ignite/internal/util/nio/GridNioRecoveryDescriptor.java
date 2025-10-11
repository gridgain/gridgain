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

package org.apache.ignite.internal.util.nio;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.unmodifiableCollection;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_NIO_RECOVERY_DESCRIPTOR_RESERVATION_TIMEOUT;

/**
 * Recovery information for single node.
 */
public class GridNioRecoveryDescriptor {
    /** @see IgniteSystemProperties#IGNITE_NIO_RECOVERY_DESCRIPTOR_RESERVATION_TIMEOUT */
    public static final int DFLT_NIO_RECOVERY_DESCRIPTOR_RESERVATION_TIMEOUT = 5_000;

    /** Timeout for outgoing recovery descriptor reservation. */
    private static final long DESC_RESERVATION_TIMEOUT =
        Math.max(1_000, IgniteSystemProperties.getLong(IGNITE_NIO_RECOVERY_DESCRIPTOR_RESERVATION_TIMEOUT,
            DFLT_NIO_RECOVERY_DESCRIPTOR_RESERVATION_TIMEOUT));

    /** Number of acknowledged messages. */
    private long acked;

    /**
     * Unacknowledged messages.
     * <p>
     * {@link #msgReqsSize} MUST be updated whenever size of this queue is updated!
     */
    private final ArrayDeque<SessionWriteRequest> msgReqs;

    /**
     * Size of {@link #msgReqs}. Used to access queue size via race. It is safer than calling msgReqs.size() via race
     * as field read is atomic and size() is not guaranteed to be atomic. Visibility concerns are handled using
     * external mechanisms.
     * <p>
     * This MUST be updated whenever {@link #msgReqs} size is updated!
     */
    private int msgReqsSize = 0;

    /** Number of messages to resend. */
    private int resendCnt;

    /** Number of received messages. */
    private long rcvCnt;

    /** Number of received bytes. */
    private long rcvBytes;

    /** Number of sent messages. */
    private long sentCnt;

    /** Reserved flag. */
    private boolean reserved;

    /** Last acknowledged message. */
    private long lastAck;

    /** Number of received bytes at the moment of the last ack. */
    private long lastAckRcvBytes;

    /** Node left flag. */
    private boolean nodeLeft;

    /** Target node. */
    private final ClusterNode node;

    /** Logger. */
    private final IgniteLogger log;

    /** Connected flag. */
    private boolean connected;

    /** Number of outgoing connect attempts. */
    private long connectCnt;

    /** Maximum size of unacknowledged messages queue. */
    private final int queueLimit;

    /** Number of accrued bytes of received messages to trigger an ack. */
    private final long ackThresholdBytes;

    /** Number of descriptor reservations (for info purposes). */
    private int reserveCnt;

    /** */
    private final boolean pairedConnections;

    /** Session for the descriptor. */
    @GridToStringExclude
    @Nullable
    private GridNioSession ses;

    /**
     * Used to synchronize access to methods and fields used to determine whether an ack should be sent
     * ({@link #rcvCnt}, {@link #rcvBytes}, {@link #lastAckRcvBytes}, {@link #onReceived()},
     * {@link #lastAcknowledged()}) and sending acks back ({@link #lastAcknowledged(long)}.
     */
    private final Object receiveAndAckMonitor = new Object();

    /**
     * @param pairedConnections {@code True} if in/out connections pair is used for communication with node.
     * @param queueLimit Maximum size of unacknowledged messages queue.
     * @param ackThresholdBytes Number of accrued bytes of received messages to trigger ack.
     * @param node Node.
     * @param log Logger.
     */
    public GridNioRecoveryDescriptor(
        boolean pairedConnections,
        int queueLimit,
        long ackThresholdBytes,
        ClusterNode node,
        IgniteLogger log
    ) {
        assert !node.isLocal() : node;
        assert queueLimit > 0;
        assert ackThresholdBytes > 0;

        msgReqs = new ArrayDeque<>(queueLimit);

        this.pairedConnections = pairedConnections;
        this.queueLimit = queueLimit;
        this.ackThresholdBytes = ackThresholdBytes;
        this.node = node;
        this.log = log;
    }

    /**
     * @return {@code True} if in/out connections pair is used for communication with node.
     */
    public boolean pairedConnections() {
        return pairedConnections;
    }

    /**
     * @return Connect count.
     */
    public long incrementConnectCount() {
        return connectCnt++;
    }

    /**
     * @return Node.
     */
    public ClusterNode node() {
        return node;
    }

    /**
     * Increments received messages counter.
     *
     * @param currentBytesReceived Current bytesReceived on the session.
     * @return Number of received messages.
     */
    public long onReceived(long currentBytesReceived) {
        rcvCnt++;

        rcvBytes = currentBytesReceived;

        return rcvCnt;
    }

    /**
     * @return Number of received messages.
     */
    public long received() {
        synchronized (receiveAndAckMonitor) {
            return rcvCnt;
        }
    }

    /**
     * @return Number of sent messages.
     */
    public long sent() {
        return sentCnt;
    }

    /**
     * @param lastAck Last acknowledged message.
     */
    public void lastAcknowledged(long lastAck) {
        this.lastAck = lastAck;

        lastAckRcvBytes = rcvBytes;
    }

    /**
     * @return Last acknowledged message.
     */
    public long lastAcknowledged() {
        synchronized (receiveAndAckMonitor) {
            return lastAck;
        }
    }

    /**
     * @return Maximum size of unacknowledged messages queue.
     */
    public int queueLimit() {
        return queueLimit;
    }

    /**
     * @param req Write request.
     * @return {@code False} if queue limit is exceeded.
     */
    public boolean add(SessionWriteRequest req) {
        assert req != null;

        if (!req.skipRecovery()) {
            if (resendCnt == 0) {
                msgReqs.addLast(req);
                msgReqsSize = msgReqs.size();

                sentCnt++;

                return msgReqs.size() < queueLimit;
            }
            else {
                // Recovery is happening now and messages that were in #msgReqs at the moment the recovery was started
                // are added for a resend. While #resendCnt is positive we ONLY get messages that were not acked, so
                // they are already in #msgReqs, so we don't need to add them there again, so we just decrease
                // #resendCnt. When it reaches zero, we'll switch to the 'normal' mode (as this will mean that all
                // messages that were in #msgReqs at the moment when recovery started are passed through this method).

                resendCnt--;
            }
        }

        return true;
    }

    /**
     * @param rcvCnt Number of messages received by remote node.
     */
    public void ackReceived(long rcvCnt) {
        if (log.isDebugEnabled())
            log.debug("Handle acknowledgment [acked=" + acked + ", rcvCnt=" + rcvCnt +
                ", msgReqs=" + msgReqs.size() + ']');

        while (acked < rcvCnt) {
            SessionWriteRequest req = msgReqs.pollFirst();
            msgReqsSize = msgReqs.size();

            assert req != null : "Missed message [rcvCnt=" + rcvCnt +
                ", acked=" + acked +
                ", desc=" + this + ']';

            if (req.ackClosure() != null)
                req.ackClosure().apply(null);

            req.onAckReceived();

            acked++;
        }
    }

    /**
     * @return Last acked message by remote node.
     */
    public long acked() {
        return acked;
    }

    /**
     * Node left callback.
     *
     * @return {@code False} if descriptor is reserved.
     */
    public boolean onNodeLeft() {
        SessionWriteRequest[] reqs = null;

        synchronized (this) {
            nodeLeft = true;

            if (reserved)
                return false;

            if (!msgReqs.isEmpty()) {
                reqs = msgReqs.toArray(new SessionWriteRequest[msgReqs.size()]);

                msgReqs.clear();
                msgReqsSize = msgReqs.size();
            }
        }

        if (reqs != null)
            notifyOnNodeLeft(reqs);

        return true;
    }

    /**
     * @return Requests for unacknowledged messages.
     */
    public Collection<SessionWriteRequest> messagesRequests() {
        return unmodifiableCollection(msgReqs);
    }

    /**
     * Returns {@code true} if and only if the unacknowledged messages queue is empty.
     *
     * @return {@code true} if and only if the unacknowledged messages queue is empty.
     */
    public boolean isMessageRequestsEmpty() {
        return msgReqs.isEmpty();
    }

    /**
     * Returns number of messages that are not acknowledged yet. Without proper synchronization by the caller,
     * this might return a stale value.
     *
     * @return Number of messages that are not acknowledged yet.
     */
    public int messageRequestsCount() {
        return msgReqsSize;
    }

    /**
     * @param node Node.
     * @return {@code True} if node is not null and has the same order as initial remote node.
     */
    public boolean nodeAlive(@Nullable ClusterNode node) {
        return node != null && node.order() == this.node.order();
    }

    /**
     * @return {@code True} if reserved.
     * @throws InterruptedException If interrupted.
     */
    public boolean reserve() throws InterruptedException {
        synchronized (this) {
            long t0 = System.nanoTime();

            while (!connected && reserved) {
                wait(DESC_RESERVATION_TIMEOUT);

                if ((System.nanoTime() - t0) / 1_000_000 >= DESC_RESERVATION_TIMEOUT - 100) {
                    // Dumping a descriptor.
                    log.error("Failed to wait for recovery descriptor reservation " +
                        "[desc=" + this + ", ses=" + ses + ']');

                    return false;
                }
            }

            if (!connected) {
                reserved = true;

                reserveCnt++;
            }

            return !connected;
        }
    }

    /**
     * @param rcvCnt Number of messages received by remote node.
     */
    public void onHandshake(long rcvCnt) {
        synchronized (this) {
            if (!nodeLeft)
                ackReceived(rcvCnt);

            resendCnt = msgReqs.size();
        }
    }

    /**
     *
     */
    public void onConnected() {
        synchronized (this) {
            assert reserved : this;
            assert !connected : this;

            connected = true;

            notifyAll();
        }
    }

    /**
     * @return Connected flag.
     */
    public boolean connected() {
        synchronized (this) {
            return connected;
        }
    }

    /**
     * @return Reserved flag.
     */
    public boolean reserved() {
        synchronized (this) {
            return reserved;
        }
    }

    /**
     *
     */
    public void release() {
        SessionWriteRequest[] futs = null;

        synchronized (this) {
            ses = null;

            connected = false;

            reserved = false;

            notifyAll();

            if (nodeLeft && !msgReqs.isEmpty()) {
                futs = msgReqs.toArray(new SessionWriteRequest[msgReqs.size()]);

                msgReqs.clear();
                msgReqsSize = msgReqs.size();
            }
        }

        if (futs != null)
            notifyOnNodeLeft(futs);
    }

    /**
     * @return {@code True} if reserved.
     */
    public boolean tryReserve() {
        synchronized (this) {
            if (connected || reserved)
                return false;
            else {
                reserved = true;

                reserveCnt++;

                return true;
            }
        }
    }

    /**
     * @return Number of descriptor reservations.
     */
    public int reserveCount() {
        synchronized (this) {
            return reserveCnt;
        }
    }

    /**
     * @return Current session.
     */
    public synchronized GridNioSession session() {
        return ses;
    }

    /**
     * @param ses Session.
     */
    public synchronized void session(GridNioSession ses) {
        this.ses = ses;
    }

    /**
     * @param reqs Requests to notify about error.
     */
    private void notifyOnNodeLeft(SessionWriteRequest[] reqs) {
        IOException e = new IOException("Failed to send message, node has left: " + node.id());
        IgniteException cloErr = null;

        for (SessionWriteRequest req : reqs) {
            req.onError(e);

            if (req.ackClosure() != null) {
                if (cloErr == null)
                    cloErr = new IgniteException(e);

                req.ackClosure().apply(cloErr);
            }
        }
    }

    /**
     * @return {@code true} if enough received messages were accrued since last acknowledge to trigger an ack.
     */
    public boolean ackThresholdInBytesExceeded() {
        return rcvBytes - lastAckRcvBytes >= ackThresholdBytes;
    }

    /**
     * @return Monitor used to synchronize access to methods and fields used to determine whether an ack should be sent
     * and sending acks back.
     */
    public Object receiveAndAckMonitor() {
        return receiveAndAckMonitor;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioRecoveryDescriptor.class, this);
    }
}
