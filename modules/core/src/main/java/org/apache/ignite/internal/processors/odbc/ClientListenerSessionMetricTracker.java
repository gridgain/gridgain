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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;

/**
 * Client listener session metric tracker.
 *
 * Helps thin clients to track session
 */
public class ClientListenerSessionMetricTracker {
    /** Client metric group. */
    public static final String CLIENT_METRIC_GROUP = "client";

    /** Client sessions metric group. */
    public static final String CLIENT_SESSIONS_METRIC_GROUP = MetricUtils.metricName(CLIENT_METRIC_GROUP, "sessions");

    /** Client requests metric group. */
    public static final String CLIENT_REQUESTS_METRIC_GROUP = MetricUtils.metricName(CLIENT_METRIC_GROUP, "requests");

    /** Reject reason: timeout. */
    public static final int REJECT_REASON_TIMEOUT = 1;

    /** Reject reason: parsing error. */
    public static final int REJECT_REASON_PARSING_ERROR = 2;

    /** Reject reason: handshake params . */
    public static final int REJECT_REASON_HANDSHAKE_PARAMS = 3;

    /** Reject reason: timeout. */
    public static final int REJECT_REASON_AUTHENTICATION_FAILURE = 4;

    /** Kernal context. */
    private GridKernalContext ctx;

    /** Reject reaon. */
    private int rejectReason = REJECT_REASON_HANDSHAKE_PARAMS;

    /** Indicate whether handshake was accepted. */
    private boolean established;

    /** Number of sessions that did not pass handshake yet. */
    private final AtomicLongMetric waiting;

    /** Number of sessions that were not established because of handshake timeout. */
    private final AtomicLongMetric rejectedDueTimeout;

    /** Number of sessions that were not established because of invalid handshake message. */
    private final AtomicLongMetric rejectedDueParsingError;

    /** Number of sessions that were not established because of rejected handshake message. */
    private AtomicLongMetric rejectedDueHandshakeParams;

    /** Number of sessions that were not established because of failed authentication. */
    private AtomicLongMetric rejectedDueAuthentication;

    /** Number of successfully established sessions. */
    private AtomicLongMetric accepted;

    /** Number of active sessions. */
    private AtomicLongMetric active;

    /** Number of closed sessions. */
    private AtomicLongMetric closed;

    /** Number of handled requests. */
    private AtomicLongMetric handledRequests;

    /** Number of failed requests. */
    private AtomicLongMetric failedRequests;

    /**
     * @param ctx Kernal context.
     */
    public ClientListenerSessionMetricTracker(GridKernalContext ctx) {
        this.ctx = ctx;

        MetricRegistry mreg = ctx.metric().registry(CLIENT_SESSIONS_METRIC_GROUP);

        waiting = mreg.longMetric("waiting", "Number of sessions that did not pass handshake yet.");

        rejectedDueTimeout = mreg.longMetric("rejectedDueTimeout",
            "Number of sessions that were not established because of handshake timeout.");

        rejectedDueParsingError = mreg.longMetric("rejectedDueParsingError",
            "Number of sessions that were not established because of corrupt handshake message.");

        waiting.increment();
    }

    /**
     * Handle handshake.
     * @param clientName Client name.
     */
    public void onHandshakeReceived(String clientName) {
        MetricRegistry mregSes = ctx.metric().registry(MetricUtils.metricName(CLIENT_SESSIONS_METRIC_GROUP, clientName));

        rejectedDueHandshakeParams = mregSes.longMetric("rejectedDueHandshakeParams",
            "Number of sessions that were not established because of rejected handshake message.");

        rejectedDueAuthentication = mregSes.longMetric("rejectedDueAuthentication",
            "Number of sessions that were not established because of failed authentication.");

        accepted = mregSes.longMetric("accepted", "Number of successfully established sessions.");

        active = mregSes.longMetric("active", "Number of active sessions.");

        closed = mregSes.longMetric("closed", "Number of closed sessions.");

        MetricRegistry mregReq = ctx.metric().registry(MetricUtils.metricName(CLIENT_REQUESTS_METRIC_GROUP, clientName));

        handledRequests = mregReq.longMetric("handled", "Number of handled requests.");
        failedRequests = mregReq.longMetric("failed", "Number of failed requests.");
    }

    /**
     * Handle request handling.
     */
    public  void onRequestHandled() {
        handledRequests.increment();
    }

    /**
     * Handle request failing.
     */
    public  void onRequestFailed() {
        failedRequests.increment();
    }

    /**
     * Handle session acceptance.
     */
    public void onHandshakeAccepted() {
        established = true;

        accepted.increment();
        active.increment();
        waiting.decrement();
    }

    /**
     * Handle sesstion rejection.
     * @param reason Reject reason.
     */
    public void onHandshakeRejected(int reason) {
        rejectReason = reason;
    }

    /**
     * Handle session close.
     */
    public void onSessionClosed() {
        if (established) {
            active.decrement();

            closed.increment();
        }
        else
        {
            waiting.decrement();

            switch (rejectReason) {
                case REJECT_REASON_TIMEOUT: {
                    rejectedDueTimeout.increment();

                    break;
                }

                case REJECT_REASON_PARSING_ERROR: {
                    rejectedDueParsingError.increment();

                    break;
                }

                case REJECT_REASON_AUTHENTICATION_FAILURE: {
                    rejectedDueAuthentication.increment();

                    break;
                }

                case REJECT_REASON_HANDSHAKE_PARAMS:
                default: {
                    rejectedDueHandshakeParams.increment();

                    break;
                }
            }
        }
    }
}
