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
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;

import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.CLIENT_METRIC_GROUP;

/**
 * Client listener session metric tracker.
 *
 * Helps thin clients to track session
 */
public class ClientListenerSessionMetricTracker {
    /** Client sessions metric group. */
    public static final String CLIENT_SESSIONS_METRIC_GROUP = MetricUtils.metricName(CLIENT_METRIC_GROUP, "sessions");

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
    private final IntMetricImpl waiting;

    /** Number of sessions that were not established because of handshake timeout. */
    private final IntMetricImpl rejectedDueTimeout;

    /** Number of sessions that were not established because of invalid handshake message. */
    private final IntMetricImpl rejectedDueParsingError;

    /** Number of sessions that were not established because of rejected handshake message. */
    private IntMetricImpl rejectedDueHandshakeParams;

    /** Number of sessions that were not established because of failed authentication. */
    private IntMetricImpl rejectedDueAuthentication;

    /** Number of successfully established sessions. */
    private IntMetricImpl accepted;

    /** Number of active sessions. */
    private IntMetricImpl active;

    /** Number of closed sessions. */
    private IntMetricImpl closed;

    /**
     * @param ctx Kernal context.
     */
    public ClientListenerSessionMetricTracker(GridKernalContext ctx) {
        this.ctx = ctx;

        MetricRegistry mreg = ctx.metric().registry(CLIENT_SESSIONS_METRIC_GROUP);

        waiting = mreg.intMetric("waiting", "Number of sessions that did not pass handshake yet.");

        rejectedDueTimeout = mreg.intMetric("rejectedDueTimeout",
            "Number of sessions that were not established because of handshake timeout.");

        rejectedDueParsingError = mreg.intMetric("rejectedDueParsingError",
            "Number of sessions that were not established because of corrupt handshake message.");

        waiting.increment();
    }

    /**
     * Handle handshake.
     * @param clientName Client name.
     */
    public void onHandshakeReceived(String clientName) {
        MetricRegistry mreg = ctx.metric().registry(MetricUtils.metricName(CLIENT_SESSIONS_METRIC_GROUP, clientName));

        rejectedDueHandshakeParams = mreg.intMetric("rejectedDueHandshakeParams",
            "Number of sessions that were not established because of rejected handshake message.");

        rejectedDueAuthentication = mreg.intMetric("rejectedDueAuthentication",
            "Number of sessions that were not established because of failed authentication.");

        accepted = mreg.intMetric("accepted", "Number of successfully established sessions.");

        active = mreg.intMetric("active", "Number of active sessions.");

        closed = mreg.intMetric("closed", "Number of closed sessions.");
    }

    /**
     * Handle session acceptance.
     */
    public void onHandshakeAccepted() {
        established = true;

        accepted.increment();
        active.increment();
        waiting.add(-1);
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
            active.add(-1);

            closed.increment();
        }
        else
        {
            waiting.add(-1);

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
