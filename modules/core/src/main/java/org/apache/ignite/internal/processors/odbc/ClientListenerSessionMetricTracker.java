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

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;

import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.JDBC_CLIENT;
import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.ODBC_CLIENT;
import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.THIN_CLIENT;

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

    /** Kernal context. */
    private GridKernalContext ctx;

    /** Indicate whether handshake was accepted. */
    private boolean established;

    /** Number of sessions that did not pass handshake yet. */
    private final AtomicLongMetric waiting;

    /** Number of sessions that were rejected. */
    private final AtomicLongMetric rejected;

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

        waiting = mreg.longMetric("waiting", "Number of sessions that did not pass handshake yet");
        rejected = mreg.longMetric("rejected", "Number of sessions that were rejected");
    }

    /**
     * Handle handshake.
     * @param clientType Client type.
     */
    public void onHandshakeReceived(byte clientType) {
        String clientName = clientTypeToMetricNamespace(clientType);

        MetricRegistry mregSes = ctx.metric().registry(MetricUtils.metricName(CLIENT_SESSIONS_METRIC_GROUP, clientName));

        accepted = mregSes.longMetric("accepted", "Number of successfully established sessions");

        active = mregSes.longMetric("active", "Number of active sessions");

        closed = mregSes.longMetric("closed", "Number of closed sessions");

        MetricRegistry mregReq = ctx.metric().registry(MetricUtils.metricName(CLIENT_REQUESTS_METRIC_GROUP, clientName));

        handledRequests = mregReq.longMetric("handled", "Number of handled requests");
        failedRequests = mregReq.longMetric("failed", "Number of failed requests");
    }

    /**
     * Initialize metrics.
     * @param ctx Kernal context.
     */
    public static void initMetrics(GridKernalContext ctx) {
        ClientListenerSessionMetricTracker metrics = new ClientListenerSessionMetricTracker(ctx);

        metrics.onHandshakeReceived(ODBC_CLIENT);
        metrics.onHandshakeReceived(JDBC_CLIENT);
        metrics.onHandshakeReceived(THIN_CLIENT);
    }

    /**
     * Handle connection.
     */
    public void onSessionEstablished() {
        waiting.increment();
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
     * Handle session close.
     */
    public void onSessionClosed() {
        if (established) {
            active.decrement();
            closed.increment();
        } else {
            waiting.decrement();
            rejected.increment();
        }
    }

    /**
     * Get metric namespace from client type.
     * @param clientType Client type.
     * @return Metric namespace for client.
     */
    private static String clientTypeToMetricNamespace(byte clientType) {
        switch (clientType) {
            case ODBC_CLIENT:
                return "odbc";

            case JDBC_CLIENT:
                return "jdbc";

            case THIN_CLIENT:
                return "thin";

            default:
                throw new IgniteException("Unknown client type: " + clientType);
        }
    }
}
