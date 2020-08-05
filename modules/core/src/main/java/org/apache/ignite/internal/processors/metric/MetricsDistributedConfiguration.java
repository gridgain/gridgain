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
package org.apache.ignite.internal.processors.metric;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedLongProperty;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;

import static java.lang.String.format;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MESSAGE_STATS_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_STAT_TOO_LONG_PROCESSING;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_STAT_TOO_LONG_WAITING;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty.detachedBooleanProperty;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedLongProperty.detachedLongProperty;
import static org.apache.ignite.internal.util.IgniteUtils.checkRange;

/**
 * Metrics distributed configuration.
 */
public class MetricsDistributedConfiguration {
    /** */
    private final boolean defaultDiagnosticMessageStatsEnabled = getBoolean(IGNITE_MESSAGE_STATS_ENABLED, true);

    /** */
    private final long defaultDiagnosticMessageStatsTooLongProcessing =
            checkRange(0, Long.MAX_VALUE, getLong(IGNITE_STAT_TOO_LONG_PROCESSING, 250), IGNITE_STAT_TOO_LONG_PROCESSING);

    /** */
    private final long defaultDiagnosticMessageStatsTooLongWaiting =
            checkRange(0, Long.MAX_VALUE, getLong(IGNITE_STAT_TOO_LONG_WAITING, 250), IGNITE_STAT_TOO_LONG_WAITING);

    /** */
    private final IgniteLogger log;

    /**
     * Whether diagnostics of messages is enabled.
     */
    private final DistributedBooleanProperty diagnosticMessageStatsEnabled = detachedBooleanProperty("diagnosticMessageStatsEnabled");

    /**
     * Long message processing threshold.
     */
    private final DistributedLongProperty diagnosticMessageStatTooLongProcessing = detachedLongProperty("diagnosticMessageStatTooLongProcessing");

    /**
     * Long message queue waiting threshold.
     */
    private final DistributedLongProperty diagnosticMessageStatTooLongWaiting = detachedLongProperty("diagnosticMessageStatTooLongWaiting");

    /** */
    public MetricsDistributedConfiguration(GridInternalSubscriptionProcessor subscriptionProcessor, IgniteLogger log) {
        this.log = log;

        subscriptionProcessor.registerDistributedConfigurationListener(dispatcher -> {
            diagnosticMessageStatsEnabled.addListener(this::updateListener);
            diagnosticMessageStatTooLongProcessing.addListener(this::updateListener);
            diagnosticMessageStatTooLongWaiting.addListener(this::updateListener);

            dispatcher.registerProperty(diagnosticMessageStatsEnabled);
            dispatcher.registerProperty(diagnosticMessageStatTooLongProcessing);
            dispatcher.registerProperty(diagnosticMessageStatTooLongWaiting);
        });
    }

    /** */
    private <T> void updateListener(String key, T oldVal, T newVal) {
        log.info(format("Metric distributed property '%s' was changed, oldVal: '%s', newVal: '%s'", key, oldVal, newVal));
    }

    /** */
    public boolean diagnosticMessageStatsEnabled() {
        return diagnosticMessageStatsEnabled.getOrDefault(defaultDiagnosticMessageStatsEnabled);
    }

    /** */
    public void diagnosticMessageStatsEnabled(boolean diagnosticMessageStatsEnabled) {
        try {
            this.diagnosticMessageStatsEnabled.propagate(diagnosticMessageStatsEnabled);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    public long diagnosticMessageStatTooLongProcessing() {
        return diagnosticMessageStatTooLongProcessing.getOrDefault(defaultDiagnosticMessageStatsTooLongProcessing);
    }

    /** */
    public void diagnosticMessageStatTooLongProcessing(long diagnosticMessageStatTooLongProcessing) {
        try {
            this.diagnosticMessageStatTooLongProcessing.propagate(
                checkRange(0, Long.MAX_VALUE, diagnosticMessageStatTooLongProcessing, IGNITE_STAT_TOO_LONG_PROCESSING)
            );
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    public long diagnosticMessageStatTooLongWaiting() {
        return diagnosticMessageStatTooLongWaiting.getOrDefault(defaultDiagnosticMessageStatsTooLongWaiting);
    }

    /** */
    public void diagnosticMessageStatTooLongWaiting(long diagnosticMessageStatTooLongWaiting) {
        try {
            this.diagnosticMessageStatTooLongWaiting.propagate(
                checkRange(0, Long.MAX_VALUE, diagnosticMessageStatTooLongWaiting, IGNITE_STAT_TOO_LONG_WAITING)
            );
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
