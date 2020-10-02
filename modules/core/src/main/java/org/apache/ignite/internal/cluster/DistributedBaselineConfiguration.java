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

package org.apache.ignite.internal.cluster;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.configuration.distributed.DistributePropertyListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationLifecycleListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.NotNull;

import static java.lang.String.format;
import static org.apache.ignite.internal.IgniteFeatures.BASELINE_AUTO_ADJUSTMENT;
import static org.apache.ignite.internal.IgniteFeatures.allNodesSupport;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_AUTO_ADJUST_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_DISTRIBUTED_META_STORAGE_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.isFeatureEnabled;
import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.makeUpdateListener;
import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.setDefaultValue;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty.detachedBooleanProperty;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedLongProperty.detachedLongProperty;

/**
 * Distributed baseline configuration.
 */
public class DistributedBaselineConfiguration {
    /** Default auto-adjust timeout for persistence grid. */
    private static final int DEFAULT_PERSISTENCE_TIMEOUT = 5 * 60_000;

    /** Default auto-adjust timeout for in-memory grid. */
    private static final int DEFAULT_IN_MEMORY_TIMEOUT = 0;

    /** Message of baseline auto-adjust configuration. */
    private static final String AUTO_ADJUST_CONFIGURED_MESSAGE = "Baseline auto-adjust is '%s' with timeout='%d' ms";

    /** Message of baseline auto-adjust parameter was changed. */
    private static final String PROPERTY_UPDATE_MESSAGE =
        "Baseline parameter '%s' was changed from '%s' to '%s'";

    /** */
    private volatile long dfltTimeout;

    /** Default auto-adjust enable/disable. */
    private volatile boolean dfltEnabled;

    /** */
    private final IgniteLogger log;

    /** Value of manual baseline control or auto adjusting baseline. */
    private final DistributedChangeableProperty<Boolean> baselineAutoAdjustEnabled =
        detachedBooleanProperty("baselineAutoAdjustEnabled");

    /**
     * Value of time which we would wait before the actual topology change since last discovery event(node join/exit).
     */
    private final DistributedChangeableProperty<Long> baselineAutoAdjustTimeout =
        detachedLongProperty("baselineAutoAdjustTimeout");

    /** Persistence enabled flag. */
    final boolean persistenceEnabled;

    /**
     * @param isp Subscription processor.
     * @param ctx Kernal context.
     */
    public DistributedBaselineConfiguration(
        GridInternalSubscriptionProcessor isp,
        GridKernalContext ctx,
        IgniteLogger log
    ) {
        this.log = log;

        if (isFeatureEnabled(IGNITE_BASELINE_AUTO_ADJUST_FEATURE) && (
            !isFeatureEnabled(IGNITE_DISTRIBUTED_META_STORAGE_FEATURE)
                || !isFeatureEnabled(IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE)
        ))
            throw new IllegalArgumentException(
                IGNITE_BASELINE_AUTO_ADJUST_FEATURE + " depends on "
                    + IGNITE_DISTRIBUTED_META_STORAGE_FEATURE + " and "
                    + IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE
                    + " so please keep all of them in same state");

        persistenceEnabled = ctx.config() != null && CU.isPersistenceEnabled(ctx.config());

        dfltTimeout = persistenceEnabled ? DEFAULT_PERSISTENCE_TIMEOUT : DEFAULT_IN_MEMORY_TIMEOUT;
        dfltEnabled = false;
        boolean serverMode = !ctx.config().isClientMode();

        isp.registerDistributedConfigurationListener(
            new DistributedConfigurationLifecycleListener() {
                @Override public void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
                    baselineAutoAdjustEnabled.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));
                    baselineAutoAdjustTimeout.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));

                    dispatcher.registerProperties(baselineAutoAdjustEnabled, baselineAutoAdjustTimeout);
                }

                @Override public void onReadyToWrite() {
                    if (isFeatureEnabled(IGNITE_BASELINE_AUTO_ADJUST_FEATURE) &&
                        allNodesSupport(ctx, BASELINE_AUTO_ADJUSTMENT) && serverMode) {
                        initDfltAutoAdjustVars(ctx);
                        setDefaultValue(baselineAutoAdjustEnabled, dfltEnabled, log);
                        setDefaultValue(baselineAutoAdjustTimeout, dfltTimeout, log);
                    }
                }
            }
        );
    }

    /**
     * @param ctx Context.
     */
    public void initDfltAutoAdjustVars(GridKernalContext ctx) {
        if (isFeatureEnabled(IGNITE_BASELINE_AUTO_ADJUST_FEATURE) &&
            allNodesSupport(ctx, BASELINE_AUTO_ADJUSTMENT)) {
            dfltTimeout = persistenceEnabled ? DEFAULT_PERSISTENCE_TIMEOUT : DEFAULT_IN_MEMORY_TIMEOUT;
            dfltEnabled = isFeatureEnabled(IGNITE_BASELINE_AUTO_ADJUST_FEATURE) && !persistenceEnabled;
        }
    }

    /** */
    public void listenAutoAdjustEnabled(DistributePropertyListener<? super Boolean> lsnr) {
        baselineAutoAdjustEnabled.addListener(lsnr);
    }

    /** */
    public void listenAutoAdjustTimeout(DistributePropertyListener<? super Long> lsnr) {
        baselineAutoAdjustTimeout.addListener(lsnr);
    }

    /**
     * Called when cluster performing activation.
     */
    public void onActivate() throws IgniteCheckedException {
        if (!isFeatureEnabled(IGNITE_BASELINE_AUTO_ADJUST_FEATURE))
            return;

        if (log.isInfoEnabled())
            log.info(format(AUTO_ADJUST_CONFIGURED_MESSAGE,
                (isBaselineAutoAdjustEnabled() ? "enabled" : "disabled"),
                getBaselineAutoAdjustTimeout()
            ));
    }

    /**
     * @return Value of manual baseline control or auto adjusting baseline.
     */
    public boolean isBaselineAutoAdjustEnabled() {
        return baselineAutoAdjustEnabled.getOrDefault(dfltEnabled);
    }

    /**
     * @param baselineAutoAdjustEnabled Value of manual baseline control or auto adjusting baseline.
     * @throws IgniteCheckedException if failed.
     */
    public GridFutureAdapter<?> updateBaselineAutoAdjustEnabledAsync(GridKernalContext ctx, boolean baselineAutoAdjustEnabled)
        throws IgniteCheckedException {
        if (!isFeatureEnabled(IGNITE_BASELINE_AUTO_ADJUST_FEATURE))
            return finishFuture();

        if (!allNodesSupport(ctx, BASELINE_AUTO_ADJUSTMENT))
            throw new IgniteCheckedException("Not all nodes in the cluster support baseline auto-adjust.");

        return this.baselineAutoAdjustEnabled.propagateAsync(!baselineAutoAdjustEnabled, baselineAutoAdjustEnabled);
    }

    /**
     * @return Value of time which we would wait before the actual topology change since last discovery event(node
     * join/exit).
     */
    public long getBaselineAutoAdjustTimeout() {
        return baselineAutoAdjustTimeout.getOrDefault(dfltTimeout);
    }

    /**
     * @param baselineAutoAdjustTimeout Value of time which we would wait before the actual topology change since last
     * discovery event(node join/exit).
     * @throws IgniteCheckedException If failed.
     */
    public GridFutureAdapter<?> updateBaselineAutoAdjustTimeoutAsync(GridKernalContext ctx,
        long baselineAutoAdjustTimeout) throws IgniteCheckedException {
        if (!isFeatureEnabled(IGNITE_BASELINE_AUTO_ADJUST_FEATURE))
            return finishFuture();

        if (!allNodesSupport(ctx, BASELINE_AUTO_ADJUSTMENT))
            throw new IgniteCheckedException("Not all nodes in the cluster support baseline auto-adjust.");

        return this.baselineAutoAdjustTimeout.propagateAsync(baselineAutoAdjustTimeout);
    }

    /**
     * @return Finished future.
     */
    @NotNull private GridFutureAdapter<?> finishFuture() {
        GridFutureAdapter<Object> adapter = new GridFutureAdapter<>();

        adapter.onDone();

        return adapter;
    }
}
