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
import static org.apache.ignite.internal.IgniteFeatures.BASELINE_SEPARATE_AUTO_ADJUSTMENT;
import static org.apache.ignite.internal.IgniteFeatures.allNodesSupport;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_AUTO_ADJUST_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_DISTRIBUTED_META_STORAGE_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE;
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

    /** Message of baseline auto-adjust configuration for separate scale up and down scenarios. */
    private static final String SEPARATE_AUTO_ADJUST_CONFIGURED_MESSAGE = "Separate baseline auto-adjust feature is enabled " +
        "scaleUp is '%s' with timeout='%d' ms and scaleDown is '%s' with timeout='%d' ms";

    /** Message of baseline auto-adjust parameter was changed. */
    private static final String PROPERTY_UPDATE_MESSAGE =
        "Baseline parameter '%s' was changed from '%s' to '%s'";

    /** */
    private volatile long dfltTimeout;

    /** Default auto-adjust enable/disable. */
    private volatile boolean dfltEnabled;

    /** */
    private volatile long dfltScaleUpTimeout;

    /** Default scale up auto-adjust enable/disable. */
    private volatile boolean dfltScaleUpEnabled;

    /** */
    private volatile long dfltScaleDownTimeout;

    /** Default scale down auto-adjust enable/disable. */
    private volatile boolean dfltScaleDownEnabled;

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

    /** Value of manual baseline control or auto adjusting baseline for scale up scenario. */
    private final DistributedChangeableProperty<Boolean> baselineScaleUpAutoAdjustEnabled =
        detachedBooleanProperty("baselineScaleUpAutoAdjustEnabled");

    /**
     * Value of time which we would wait before the actual topology change since last discovery event(node join).
     */
    private final DistributedChangeableProperty<Long> baselineScaleUpAutoAdjustTimeout =
        detachedLongProperty("baselineScaleUpAutoAdjustTimeout");

    /** Value of manual baseline control or auto adjusting baseline for scale down scenario. */
    private final DistributedChangeableProperty<Boolean> baselineScaleDownAutoAdjustEnabled =
        detachedBooleanProperty("baselineScaleDownAutoAdjustEnabled");

    /**
     * Value of time which we would wait before the actual topology change since last discovery event(node exit).
     */
    private final DistributedChangeableProperty<Long> baselineScaleDownAutoAdjustTimeout =
        detachedLongProperty("baselineScaleDownAutoAdjustTimeout");

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

        if (isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE)
            && !isFeatureEnabled(IGNITE_BASELINE_AUTO_ADJUST_FEATURE))
            throw new IllegalArgumentException(
                IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE + " depends on "
                    + IGNITE_BASELINE_AUTO_ADJUST_FEATURE
                    + " so please keep all of them in same state");

        persistenceEnabled = ctx.config() != null && CU.isPersistenceEnabled(ctx.config());

        dfltTimeout = persistenceEnabled ? DEFAULT_PERSISTENCE_TIMEOUT : DEFAULT_IN_MEMORY_TIMEOUT;
        dfltEnabled = false;

        dfltScaleUpEnabled = dfltEnabled;
        dfltScaleUpTimeout = dfltTimeout;

        dfltScaleDownEnabled = dfltEnabled;
        dfltScaleDownTimeout = dfltTimeout;

        boolean serverMode = !ctx.config().isClientMode();

        isp.registerDistributedConfigurationListener(
            new DistributedConfigurationLifecycleListener() {
                @Override public void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
                    baselineAutoAdjustEnabled.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));
                    baselineAutoAdjustTimeout.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));

                    baselineScaleUpAutoAdjustEnabled.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));
                    baselineScaleUpAutoAdjustTimeout.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));

                    baselineScaleDownAutoAdjustEnabled.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));
                    baselineScaleDownAutoAdjustTimeout.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));

                    dispatcher.registerProperties(
                        baselineAutoAdjustEnabled,
                        baselineAutoAdjustTimeout,

                        baselineScaleUpAutoAdjustEnabled,
                        baselineScaleUpAutoAdjustTimeout,

                        baselineScaleDownAutoAdjustEnabled,
                        baselineScaleDownAutoAdjustTimeout
                    );
                }

                @Override public void onReadyToWrite() {
                    if (isFeatureEnabled(IGNITE_BASELINE_AUTO_ADJUST_FEATURE) &&
                        allNodesSupport(ctx, BASELINE_AUTO_ADJUSTMENT) && serverMode) {
                        initDfltAutoAdjustVars(ctx);

                        setDefaultValue(baselineAutoAdjustEnabled, dfltEnabled, log);
                        setDefaultValue(baselineAutoAdjustTimeout, dfltTimeout, log);

                        setDefaultValue(baselineScaleUpAutoAdjustEnabled, dfltScaleUpEnabled, log);
                        setDefaultValue(baselineScaleUpAutoAdjustTimeout, dfltScaleUpTimeout, log);

                        setDefaultValue(baselineScaleDownAutoAdjustEnabled, dfltScaleDownEnabled, log);
                        setDefaultValue(baselineScaleDownAutoAdjustTimeout, dfltScaleDownTimeout, log);
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
            dfltEnabled = !persistenceEnabled;
        }

        if (isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE)
            && allNodesSupport(ctx, BASELINE_SEPARATE_AUTO_ADJUSTMENT)) {
            dfltScaleUpEnabled = dfltEnabled;
            dfltScaleUpTimeout = dfltTimeout;

            dfltScaleDownEnabled = dfltEnabled;
            dfltScaleDownTimeout = dfltTimeout;
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

    /** */
    public void listenAutoAdjustScaleUpEnabled(DistributePropertyListener<? super Boolean> lsnr) {
        baselineScaleUpAutoAdjustEnabled.addListener(lsnr);
    }

    /** */
    public void listenAutoAdjustScaleUpTimeout(DistributePropertyListener<? super Long> lsnr) {
        baselineScaleUpAutoAdjustTimeout.addListener(lsnr);
    }

    /** */
    public void listenAutoAdjustScaleDownEnabled(DistributePropertyListener<? super Boolean> lsnr) {
        baselineScaleDownAutoAdjustEnabled.addListener(lsnr);
    }

    /** */
    public void listenAutoAdjustScaleDownTimeout(DistributePropertyListener<? super Long> lsnr) {
        baselineScaleDownAutoAdjustTimeout.addListener(lsnr);
    }

    /**
     * Called when cluster performing activation.
     */
    public void onActivate() throws IgniteCheckedException {
        if (isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE) && log.isInfoEnabled())
            log.info(format(SEPARATE_AUTO_ADJUST_CONFIGURED_MESSAGE,
                (isBaselineScaleUpAutoAdjustEnabled() ? "enabled" : "disabled"),
                getBaselineScaleUpAutoAdjustTimeout(),
                (isBaselineScaleDownAutoAdjustEnabled() ? "enabled" : "disabled"),
                getBaselineScaleDownAutoAdjustTimeout()
            ));

        if (isFeatureEnabled(IGNITE_BASELINE_AUTO_ADJUST_FEATURE) && log.isInfoEnabled())
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
     * @return Value of manual baseline control or auto adjusting baseline on scale up.
     */
    public boolean isBaselineScaleUpAutoAdjustEnabled() {
        return baselineAutoAdjustEnabled.getOrDefault(dfltEnabled)
            && baselineScaleUpAutoAdjustEnabled.getOrDefault(dfltScaleUpEnabled);
    }

    /**
     * @return Value of manual baseline control or auto adjusting baseline on scale down.
     */
    public boolean isBaselineScaleDownAutoAdjustEnabled() {
        return baselineAutoAdjustEnabled.getOrDefault(dfltEnabled)
            && baselineScaleDownAutoAdjustEnabled.getOrDefault(dfltScaleDownEnabled);
    }

    /**
     * @param baselineAutoAdjustEnabled Value of manual baseline control or auto adjusting baseline.
     * @throws IgniteCheckedException if failed.
     */
    public GridFutureAdapter<?> updateBaselineAutoAdjustEnabledAsync(GridKernalContext ctx,
        boolean baselineAutoAdjustEnabled) throws IgniteCheckedException {
        if (!isFeatureEnabled(IGNITE_BASELINE_AUTO_ADJUST_FEATURE))
            return finishFuture();

        if (!allNodesSupport(ctx, BASELINE_AUTO_ADJUSTMENT))
            throw new IgniteCheckedException("Not all nodes in the cluster support baseline auto-adjust.");

        return this.baselineAutoAdjustEnabled.propagateAsync(!baselineAutoAdjustEnabled, baselineAutoAdjustEnabled);
    }

    /**
     * @param baselineScaleUpAutoAdjustEnabled Value of manual baseline control or auto adjusting baseline on scale up.
     * @throws IgniteCheckedException if failed.
     */
    public GridFutureAdapter<?> updateBaselineScaleUpAutoAdjustEnabledAsync(GridKernalContext ctx,
        boolean baselineScaleUpAutoAdjustEnabled) throws IgniteCheckedException {
        if (!isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE))
            return finishFuture();

        if (!allNodesSupport(ctx, BASELINE_SEPARATE_AUTO_ADJUSTMENT))
            throw new IgniteCheckedException("Not all nodes in the cluster support baseline auto-adjust on scale up.");

        return this.baselineScaleUpAutoAdjustEnabled.propagateAsync(!baselineScaleUpAutoAdjustEnabled, baselineScaleUpAutoAdjustEnabled);
    }

    /**
     * @param baselineScaleDownAutoAdjustEnabled Value of manual baseline control or auto adjusting baseline on scale down.
     * @throws IgniteCheckedException if failed.
     */
    public GridFutureAdapter<?> updateBaselineScaleDownAutoAdjustEnabledAsync(GridKernalContext ctx,
        boolean baselineScaleDownAutoAdjustEnabled) throws IgniteCheckedException {
        if (!isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE))
            return finishFuture();

        if (!allNodesSupport(ctx, BASELINE_SEPARATE_AUTO_ADJUSTMENT))
            throw new IgniteCheckedException("Not all nodes in the cluster support baseline auto-adjust on scale down.");

        return this.baselineScaleDownAutoAdjustEnabled.propagateAsync(!baselineScaleDownAutoAdjustEnabled, baselineScaleDownAutoAdjustEnabled);
    }

    /**
     * @return Value of time which we would wait before the actual topology change since last discovery event(node
     * join/exit).
     */
    public long getBaselineAutoAdjustTimeout() {
        return baselineAutoAdjustTimeout.getOrDefault(dfltTimeout);
    }

    /**
     * @return Value of time which we would wait before the actual topology change since last discovery event(node
     * join).
     */
    public long getBaselineScaleUpAutoAdjustTimeout() {
        return baselineScaleUpAutoAdjustTimeout.getOrDefault(dfltScaleUpTimeout);
    }

    /**
     * @return Value of time which we would wait before the actual topology change since last discovery event(node
     * exit).
     */
    public long getBaselineScaleDownAutoAdjustTimeout() {
        return baselineScaleDownAutoAdjustTimeout.getOrDefault(dfltScaleDownTimeout);
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
     * @param baselineScaleUpAutoAdjustTimeout Value of time which we would wait before the actual topology change since last
     * discovery event(node join).
     * @throws IgniteCheckedException If failed.
     */
    public GridFutureAdapter<?> updateBaselineScaleUpAutoAdjustTimeoutAsync(GridKernalContext ctx,
        long baselineScaleUpAutoAdjustTimeout) throws IgniteCheckedException {
        if (!isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE))
            return finishFuture();

        if (!allNodesSupport(ctx, BASELINE_SEPARATE_AUTO_ADJUSTMENT))
            throw new IgniteCheckedException("Not all nodes in the cluster support baseline auto-adjust on scale up.");

        return this.baselineScaleUpAutoAdjustTimeout.propagateAsync(baselineScaleUpAutoAdjustTimeout);
    }

    /**
     * @param baselineScaleDownAutoAdjustTimeout Value of time which we would wait before the actual topology change since last
     * discovery event(node exit).
     * @throws IgniteCheckedException If failed.
     */
    public GridFutureAdapter<?> updateBaselineScaleDownAutoAdjustTimeoutAsync(GridKernalContext ctx,
        long baselineScaleDownAutoAdjustTimeout) throws IgniteCheckedException {
        if (!isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE))
            return finishFuture();

        if (!allNodesSupport(ctx, BASELINE_SEPARATE_AUTO_ADJUSTMENT))
            throw new IgniteCheckedException("Not all nodes in the cluster support baseline auto-adjust on scale down.");

        return this.baselineScaleDownAutoAdjustTimeout.propagateAsync(baselineScaleDownAutoAdjustTimeout);
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
