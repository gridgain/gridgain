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
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.configuration.distributed.DistributePropertyListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationLifecycleListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.NotNull;

import static java.lang.String.format;
import static org.apache.ignite.internal.IgniteFeatures.BASELINE_AUTO_ADJUSTMENT;
import static org.apache.ignite.internal.IgniteFeatures.SEPARATE_BASELINE_AUTO_ADJUSTMENT;
import static org.apache.ignite.internal.IgniteFeatures.allNodesSupport;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_AUTO_ADJUST_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_DISTRIBUTED_META_STORAGE_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.isFeatureEnabled;
import static org.apache.ignite.internal.cluster.AutoAdjustMode.SCALE_DOWN;
import static org.apache.ignite.internal.cluster.AutoAdjustMode.SCALE_UP;
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

    /** Default scale down auto-adjust timeout for persistence grid. */
    private static final long DEFAULT_SCALE_DOWN_PERSISTENCE_TIMEOUT = Long.MAX_VALUE;

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
            && allNodesSupport(ctx, SEPARATE_BASELINE_AUTO_ADJUSTMENT)) {
            dfltScaleUpEnabled = dfltEnabled;
            dfltScaleUpTimeout = dfltTimeout;

            dfltScaleDownEnabled = dfltEnabled;
            dfltScaleDownTimeout = dfltTimeout;
        }
    }

    /** */
    @Deprecated
    public void listenAutoAdjustEnabled(DistributePropertyListener<? super Boolean> lsnr) {
        if (isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE)) {
            baselineScaleUpAutoAdjustEnabled.addListener(lsnr);
            baselineScaleDownAutoAdjustEnabled.addListener(lsnr);
        }
        else
            baselineAutoAdjustEnabled.addListener(lsnr);
    }

    /** */
    @Deprecated
    public void listenAutoAdjustTimeout(DistributePropertyListener<? super Long> lsnr) {
        if (isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE)) {
            baselineScaleUpAutoAdjustTimeout.addListener(lsnr);
            baselineScaleDownAutoAdjustTimeout.addListener(lsnr);
        }
        else
            baselineAutoAdjustTimeout.addListener(lsnr);
    }

    /** */
    public void listenAutoAdjustEnabled(AutoAdjustMode mode, DistributePropertyListener<? super Boolean> lsnr) {
        switch (mode) {
            case SCALE_UP:
                baselineScaleUpAutoAdjustEnabled.addListener(lsnr);
                break;
            case SCALE_DOWN:
                baselineScaleDownAutoAdjustEnabled.addListener(lsnr);
                break;
            default:
                throw new IgniteException("Unsupported auto-adjustment mode: " + mode);
        }
    }

    /** */
    public void listenAutoAdjustTimeout(AutoAdjustMode mode, DistributePropertyListener<? super Long> lsnr) {
        switch (mode) {
            case SCALE_UP:
                baselineScaleUpAutoAdjustTimeout.addListener(lsnr);
                break;
            case SCALE_DOWN:
                baselineScaleDownAutoAdjustTimeout.addListener(lsnr);
                break;
            default:
                throw new IgniteException("Unsupported auto-adjustment mode: " + mode);
        }
    }

    /**
     * Called when cluster performing activation.
     */
    public void onActivate() throws IgniteCheckedException {
        if (isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE) && log.isInfoEnabled())
            log.info(format(SEPARATE_AUTO_ADJUST_CONFIGURED_MESSAGE,
                (isBaselineAutoAdjustEnabled(SCALE_UP) ? "enabled" : "disabled"),
                getBaselineAutoAdjustTimeout(SCALE_UP),
                (isBaselineAutoAdjustEnabled(SCALE_DOWN) ? "enabled" : "disabled"),
                getBaselineAutoAdjustTimeout(SCALE_DOWN)
            ));
        else if (isFeatureEnabled(IGNITE_BASELINE_AUTO_ADJUST_FEATURE) && log.isInfoEnabled())
            log.info(format(AUTO_ADJUST_CONFIGURED_MESSAGE,
                (isBaselineAutoAdjustEnabled() ? "enabled" : "disabled"),
                getBaselineAutoAdjustTimeout()
            ));
    }

    /**
     * @return Value of manual baseline control or auto adjusting baseline.
     */
    @Deprecated
    public boolean isBaselineAutoAdjustEnabled() {
        if (isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE))
            return isBaselineAutoAdjustEnabled(SCALE_UP) ||
                isBaselineAutoAdjustEnabled(SCALE_DOWN);

        return baselineAutoAdjustEnabled.getOrDefault(dfltEnabled);
    }

    /**
     * @param scaleUp If {@code true}, the scale up's baseline auto adjust status will be returned,
     *                if {@code false} - scale down's.
     * @return Value of manual baseline control or auto adjusting baseline.
     */
    public boolean isBaselineAutoAdjustEnabled(AutoAdjustMode mode) {
        switch (mode) {
            case SCALE_UP:
                return baselineScaleUpAutoAdjustEnabled.getOrDefault(dfltScaleUpEnabled);
            case SCALE_DOWN:
                return baselineScaleDownAutoAdjustEnabled.getOrDefault(dfltScaleDownEnabled);
            default:
                throw new IgniteException("Unsupported auto-adjustment mode: " + mode);
        }
    }

    /**
     * @param baselineAutoAdjustEnabled Value of manual baseline control or auto adjusting baseline.
     * @throws IgniteCheckedException if failed.
     * @deprecated Use {@link DistributedBaselineConfiguration#updateBaselineAutoAdjustEnabledAsync} with
     *             SCALE_UP/SCALE_DOWN instead.
     */
    @Deprecated
    public GridFutureAdapter<?> updateBaselineAutoAdjustEnabledAsync(GridKernalContext ctx,
                                                                     boolean baselineAutoAdjustEnabled) throws IgniteCheckedException {
        if (isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE)) {
            GridCompoundFuture<Object, Object> fut = new GridCompoundFuture<>();
            fut.add((IgniteInternalFuture<Object>) updateBaselineAutoAdjustEnabledAsync(SCALE_UP, ctx, baselineAutoAdjustEnabled));
            fut.add((IgniteInternalFuture<Object>) updateBaselineAutoAdjustEnabledAsync(SCALE_DOWN, ctx, baselineAutoAdjustEnabled));
            fut.markInitialized();

            return fut;
        }

        if (!isFeatureEnabled(IGNITE_BASELINE_AUTO_ADJUST_FEATURE))
            return finishFuture();

        if (!allNodesSupport(ctx, BASELINE_AUTO_ADJUSTMENT))
            throw new IgniteCheckedException("Not all nodes in the cluster support baseline auto-adjust.");

        return this.baselineAutoAdjustEnabled.propagateAsync(!baselineAutoAdjustEnabled, baselineAutoAdjustEnabled);
    }

    /**
     * @param scaleUp If {@code true}, the scale up's baseline auto adjust enable flag will be updated,
     *                if {@code false} - scale down's.
     * @param baselineAutoAdjustEnabled Value of manual baseline control or auto adjusting baseline.
     * @throws IgniteCheckedException if failed.
     */
    public GridFutureAdapter<?> updateBaselineAutoAdjustEnabledAsync(AutoAdjustMode mode, GridKernalContext ctx,
                                                                     boolean baselineAutoAdjustEnabled) throws IgniteCheckedException {
        if (!isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE))
            return finishFuture();

        if (!allNodesSupport(ctx, SEPARATE_BASELINE_AUTO_ADJUSTMENT))
            throw new IgniteCheckedException("Not all nodes in the cluster support separate baseline auto-adjust.");

        switch (mode) {
            case SCALE_UP:
                return baselineScaleUpAutoAdjustEnabled.propagateAsync(!baselineAutoAdjustEnabled, baselineAutoAdjustEnabled);
            case SCALE_DOWN:
                return baselineScaleDownAutoAdjustEnabled.propagateAsync(!baselineAutoAdjustEnabled, baselineAutoAdjustEnabled);
            default:
                throw new IgniteException("Unsupported auto-adjustment mode: " + mode);
        }
    }

    /**
     * @return Value of time which we would wait before the actual topology change since last discovery event(node
     *         join/exit).
     * @deprecated
     */
    @Deprecated
    public long getBaselineAutoAdjustTimeout() {
        if (isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE))
            return baselineScaleUpAutoAdjustTimeout.getOrDefault(dfltScaleUpTimeout);

        return baselineAutoAdjustTimeout.getOrDefault(dfltTimeout);
    }

    /**
     * @param scaleUp If {@code true}, the scale up's baseline auto adjust timeout will be returned,
     *                if {@code false} - scale down's.
     * @return Value of time which we would wait before the actual topology change since last discovery event(node
     *         join/exit).
     */
    public long getBaselineAutoAdjustTimeout(AutoAdjustMode mode) {
        switch (mode) {
            case SCALE_UP:
                return baselineScaleUpAutoAdjustTimeout.getOrDefault(dfltScaleUpTimeout);
            case SCALE_DOWN:
                return baselineScaleDownAutoAdjustTimeout.getOrDefault(dfltScaleDownTimeout);
            default:
                throw new IgniteException("Unsupported auto-adjustment mode: " + mode);
        }
    }

    /**
     * @param baselineAutoAdjustTimeout Value of time which we would wait before the actual topology change since last
     * discovery event(node join/exit).
     * @throws IgniteCheckedException If failed.
     */
    @Deprecated
    public GridFutureAdapter<?> updateBaselineAutoAdjustTimeoutAsync(GridKernalContext ctx,
                                                                     long baselineAutoAdjustTimeout) throws IgniteCheckedException {
        if (isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE)) {
            GridCompoundFuture<Object, Object> fut = new GridCompoundFuture<>();
            fut.add((IgniteInternalFuture<Object>) updateBaselineAutoAdjustTimeoutAsync(SCALE_UP, ctx, baselineAutoAdjustTimeout));
            fut.add((IgniteInternalFuture<Object>) updateBaselineAutoAdjustTimeoutAsync(SCALE_DOWN, ctx, baselineAutoAdjustTimeout));
            fut.markInitialized();

            return fut;
        }

        if (!isFeatureEnabled(IGNITE_BASELINE_AUTO_ADJUST_FEATURE))
            return finishFuture();

        if (!allNodesSupport(ctx, BASELINE_AUTO_ADJUSTMENT))
            throw new IgniteCheckedException("Not all nodes in the cluster support baseline auto-adjust.");

        return this.baselineAutoAdjustTimeout.propagateAsync(baselineAutoAdjustTimeout);
    }

    /**
     * @param scaleUp If {@code true}, the scale up's baseline auto adjust timeout will be updated, if {@code false} - scale down's.
     * @param baselineAutoAdjustTimeout Value of time which we would wait before the actual topology change since last
     *                                  discovery event(node join/exit).
     * @throws IgniteCheckedException If failed.
     */
    public GridFutureAdapter<?> updateBaselineAutoAdjustTimeoutAsync(AutoAdjustMode mode, GridKernalContext ctx,
                                                                     long baselineAutoAdjustTimeout) throws IgniteCheckedException {
        if (!isFeatureEnabled(IGNITE_BASELINE_AUTO_ADJUST_FEATURE))
            return finishFuture();

        if (!allNodesSupport(ctx, SEPARATE_BASELINE_AUTO_ADJUSTMENT))
            throw new IgniteCheckedException("Not all nodes in the cluster support separate baseline auto-adjust.");

        switch (mode) {
            case SCALE_UP:
                return baselineScaleUpAutoAdjustTimeout.propagateAsync(baselineAutoAdjustTimeout);
            case SCALE_DOWN:
                return baselineScaleDownAutoAdjustTimeout.propagateAsync(baselineAutoAdjustTimeout);
            default:
                throw new IgniteException("Unsupported auto-adjustment mode: " + mode);
        }
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
