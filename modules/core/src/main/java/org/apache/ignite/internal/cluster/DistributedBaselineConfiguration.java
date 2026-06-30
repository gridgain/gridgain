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

import org.apache.ignite.AutoAdjustMode;
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
import static org.apache.ignite.AutoAdjustMode.SCALE_DOWN;
import static org.apache.ignite.AutoAdjustMode.SCALE_UP;
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

    /** The default auto-adjust enable/disable. */
    private volatile boolean dfltEnabled;

    /** */
    private volatile long dfltScaleUpTimeout;

    /** The default scale up auto-adjust enable/disable. */
    private volatile boolean dfltScaleUpEnabled;

    /** */
    private volatile long dfltScaleDownTimeout;

    /** The default scale down auto-adjust enable/disable. */
    private volatile boolean dfltScaleDownEnabled;

    /** */
    private final IgniteLogger log;

    /** The value of the manual baseline control or the baseline auto-adjust. */
    private final DistributedChangeableProperty<Boolean> baselineAutoAdjustEnabled =
        detachedBooleanProperty("baselineAutoAdjustEnabled");

    /**
     * The value of time which it will wait before the actual topology change since last discovery event (node join/exit).
     */
    private final DistributedChangeableProperty<Long> baselineAutoAdjustTimeout =
        detachedLongProperty("baselineAutoAdjustTimeout");

    /** The value of the manual baseline control or the baseline auto-adjust for the scale up. */
    private final DistributedChangeableProperty<Boolean> baselineScaleUpAutoAdjustEnabled =
        detachedBooleanProperty("baselineScaleUpAutoAdjustEnabled");

    /**
     * The value of time which it will wait before the actual topology change since last discovery event (node join).
     */
    private final DistributedChangeableProperty<Long> baselineScaleUpAutoAdjustTimeout =
        detachedLongProperty("baselineScaleUpAutoAdjustTimeout");

    /** The value of the manual baseline control or the baseline auto-adjust for the scale down. */
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

        if (isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE) && (
            !isFeatureEnabled(IGNITE_DISTRIBUTED_META_STORAGE_FEATURE)
                || !isFeatureEnabled(IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE)
        ))
            throw new IllegalArgumentException(
                IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE + " depends on "
                    + IGNITE_DISTRIBUTED_META_STORAGE_FEATURE + " and "
                    + IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE
                    + " so please keep all of them in same state");

        persistenceEnabled = ctx.config() != null && CU.isPersistenceEnabled(ctx.config());

        dfltTimeout = persistenceEnabled ? DEFAULT_PERSISTENCE_TIMEOUT : DEFAULT_IN_MEMORY_TIMEOUT;
        dfltEnabled = false;

        dfltScaleUpEnabled = dfltEnabled;
        dfltScaleUpTimeout = dfltTimeout;

        dfltScaleDownEnabled = dfltEnabled;
        dfltScaleDownTimeout = persistenceEnabled ? DEFAULT_SCALE_DOWN_PERSISTENCE_TIMEOUT : dfltTimeout;

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
            dfltScaleDownTimeout = persistenceEnabled ? DEFAULT_SCALE_DOWN_PERSISTENCE_TIMEOUT : dfltTimeout;
        }
    }

    /**
     * Adds a listener for auto-adjust enabled flag.
     * If the IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE is {@code true}, it adds both SCALE_UP and SCALE_DOWN listeners.
     * If the IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE is {@code false}, it adds general listener, which corresponds
     * the old behavior.
     *
     * @deprecated Use {@link #listenAutoAdjustEnabled(AutoAdjustMode, DistributePropertyListener)} instead.
     */
    @Deprecated
    public void listenAutoAdjustEnabled(DistributePropertyListener<? super Boolean> lsnr) {
        if (isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE)) {
            baselineScaleUpAutoAdjustEnabled.addListener(lsnr);
            baselineScaleDownAutoAdjustEnabled.addListener(lsnr);
        }
        else
            baselineAutoAdjustEnabled.addListener(lsnr);
    }

    /**
     * Adds a listener for auto-adjust timeout.
     * If the IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE is {@code true}, it adds both SCALE_UP and SCALE_DOWN listeners.
     * If the IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE is {@code false}, it adds general listener, which corresponds
     * the old behavior.
     *
     * @deprecated Use {@link #listenAutoAdjustTimeout(AutoAdjustMode, DistributePropertyListener)} instead.
     */
    @Deprecated
    public void listenAutoAdjustTimeout(DistributePropertyListener<? super Long> lsnr) {
        if (isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE)) {
            baselineScaleUpAutoAdjustTimeout.addListener(lsnr);
            baselineScaleDownAutoAdjustTimeout.addListener(lsnr);
        }
        else
            baselineAutoAdjustTimeout.addListener(lsnr);
    }

    /**
     * Adds a listener for auto-adjust enabled flag which corresponds to the auto-adjust mode {@link AutoAdjustMode}.
     */
    public void listenAutoAdjustEnabled(AutoAdjustMode mode, DistributePropertyListener<? super Boolean> lsnr) {
        switch (mode) {
            case SCALE_UP:
                baselineScaleUpAutoAdjustEnabled.addListener(lsnr);
                break;
            case SCALE_DOWN:
                baselineScaleDownAutoAdjustEnabled.addListener(lsnr);
                break;
            default:
                throw new IgniteException("Unsupported auto-adjustment mode: " + mode + ". Use SCALE_UP or SCALE_DOWN.");
        }
    }

    /**
     * Adds a listener for auto-adjust timeout which corresponds to the auto-adjust mode {@link AutoAdjustMode}.
     */
    public void listenAutoAdjustTimeout(AutoAdjustMode mode, DistributePropertyListener<? super Long> lsnr) {
        switch (mode) {
            case SCALE_UP:
                baselineScaleUpAutoAdjustTimeout.addListener(lsnr);
                break;
            case SCALE_DOWN:
                baselineScaleDownAutoAdjustTimeout.addListener(lsnr);
                break;
            default:
                throw new IgniteException("Unsupported auto-adjustment mode: " + mode + ". Use SCALE_UP or SCALE_DOWN.");
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
     * Returns the value of the manual baseline control or the baseline auto-adjust.
     * If the IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE is {@code true}, it returns {@code true} if any of scale
     * directions is enabled.
     * If the IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE is {@code false}, it returns {@code true} if general auto-adjust
     * is enabled, which corresponds to the old behavior.
     *
     * @return Value of manual baseline control or auto-adjust baseline.
     * @deprecated Use {@link #isBaselineAutoAdjustEnabled(AutoAdjustMode)} instead.
     */
    @Deprecated
    public boolean isBaselineAutoAdjustEnabled() {
        if (isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE))
            return isBaselineAutoAdjustEnabled(SCALE_UP) ||
                isBaselineAutoAdjustEnabled(SCALE_DOWN);

        return baselineAutoAdjustEnabled.getOrDefault(dfltEnabled);
    }

    /**
     * Returns the value of the manual baseline control or the baseline auto-adjust for the scale direction which corresponds
     * to the provided auto-adjust mode {@link AutoAdjustMode}.
     *
     * @param mode The baseline scale direction.
     * @return The value of the manual baseline control or the baseline auto-adjust.
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
     * Updates the baseline auto-adjust enabled flag.
     * If the IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE is {@code true}, it updates both SCALE_UP and SCALE_DOWN
     * baseline auto-adjust enbaled flags.
     * If the IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE is {@code false}, it updates only general baseline auto-adjust
     * flag, which corresponds the old behavior.
     *
     * @param baselineAutoAdjustEnabled The value of the manual baseline control or the baseline auto-adjust.
     * @throws IgniteCheckedException if not all nodes in the cluster support baseline auto-adjust.
     * @deprecated Use {@link #updateBaselineAutoAdjustEnabledAsync(AutoAdjustMode, GridKernalContext, boolean)} instead.
     */
    @Deprecated
    public GridFutureAdapter<?> updateBaselineAutoAdjustEnabledAsync(
        GridKernalContext ctx,
        boolean baselineAutoAdjustEnabled
    ) throws IgniteCheckedException {
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
     * Updates the baseline auto-adjust enabled flag, which corresponds to the provided auto-adjust mode {@link AutoAdjustMode}.
     *
     * @param mode The baseline scale direction.
     * @param baselineAutoAdjustEnabled The value of the manual baseline control or the baseline auto-adjust.
     * @throws IgniteCheckedException if failed.
     */
    public GridFutureAdapter<?> updateBaselineAutoAdjustEnabledAsync(
        AutoAdjustMode mode,
        GridKernalContext ctx,
        boolean baselineAutoAdjustEnabled
    ) throws IgniteCheckedException {
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
     * Returns the value of the baseline auto-adjust timeout, which it will wait before the actual topology change since
     * the last discovery event (node join/exit).
     * If the IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE is {@code true}, it returns the SCALE_UP timeout, as the default
     * SCALE_DOWN timeout is {@link Long#MAX_VALUE}.
     * If the IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE is {@code false}, it returns the general timeout, which
     * corresponds the old behavior.
     *
     * @return Value of the baseline auto-adjust timeout.
     * @deprecated Use {@link #getBaselineAutoAdjustTimeout(AutoAdjustMode)} instead.
     */
    @Deprecated
    public long getBaselineAutoAdjustTimeout() {
        if (isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE))
            return baselineScaleUpAutoAdjustTimeout.getOrDefault(dfltScaleUpTimeout);

        return baselineAutoAdjustTimeout.getOrDefault(dfltTimeout);
    }

    /**
     * Returns the value of the baseline auto-adjust timeout, which it will wait before the actual topology change since
     * the last discovery event (node join/exit). The timeout corresponds to the provided auto-adjust mode {@link AutoAdjustMode}.
     *
     * @param mode The baseline scale direction.
     * @return The value of the timeout.
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
     * Updates the value of the baseline auto-adjust timeout, which it will wait before the actual topology change since
     * the last discovery event (node join/exit).
     * If the IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE is {@code true}, it updates both SCALE_UP and SCALE_DOWN
     * baseline auto-adjust timeouts.
     * If the IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE is {@code false}, it updates only general baseline
     * auto-adjust timeout, which corresponds the old behavior.
     *
     * @param baselineAutoAdjustTimeout The value of the timeout.
     * @throws IgniteCheckedException If not all nodes in the cluster support baseline auto-adjust.
     * @deprecated Use {@link #updateBaselineAutoAdjustTimeoutAsync(AutoAdjustMode, GridKernalContext, long)} instead.
     */
    @Deprecated
    public GridFutureAdapter<?> updateBaselineAutoAdjustTimeoutAsync(
        GridKernalContext ctx,
        long baselineAutoAdjustTimeout
    ) throws IgniteCheckedException {
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
     * Updates the value of the baseline auto-adjust timeout, which it will wait before the actual topology change since
     * the last discovery event (node join/exit). The timeout corresponds to the provided auto-adjust mode {@link AutoAdjustMode}.
     *
     * @param mode The baseline scale direction {@link AutoAdjustMode}.
     * @param baselineAutoAdjustTimeout The value of the timeout.
     * @throws IgniteCheckedException If not all nodes in the cluster support separate baseline auto-adjust.
     */
    public GridFutureAdapter<?> updateBaselineAutoAdjustTimeoutAsync(
        AutoAdjustMode mode,
        GridKernalContext ctx,
        long baselineAutoAdjustTimeout
    ) throws IgniteCheckedException {
        if (!isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE))
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
