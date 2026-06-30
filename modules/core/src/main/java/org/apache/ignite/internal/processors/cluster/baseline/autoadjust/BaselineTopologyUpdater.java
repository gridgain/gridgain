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

package org.apache.ignite.internal.processors.cluster.baseline.autoadjust;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.SupportFeaturesUtils;
import org.apache.ignite.AutoAdjustMode;
import org.apache.ignite.internal.cluster.DistributedBaselineConfiguration;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor;
import org.apache.ignite.internal.util.typedef.internal.CU;

import static org.apache.ignite.AutoAdjustMode.SCALE_UP_DOWN;
import static org.apache.ignite.internal.IgniteFeatures.BASELINE_AUTO_ADJUSTMENT;
import static org.apache.ignite.internal.IgniteFeatures.SEPARATE_BASELINE_AUTO_ADJUSTMENT;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.isFeatureEnabled;
import static org.apache.ignite.AutoAdjustMode.SCALE_DOWN;
import static org.apache.ignite.AutoAdjustMode.SCALE_UP;
import static org.apache.ignite.internal.processors.cluster.baseline.autoadjust.BaselineAutoAdjustData.NULL_BASELINE_DATA;
import static org.apache.ignite.internal.util.IgniteUtils.isLocalNodeCoordinator;

/**
 * Baseline topology updater with ability to watch of topology changes.
 * It initiates update to set new baseline after some timeout.
 */
public class BaselineTopologyUpdater {
    /** */
    private final IgniteLogger log;

    /** */
    private final IgniteClusterImpl cluster;

    /** */
    private final GridCachePartitionExchangeManager<?, ?> exchangeManager;

    /** Configuration of baseline. */
    private final DistributedBaselineConfiguration baselineConfiguration;

    /** Discovery manager. */
    private final GridDiscoveryManager discoveryMgr;

    /** */
    private final GridClusterStateProcessor stateProcessor;

    /** Scheduler of specific task of baseline changing. */
    private final BaselineAutoAdjustScheduler baselineAutoAdjustScheduler;

    /** Context.*/
    private final GridKernalContext ctx;

    /** Last data for set new baseline. */
    private BaselineAutoAdjustData lastBaselineData = NULL_BASELINE_DATA;

    /**
     * @param ctx Context.
     */
    public BaselineTopologyUpdater(GridKernalContext ctx) {
        this.log = ctx.log(BaselineTopologyUpdater.class);
        this.cluster = ctx.cluster().get();
        this.baselineConfiguration = ctx.state().baselineConfiguration();
        this.exchangeManager = ctx.cache().context().exchange();
        this.stateProcessor = ctx.state();
        this.baselineAutoAdjustScheduler = new BaselineAutoAdjustScheduler(ctx.timeout(), new BaselineAutoAdjustExecutor(
            ctx.log(BaselineAutoAdjustExecutor.class),
            cluster,
            ctx.pools().getSystemExecutorService(),
            this::isTopologyWatcherEnabled,
            () -> isTopologyWatcherEnabled(SCALE_UP),
            () -> isTopologyWatcherEnabled(SCALE_DOWN)
        ), ctx.log(BaselineAutoAdjustScheduler.class));
        this.discoveryMgr = ctx.discovery();
        this.ctx = ctx;
    }

    /**
     * Schedule update of the baseline topology.
     * @param topologyVersion version of topology.
     * @deprecated Use {@link #triggerBaselineUpdate(long, AutoAdjustMode)} instead.
     */
    @Deprecated
    public void triggerBaselineUpdate(long topologyVersion) {
        if (isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE)) {
            triggerBaselineUpdate(topologyVersion, SCALE_UP);
            triggerBaselineUpdate(topologyVersion, SCALE_DOWN);

            return;
        }

        if (!isTopologyWatcherEnabled()) {
            synchronized (this) {
                lastBaselineData = NULL_BASELINE_DATA;
            }
        }

        synchronized (this) {
            lastBaselineData = lastBaselineData.next(topologyVersion);

            final BaselineAutoAdjustData baselineData = lastBaselineData;

            if (isLocalNodeCoordinator(discoveryMgr)) {
                exchangeManager.affinityReadyFuture(new AffinityTopologyVersion(topologyVersion))
                    .listen(future -> {
                        if (future.error() != null)
                            return;

                        if (exchangeManager.lastFinishedFuture().hasLostPartitions()) {
                            log.warning("Baseline won't be changed cause lost partitions were detected");

                            return;
                        }

                        long timeout = baselineConfiguration.getBaselineAutoAdjustTimeout();

                        // In case of merging exchanges the baseline data can be already expired
                        // and so it should be rejected by scheduler.
                        if (baselineAutoAdjustScheduler.schedule(baselineData, timeout))
                            log.warning("Baseline auto-adjust will be executed in '" + timeout + "' ms");
                    });
            }
        }
    }

    /**
     * Schedule update of the baseline topology in direction corresponding to the provided auto-adjust mode
     * {@link AutoAdjustMode}.
     *
     * @param topologyVersion version of topology.
     * @param mode The baseline scale direction.
     */
    public void triggerBaselineUpdate(long topologyVersion, AutoAdjustMode mode) {
        // Only reset shared data when NEITHER direction is watching.
        if (!isTopologyWatcherEnabled(SCALE_UP) && !isTopologyWatcherEnabled(SCALE_DOWN)) {
            synchronized (this) {
                lastBaselineData = NULL_BASELINE_DATA;
            }

            return;
        }

        synchronized (this) {
            lastBaselineData = lastBaselineData.next(topologyVersion);

            final BaselineAutoAdjustData baselineData = lastBaselineData;

            if (isLocalNodeCoordinator(discoveryMgr)) {
                exchangeManager.affinityReadyFuture(new AffinityTopologyVersion(topologyVersion))
                    .listen(future -> {
                        if (future.error() != null)
                            return;

                        if (exchangeManager.lastFinishedFuture().hasLostPartitions()) {
                            log.warning("Baseline won't be changed cause lost partitions were detected");

                            return;
                        }

                        boolean scheduled;
                        long timeout;

                        switch (mode) {
                            case SCALE_UP:
                                timeout = baselineConfiguration.getBaselineAutoAdjustTimeout(mode);
                                scheduled = baselineAutoAdjustScheduler.scheduleScaleUp(baselineData, timeout);

                                break;
                            case SCALE_DOWN:
                                timeout = baselineConfiguration.getBaselineAutoAdjustTimeout(mode);
                                scheduled = baselineAutoAdjustScheduler.scheduleScaleDown(baselineData, timeout);

                                break;

                            default:
                                throw new IgniteException("Unsupported auto-adjustment mode: " + mode);
                        }

                        // In case of merging exchanges the baseline data can be already expired
                        // and so it should be rejected by scheduler.
                        if (scheduled)
                            log.warning("Baseline auto-adjust will be executed in '" + timeout + "' ms");
                    });
            }
        }
    }

    /**
     * @return {@code true} if auto-adjust baseline enabled.
     */
    private boolean isTopologyWatcherEnabled() {
        return isSupported(ctx)
            && !ctx.clientNode()
            && stateProcessor.clusterState().active()
            && baselineConfiguration.isBaselineAutoAdjustEnabled()
            && (CU.isPersistenceEnabled(cluster.ignite().configuration())
            || cluster.baselineAutoAdjustTimeout() != 0L);
    }

    /**
     * @return {@code true} if auto-adjust baseline enabled for the scale up {@code true} or scale down {@code false}.
     */
    private boolean isTopologyWatcherEnabled(AutoAdjustMode mode) {
        return isSupported(ctx)
            && !ctx.clientNode()
            && stateProcessor.clusterState().active()
            && baselineConfiguration.isBaselineAutoAdjustEnabled(mode)
            && (CU.isPersistenceEnabled(cluster.ignite().configuration())
            || cluster.baselineAutoAdjustTimeout(mode) != 0L);
    }

    /**
     * @return {@code True} if all nodes in the cluster support baseline auto-adjust.
     *         If {@link SupportFeaturesUtils#IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE} is {@code true}, return
     *         {@code True} if all nodes in the cluster support separate baseline auto-adjust.
     * @see IgniteFeatures#BASELINE_AUTO_ADJUSTMENT
     */
    public static boolean isSupported(GridKernalContext ctx) {
        if (isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE))
            return IgniteFeatures.allNodesSupport(ctx, SEPARATE_BASELINE_AUTO_ADJUSTMENT);

        return IgniteFeatures.allNodesSupport(ctx, BASELINE_AUTO_ADJUSTMENT);
    }

    /**
     * @return Statistic of baseline auto-adjust.
     * @deprecated Use {@link #getStatus(AutoAdjustMode)} instead.
     */
    @Deprecated
    public BaselineAutoAdjustStatus getStatus() {
        synchronized (this) {
            if (lastBaselineData.isAdjusted() || baselineAutoAdjustScheduler.isExecutionExpired(lastBaselineData, SCALE_UP_DOWN))
                return BaselineAutoAdjustStatus.notScheduled();

            long timeToLastTask = baselineAutoAdjustScheduler.lastScheduledTaskTime();

            if (timeToLastTask <= 0)
                return BaselineAutoAdjustStatus.inProgress();

            return BaselineAutoAdjustStatus.scheduled(timeToLastTask);
        }
    }

    /**
     * Returns the statistics of the baseline auto-adjust which correspond to the provided auto-adjust mode {@link AutoAdjustMode}.
     *
     * @param mode The baseline scale direction {@link AutoAdjustMode}.
     * @return Statistic of baseline auto-adjust.
     */
    public BaselineAutoAdjustStatus getStatus(AutoAdjustMode mode) {
        synchronized (this) {
            if (lastBaselineData.isAdjusted(mode) || baselineAutoAdjustScheduler.isExecutionExpired(lastBaselineData, mode))
                return BaselineAutoAdjustStatus.notScheduled();

            long timeToLastTask = baselineAutoAdjustScheduler.lastScheduledTaskTime(mode);

            if (timeToLastTask <= 0)
                return BaselineAutoAdjustStatus.inProgress();

            return BaselineAutoAdjustStatus.scheduled(timeToLastTask);
        }
    }
}
