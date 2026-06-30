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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.SupportFeaturesUtils;
import org.apache.ignite.AutoAdjustMode;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;

import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.isFeatureEnabled;
import static org.apache.ignite.AutoAdjustMode.SCALE_DOWN;
import static org.apache.ignite.AutoAdjustMode.SCALE_UP;

/**
 * This executor try to set new baseline by given data.
 */
class BaselineAutoAdjustExecutor {
    /** */
    private final IgniteLogger log;

    /** */
    private final IgniteClusterImpl cluster;

    /** Service for execute this task in async. */
    private final ExecutorService executorService;

    /** {@code true} if baseline auto-adjust enabled. */
    private final BooleanSupplier isBaselineAutoAdjustEnabled;

    /** {@code true} if baseline scale up auto-adjust enabled. */
    private final BooleanSupplier isBaselineScaleUpAutoAdjustEnabled;

    /** {@code true} if baseline scale down auto-adjust enabled. */
    private final BooleanSupplier isBaselineScaleDownAutoAdjustEnabled;

    /** This protect from execution more than one task at same moment. */
    private final Lock executionGuard = new ReentrantLock();

    /**
     * @param log Logger.
     * @param cluster Ignite cluster.
     * @param executorService Thread pool for changing baseline.
     * @param enabledSupplier Supplier return {@code true} if baseline auto-adjust enabled.
     * @param scaleUpEnabledSupplier Supplier return {@code true} if baseline scale up auto-adjust enabled.
     * @param scaleDownEnabledSupplier Supplier return {@code true} if baseline scale down auto-adjust enabled.
     */
    public BaselineAutoAdjustExecutor(
        IgniteLogger log,
        IgniteClusterImpl cluster,
        ExecutorService executorService,
        BooleanSupplier enabledSupplier,
        BooleanSupplier scaleUpEnabledSupplier,
        BooleanSupplier scaleDownEnabledSupplier
    ) {
        this.log = log;
        this.cluster = cluster;
        this.executorService = executorService;
        isBaselineAutoAdjustEnabled = enabledSupplier;
        isBaselineScaleUpAutoAdjustEnabled = scaleUpEnabledSupplier;
        isBaselineScaleDownAutoAdjustEnabled = scaleDownEnabledSupplier;
    }

    /**
     * Try to set baseline if all conditions it allowed.
     *
     * @param data Data for operation.
     * @param mode The baseline scale direction {@link AutoAdjustMode}.
     */
    public void execute(BaselineAutoAdjustData data, AutoAdjustMode mode) {
        executorService.submit(() ->
            {
                if (isExecutionExpired(data, mode))
                    return;

                executionGuard.lock();
                try {
                    if (isExecutionExpired(data, mode))
                        return;

                    cluster.triggerBaselineAutoAdjust(data.getTargetTopologyVersion(), mode);
                }
                catch (IgniteException e) {
                    log.error("Error during baseline changing", e);
                }
                finally {
                    data.onAdjust(mode);

                    executionGuard.unlock();
                }
            }
        );
    }

    /**
     * @param data Baseline data for adjust.
     * @param mode The baseline scale direction {@link AutoAdjustMode}.
     *             If the {@link SupportFeaturesUtils#IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE} is false, then
     *             the flag will be ignored.
     * @return {@code true} If baseline auto-adjust shouldn't be executed for given data.
     */
    public boolean isExecutionExpired(BaselineAutoAdjustData data, AutoAdjustMode mode) {
        if (isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE))
            return data.isInvalidated() || (!isBaselineScaleUpAutoAdjustEnabled.getAsBoolean() && mode == SCALE_UP) ||
                (!isBaselineScaleDownAutoAdjustEnabled.getAsBoolean() && mode == SCALE_DOWN);

        return data.isInvalidated() || !isBaselineAutoAdjustEnabled.getAsBoolean();
    }
}
