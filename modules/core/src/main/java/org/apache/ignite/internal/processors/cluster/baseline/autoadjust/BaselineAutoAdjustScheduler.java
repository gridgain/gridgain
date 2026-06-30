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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.AutoAdjustMode;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_LOG_INTERVAL;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.internal.SupportFeaturesUtils.IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE;
import static org.apache.ignite.internal.SupportFeaturesUtils.isFeatureEnabled;
import static org.apache.ignite.AutoAdjustMode.SCALE_UP_DOWN;
import static org.apache.ignite.AutoAdjustMode.SCALE_DOWN;
import static org.apache.ignite.AutoAdjustMode.SCALE_UP;

/**
 * This class able to add task of set baseline with timeout to queue. In one time only one task can be in queue. Every
 * next queuing task evicted previous one.
 */
class BaselineAutoAdjustScheduler {
    /** Timeout processor. */
    private final GridTimeoutProcessor timeoutProcessor;

    /** Executor of set baseline operation. */
    private final BaselineAutoAdjustExecutor baselineAutoAdjustExecutor;

    /** Last scheduled task for adjust new baseline. It needed for removing from queue. */
    private BaselineMultiplyUseTimeoutObject baselineTimeoutObj;

    /** Last scheduled task for scaleUp adjustment. It needed for removing from queue. */
    private BaselineMultiplyUseTimeoutObject baselineScaleUpTimeoutObj;

    /** Last scheduled task for scale down adjustment. It needed for removing from queue. */
    private BaselineMultiplyUseTimeoutObject baselineScaleDownTimeoutObj;

    /**
     * Keeps the most recent data so any pending task of EITHER direction reconciles against the latest topology.
     */
    private final AtomicReference<BaselineAutoAdjustData> lastBaselineData = new AtomicReference<>();

    /** */
    private final IgniteLogger log;

    /**
     * @param timeoutProcessor Timeout processor.
     * @param baselineAutoAdjustExecutor Executor of set baseline operation.
     * @param log Log object.
     */
    public BaselineAutoAdjustScheduler(GridTimeoutProcessor timeoutProcessor,
                                       BaselineAutoAdjustExecutor baselineAutoAdjustExecutor, IgniteLogger log) {
        this.timeoutProcessor = timeoutProcessor;
        this.baselineAutoAdjustExecutor = baselineAutoAdjustExecutor;
        this.log = log;
    }

    /**
     * Adds a new task to queue based on the given {@code baselineAutoAdjustData} with delay and remove previous one.
     * A new task can be rejected in case of the given {@code baselineAutoAdjustData} is expired or
     * the target topology version is less than the already scheduled version.
     *
     * @param baselineAutoAdjustData Data for changing baseline.
     * @param delay Delay after which set baseline should be started.
     * @return {@code true} If a new task was successfully scheduled.
     */
    @Deprecated
    public boolean schedule(BaselineAutoAdjustData baselineAutoAdjustData, long delay) {
        return schedule(baselineAutoAdjustData, delay, SCALE_UP_DOWN);
    }

    /**
     * Adds a new scale up task to queue based on the given {@code baselineAutoAdjustData} with delay and remove previous
     * one. A new task can be rejected in case of the given {@code baselineAutoAdjustData} is expired or
     * the target topology version is less than the already scheduled version.
     *
     * @param baselineAutoAdjustData Data for changing baseline.
     * @param delay Delay after which set baseline should be started.
     * @return {@code true} If a new task was successfully scheduled.
     */
    public boolean scheduleScaleUp(BaselineAutoAdjustData baselineAutoAdjustData, long delay) {
        return schedule(baselineAutoAdjustData, delay, SCALE_UP);
    }

    /**
     * Adds a new scale down task to queue based on the given {@code baselineAutoAdjustData} with delay and remove
     * previous one. A new task can be rejected in case of the given {@code baselineAutoAdjustData} is expired or
     * the target topology version is less than the already scheduled version.
     *
     * @param baselineAutoAdjustData Data for changing baseline.
     * @param delay Delay after which set baseline should be started.
     * @return {@code true} If a new task was successfully scheduled.
     */
    public boolean scheduleScaleDown(BaselineAutoAdjustData baselineAutoAdjustData, long delay) {
        return schedule(baselineAutoAdjustData, delay, SCALE_DOWN);
    }

    /**
     * Schedule baseline auto adjustment in the specified direction {@link AutoAdjustMode}.
     */
    private synchronized boolean schedule(BaselineAutoAdjustData baselineAutoAdjustData, long delay, AutoAdjustMode mode) {
        if (baselineAutoAdjustExecutor.isExecutionExpired(baselineAutoAdjustData, mode)) {
            if (log.isDebugEnabled())
                log.debug("Baseline " + mode.getLabel() + " auto adjust data is expired (will not be scheduled) [data=" + baselineAutoAdjustData + ']');

            return false;
        }

        BaselineMultiplyUseTimeoutObject timeoutObject = currentTimeoutObj(mode);

        if (timeoutObject != null) {
            long targetVer = baselineAutoAdjustData.getTargetTopologyVersion();
            long alreadyScheduledVer = timeoutObject.baselineAutoAdjustData.get().getTargetTopologyVersion();

            if (alreadyScheduledVer > targetVer) {
                if (log.isDebugEnabled()) {
                    log.debug("Baseline " + mode.getLabel() + " auto adjust data is targeted to obsolete version (will not be scheduled) " +
                        "[data=" + baselineAutoAdjustData + ", scheduled=" + timeoutObject.baselineAutoAdjustData.get() + ']');
                }

                return false;
            }

            timeoutProcessor.removeTimeoutObject(timeoutObject);
        }

        // Publish as the most recent data so any pending task of EITHER direction reconciles against the latest topology
        // and only if this data is at least as new, so a reordered older event can't downgrade the latest.
        BaselineAutoAdjustData cur = lastBaselineData.get();
        if (cur == null || baselineAutoAdjustData.getTargetTopologyVersion() >= cur.getTargetTopologyVersion())
            lastBaselineData.set(baselineAutoAdjustData);

        timeoutObject = new BaselineMultiplyUseTimeoutObject(
            lastBaselineData,
            delay, baselineAutoAdjustExecutor,
            timeoutProcessor,
            log,
            mode
        );

        setTimeoutObj(timeoutObject, mode);

        boolean added = timeoutProcessor.addTimeoutObject(timeoutObject);

        if (log.isDebugEnabled()) {
            log.info("New baseline " + mode.getLabel() + " timeout object was " + (added ? "successfully scheduled " : " rejected ") +
                " [data=" + timeoutObject.baselineAutoAdjustData.get() + ']');
        }

        return added;
    }

    /** Returns the corresponding timeout object. */
    private BaselineMultiplyUseTimeoutObject currentTimeoutObj(AutoAdjustMode mode) {
        switch (mode) {
            case SCALE_UP:
                return baselineScaleUpTimeoutObj;
            case SCALE_DOWN:
                return baselineScaleDownTimeoutObj;
            default:
                return baselineTimeoutObj;
        }
    }

    /** Sets the corresponding timeout object. */
    private void setTimeoutObj(BaselineMultiplyUseTimeoutObject timeoutObj, AutoAdjustMode mode) {
        switch (mode) {
            case SCALE_UP:
                baselineScaleUpTimeoutObj = timeoutObj;
                break;
            case SCALE_DOWN:
                baselineScaleDownTimeoutObj = timeoutObj;
                break;
            default:
                baselineTimeoutObj = timeoutObj;
                break;
        }
    }

    /**
     * @return Time of last scheduled task or -1 if it doesn't exist.
     */
    @Deprecated
    public synchronized long lastScheduledTaskTime() {
        return Math.min(lastScheduledTaskTime(SCALE_UP), lastScheduledTaskTime(SCALE_DOWN));
    }

    /**
     * @param mode The baseline scale direction {@link AutoAdjustMode}.
     * @return Time of last scheduled task or -1 if it doesn't exist.
     */
    public synchronized long lastScheduledTaskTime(AutoAdjustMode mode) {
        if (!isFeatureEnabled(IGNITE_SEPARATE_BASELINE_AUTO_ADJUST_FEATURE)) {
            if (baselineTimeoutObj == null)
                return -1;

            long lastScheduledTaskTime = baselineTimeoutObj.getTotalEndTime() - System.currentTimeMillis();

            return lastScheduledTaskTime < 0 ? -1 : lastScheduledTaskTime;
        }

        long now = System.currentTimeMillis();

        long lastScheduledTaskTime = -1;
        switch (mode) {
            case SCALE_UP:
                if (baselineScaleUpTimeoutObj == null)
                    return -1;
                else
                    lastScheduledTaskTime = baselineScaleUpTimeoutObj.getTotalEndTime() - now;

                break;
            case SCALE_DOWN:
                if (baselineScaleDownTimeoutObj == null)
                    return -1;
                else
                    lastScheduledTaskTime = baselineScaleDownTimeoutObj.getTotalEndTime() - now;

                break;
        }

        return lastScheduledTaskTime < 0 ? -1 : lastScheduledTaskTime;
    }

    /**
     * @param data Baseline data for adjust.
     * @return {@code true} If baseline auto-adjust shouldn't be executed for given data.
     */
    boolean isExecutionExpired(BaselineAutoAdjustData data, AutoAdjustMode mode) {
        return baselineAutoAdjustExecutor.isExecutionExpired(data, mode);
    }

    /**
     * Timeout object of baseline auto-adjust operation. This object able executing several times: some first times for
     * logging of expecting auto-adjust and last time for baseline adjust.
     */
    private static class BaselineMultiplyUseTimeoutObject implements GridTimeoutObject {
        /** Interval between logging of info about next baseline auto-adjust. */
        private static final long AUTO_ADJUST_LOG_INTERVAL =
            getLong(IGNITE_BASELINE_AUTO_ADJUST_LOG_INTERVAL, 60_000);

        /** Shared holder of the most recent baseline data (owned by the scheduler). */
        private final AtomicReference<BaselineAutoAdjustData> baselineAutoAdjustData;

        /** Executor of set baseline operation. */
        private final BaselineAutoAdjustExecutor baselineAutoAdjustExecutor;

        /** Timeout processor. */
        private final GridTimeoutProcessor timeoutProcessor;

        /** */
        private final IgniteLogger log;

        /** End time of whole life of this object. It represents time when auto-adjust will be executed. */
        private final long totalEndTime;

        /** Timeout ID. */
        private final IgniteUuid id = IgniteUuid.randomUuid();

        /** The baseline scale direction {@link AutoAdjustMode}. */
        private final AutoAdjustMode mode;

        /** End time of one iteration of this timeout object. */
        private long endTime;

        /**
         * @param baselineAutoAdjustData Data for changing baseline.
         * @param executionTimeout Delay after which set baseline should be started.
         * @param executor Executor of set baseline operation.
         * @param processor Timeout processor.
         * @param log Log object.
         * @param mode The baseline auto-adjust mode.
         */
        protected BaselineMultiplyUseTimeoutObject(
            AtomicReference<BaselineAutoAdjustData> baselineAutoAdjustData,
            long executionTimeout,
            BaselineAutoAdjustExecutor executor,
            GridTimeoutProcessor processor,
            IgniteLogger log,
            AutoAdjustMode mode
        ) {
            this.baselineAutoAdjustData = baselineAutoAdjustData;
            baselineAutoAdjustExecutor = executor;
            timeoutProcessor = processor;
            this.log = log;
            endTime = calculateEndTime(executionTimeout);
            this.totalEndTime = U.currentTimeMillis() + executionTimeout;
            this.mode = mode;
        }

        /**
         * @param timeout Remaining time to baseline adjust.
         * @return Calculated end time to next iteration.
         */
        private long calculateEndTime(long timeout) {
            return U.currentTimeMillis() + (AUTO_ADJUST_LOG_INTERVAL < timeout ? AUTO_ADJUST_LOG_INTERVAL : timeout);
        }

        /** {@inheritDoc}. */
        @Override public IgniteUuid timeoutId() {
            return id;
        }

        /** {@inheritDoc}. */
        @Override public long endTime() {
            return endTime;
        }

        /** {@inheritDoc}. */
        @Override public void onTimeout() {
            if (baselineAutoAdjustExecutor.isExecutionExpired(baselineAutoAdjustData.get(), mode))
                return;

            long lastScheduledTaskTime = totalEndTime - System.currentTimeMillis();

            if (lastScheduledTaskTime <= 0) {
                if (log.isInfoEnabled())
                    log.info("Baseline auto-adjust will be executed right now.");

                baselineAutoAdjustExecutor.execute(baselineAutoAdjustData.get(), mode);
            }
            else {
                if (log.isInfoEnabled())
                    log.info("Baseline auto-adjust will be executed in '" + lastScheduledTaskTime + "' ms.");

                endTime = calculateEndTime(lastScheduledTaskTime);

                timeoutProcessor.addTimeoutObject(this);
            }
        }

        /**
         * @return End time of whole life of this object. It represent time when auto-adjust will be executed.
         */
        public long getTotalEndTime() {
            return totalEndTime;
        }
    }
}
