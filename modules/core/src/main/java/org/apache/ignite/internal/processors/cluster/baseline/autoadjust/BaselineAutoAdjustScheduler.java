/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cluster.baseline.autoadjust;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_LOG_INTERVAL;
import static org.apache.ignite.IgniteSystemProperties.getLong;

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
     * Adding task to queue with delay and remove previous one.
     *
     * @param baselineAutoAdjustData Data for changing baseline.
     * @param delay Delay after which set baseline should be started.
     */
    public synchronized void schedule(BaselineAutoAdjustData baselineAutoAdjustData, long delay) {
        if (baselineTimeoutObj != null)
            timeoutProcessor.removeTimeoutObject(baselineTimeoutObj);

        timeoutProcessor.addTimeoutObject(
            baselineTimeoutObj = new BaselineMultiplyUseTimeoutObject(
                baselineAutoAdjustData,
                delay, baselineAutoAdjustExecutor,
                timeoutProcessor,
                log
            )
        );
    }

    /**
     * @return Time of last scheduled task or -1 if it doesn't exist.
     */
    public long lastScheduledTaskTime() {
        if (baselineTimeoutObj == null)
            return -1;

        long lastScheduledTaskTime = baselineTimeoutObj.getTotalEndTime() - System.currentTimeMillis();

        return lastScheduledTaskTime < 0 ? -1 : lastScheduledTaskTime;
    }

    /**
     * Timeout object of baseline auto-adjust operation. This object able executing several times: some first times for
     * logging of expecting auto-adjust and last time for baseline adjust.
     */
    private static class BaselineMultiplyUseTimeoutObject implements GridTimeoutObject {
        /** Interval between logging of info about next baseline auto-adjust. */
        private static final long AUTO_ADJUST_LOG_INTERVAL =
            getLong(IGNITE_BASELINE_AUTO_ADJUST_LOG_INTERVAL, 60_000);
        /** Last data for set new baseline. */
        private final BaselineAutoAdjustData baselineAutoAdjustData;
        /** Executor of set baseline operation. */
        private final BaselineAutoAdjustExecutor baselineAutoAdjustExecutor;
        /** Timeout processor. */
        private final GridTimeoutProcessor timeoutProcessor;
        /** */
        private final IgniteLogger log;

        /** End time of whole life of this object. It represent time when auto-adjust will be executed. */
        private final long totalEndTime;
        /** Timeout ID. */
        private final IgniteUuid id = IgniteUuid.randomUuid();
        /** End time of one iteration of this timeout object. */
        private long endTime;

        /**
         * @param data Data for changing baseline.
         * @param executionTimeout Delay after which set baseline should be started.
         * @param executor Executor of set baseline operation.
         * @param processor Timeout processor.
         * @param log Log object.
         */
        protected BaselineMultiplyUseTimeoutObject(
            BaselineAutoAdjustData data,
            long executionTimeout,
            BaselineAutoAdjustExecutor executor,
            GridTimeoutProcessor processor,
            IgniteLogger log
        ) {
            baselineAutoAdjustData = data;
            baselineAutoAdjustExecutor = executor;
            timeoutProcessor = processor;
            this.log = log;
            endTime = calculateEndTime(executionTimeout);
            this.totalEndTime = U.currentTimeMillis() + executionTimeout;
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
            if (baselineAutoAdjustExecutor.isExecutionExpired(baselineAutoAdjustData))
                return;

            long lastScheduledTaskTime = totalEndTime - System.currentTimeMillis();

            if (lastScheduledTaskTime <= 0) {
                log.info("Baseline auto-adjust will be executed right now.");

                baselineAutoAdjustExecutor.execute(baselineAutoAdjustData);
            }
            else {
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
