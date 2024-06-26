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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.util.Objects;

/**
 * This listener allows tracking the lifecycle stages of a workload.
 */
@FunctionalInterface
public interface ReconciliationEventListener {
    /**
     * Workload lifecycle stages.
     *
     * The following state transitions are possible:
     *  SCHEDULED -> BEFORE_PROCESSING -> READY -> FINISHED
     *  SCHEDULED -> SKIPPED -> FINISHED
     */
    enum WorkLoadStage {
        /** Workload is scheduled for processing. */
        SCHEDULED,

        /** Workload is ready to be processed. */
        BEFORE_PROCESSING,

        /** Workload has been processed and the processing result is ready to be used. */
        READY,

        /** Workload is skipped for some reason. */
        SKIPPED,

        /** Processing of the workload is completed. */
        FINISHED
    }

    /**
     * Callbeck for processing the given {@code stage} event of the correcponding {@code workload}.
     *
     * @param stage Workload lifecycle stage.
     * @param workload Workload.
     */
    void onEvent(WorkLoadStage stage, PipelineWorkload workload);

    /**
     * Returns a composed ReconciliationEventListener that performs, in sequence, this
     * operation followed by the {@code after} operation. If performing either
     * operation throws an exception, it is relayed to the caller of the
     * composed operation.  If performing this operation throws an exception,
     * the {@code after} operation will not be performed.
     *
     * @param after Listener to be called after this listener.
     * @return Composed {@code ReconciliationEventListener} that performs in sequence this
     *      listener followed by the {@code after} listener.
     * @throws NullPointerException If {@code after} is null.
     */
    default ReconciliationEventListener andThen(ReconciliationEventListener after) {
        Objects.requireNonNull(after);

        return (stage, workload) -> {
            onEvent(stage, workload);

            after.onEvent(stage, workload);
        };
    }
}
