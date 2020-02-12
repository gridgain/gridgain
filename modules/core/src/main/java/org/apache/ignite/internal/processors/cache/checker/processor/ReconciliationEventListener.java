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

/**
 * Description of contract for {@link ReconciliationEventListener}.
 */
public interface ReconciliationEventListener {
    /**
     * State of workload lifecycle.
     */
    enum WorkLoadStage {
        /**
         * It means that workload added to queue.
         */
        PLANNED,
        /**
         * It means that a compute result fetched and ready to process.
         */
        STARTING,
        /**
         * It means that processing finished.
         */
        FINISHING
    }

    /**
     * Process event.
     */
    void registerEvent(WorkLoadStage stage, PipelineWorkload workload);
}
