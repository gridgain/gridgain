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
 * Event listener factory for test approach.
 */
public class ReconciliationEventListenerFactory {
    /** Default instance. */
    private static volatile ReconciliationEventListener defaultInstance = (stage, workload) -> {
    };

    /**
     *
     */
    public static ReconciliationEventListener create() {
        return defaultInstance;
    }

    /**
     *
     */
    public static void setDefaultInstance(ReconciliationEventListener defaultInstance) {
        ReconciliationEventListenerFactory.defaultInstance = defaultInstance;
    }
}
