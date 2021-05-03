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

package org.apache.ignite.internal.util.worker;

/**
 * Dispatcher of workers' progress which allows us to understand if worker freezes.
 */
public interface WorkProgressDispatcher {
    /**
     * Last heatbeat timestamp.
     */
    public long heartbeatTs();

    /**
     * Notifying dispatcher that work is in progress and thread didn't freeze.
     */
    public void updateHeartbeat();

    /**
     * Protects the worker from timeout penalties if subsequent instructions in the calling thread does not update
     * heartbeat timestamp timely, e.g. due to blocking operations, up to the nearest {@link #blockingSectionEnd()}
     * call. Nested calls are not supported.
     */
    public void blockingSectionBegin();

    /**
     * Closes the protection section previously opened by {@link #blockingSectionBegin()}.
     */
    public void blockingSectionEnd();
}
