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

package org.apache.ignite.internal.client.thin;

/**
 * Notification types.
 */
enum ClientNotificationType {
    /** Continuous query  event. */
    CONTINUOUS_QUERY_EVENT(false),

    /** Compute task finished. */
    COMPUTE_TASK_FINISHED(true);

    /** */
    private final boolean keepNotificationsWithoutListener;

    /** */
    ClientNotificationType(boolean keepNotificationsWithoutListener) {
        this.keepNotificationsWithoutListener = keepNotificationsWithoutListener;
    }

    /**
     * @return {@code True} if it's required to save all received notifications when listener for this notification
     * is not yet registered. These notifications will be processed when listener for their resource will be registered.
     */
    public boolean keepNotificationsWithoutListener() {
        return keepNotificationsWithoutListener;
    }
}
