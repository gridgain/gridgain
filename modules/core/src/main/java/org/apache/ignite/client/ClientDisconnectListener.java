/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.client;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import org.apache.ignite.cache.query.ContinuousQuery;

/**
 * Client disconnected event listener. Such listeners can be used in {@link ClientCache#query(ContinuousQuery,
 * ClientDisconnectListener)} or {@link ClientCache#registerCacheEntryListener(CacheEntryListenerConfiguration,
 * ClientDisconnectListener)} methods to handle client channel failure.
 */
public interface ClientDisconnectListener {
    /**
     * Client disconnected callback.
     *
     * @param reason Exception that caused the disconnect, or {@code null} when thin client connection is closed
     * by the user.
     */
    public void onDisconnected(Exception reason);
}
