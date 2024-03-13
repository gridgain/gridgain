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

package org.apache.ignite.client;

import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.plugin.CachePluginConfiguration;

/**
 * Client cache plugin configuration. Maps to {@link CachePluginConfiguration} on the server side.
 */
public interface ClientCachePluginConfiguration {
    /**
     * Gets the name of the plugin.
     *
     * @return Plugin name.
     */
    String pluginName();

    /**
     * Serializes the configuration on the client side.
     *
     * @param writer Writer.
     */
    void write(BinaryRawWriter writer);
}
