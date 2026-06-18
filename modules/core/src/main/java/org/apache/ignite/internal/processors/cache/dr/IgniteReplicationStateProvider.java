/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.dr;

/**
 * DR replication-state SPI. OSS stub - always reports inactive; the GridGain plugin registers a
 * DR-aware subclass via {@link org.apache.ignite.plugin.PluginProvider#createComponent}. Used by
 * {@code cmd=supply-status&shutdown=true} to refuse the gate while DR is in flight.
 */
public class IgniteReplicationStateProvider {
    /** @return {@code true} if DR has work in flight on this node. OSS stub: always {@code false}. */
    public boolean isReplicationActive() {
        return false;
    }
}
