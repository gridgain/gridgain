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

/**
 * This interface provides a list of addresses of Ignite server nodes within a cluster. Thin client uses the list to
 * route user requests. There are cases when the list is not static, for example in cloud environment. In such cases
 * addresses of nodes and/or number of server nodes can change. Implementation of this interface should handle these.
 *
 * Ignite waits for a topology change to trigger {@link #getAddresses()}. There are two modes of how soon Ignite calls
 * it, lazy and eager, depending on whether the partition awareness feature is enabled. If the feature is enabled then
 * Ignite calls the method for every topology change. Otherwise Ignite uses previous addresses until a first failure.
 *
 * {@link org.apache.ignite.configuration.ClientConfiguration#setAffinityAwarenessEnabled(boolean)}
 * {@link org.apache.ignite.configuration.ClientConfiguration#setAddressesFinder(ClientAddressFinder)}
 */
public interface ClientAddressFinder {
    /**
     * Get addresses of Ignite server nodes within a cluster. An address can be IPv4 address or hostname, with or
     * without port. If port is not set then Ignite will generate multiple addresses for default port range. See
     * {@link org.apache.ignite.configuration.ClientConnectorConfiguration#DFLT_PORT},
     * {@link org.apache.ignite.configuration.ClientConnectorConfiguration#DFLT_PORT_RANGE}.
     *
     * @return Addresses of Ignite server nodes within a cluster.
     */
    public String[] getAddresses();
}
