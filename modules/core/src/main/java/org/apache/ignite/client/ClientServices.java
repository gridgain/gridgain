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

import java.util.Collection;

/**
 * Thin client services facade.
 */
public interface ClientServices {
    /**
     * Gets the cluster group to which this {@code ClientServices} instance belongs.
     *
     * @return Cluster group to which this {@code ClientServices} instance belongs.
     */
    public ClientClusterGroup clusterGroup();

    /**
     * Gets a remote handle on the service.
     * <p>
     * Note: There are no guarantees that each method invocation for the same proxy will always contact the same remote
     * service (on the same remote node).
     *
     * @param name Service name.
     * @param svcItf Interface for the service.
     * @return Proxy over remote service.
     */
    public <T> T serviceProxy(String name, Class<? super T> svcItf);

    /**
     * Gets a remote handle on the service with timeout.
     * <p>
     * Note: There are no guarantees that each method invocation for the same proxy will always contact the same remote
     * service (on the same remote node).
     *
     * @param name Service name.
     * @param svcItf Interface for the service.
     * @param timeout If greater than 0 created proxy will wait for service availability only specified time,
     *  and will limit remote service invocation time.
     * @return Proxy over remote service.
     */
    public <T> T serviceProxy(String name, Class<? super T> svcItf, long timeout);

    /**
     * Gets metadata about all deployed services in the grid.
     *
     * @return Metadata about all deployed services in the grid.
     */
    public Collection<ClientServiceDescriptor> serviceDescriptors();

    /**
     * Gets metadata about deployed services in the grid.
     *
     * @param name Service name.
     * @return Metadata about all deployed services in the grid.
     */
    public ClientServiceDescriptor serviceDescriptor(String name);
}
