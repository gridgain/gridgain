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

package org.apache.ignite.spi.systemview.view;

import java.util.UUID;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Service representation for a {@link ServiceDistributionView}.
 */
public class ServiceDistributionView {
    /** Service ID. */
    private final IgniteUuid serviceId;

    /** Node ID. */
    private final UUID nodeId;

    /** Total count of service instances. */
    private final int servicesCount;

    /**
     * @param serviceId Service ID.
     * @param nodeId Node ID.
     * @param servicesCount Total count of service instances.
     */
    public ServiceDistributionView(IgniteUuid serviceId, UUID nodeId, int servicesCount) {
        this.serviceId = serviceId;
        this.nodeId = nodeId;
        this.servicesCount = servicesCount;
    }

    /** @return Service ID. */
    @Order
    public IgniteUuid getServiceId() {
        return serviceId;
    }

    /** @return Node ID. */
    @Order(1)
    public UUID getNodeId() {
        return nodeId;
    }

    /** @return Total count of service instances. */
    @Order(2)
    public int getServicesCount() {
        return servicesCount;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceDistributionView.class, this);
    }
}
