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

package org.apache.ignite.internal.managers.systemview.walker;

import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.systemview.view.ServiceDistributionView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;

/**
 * {@link ServiceDistributionView} attributes walker.
 */
public class ServiceDistributionViewWalker implements SystemViewRowAttributeWalker<ServiceDistributionView> {
    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "serviceId", IgniteUuid.class);
        v.accept(1, "nodeId", UUID.class);
        v.accept(2, "servicesCount", int.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAll(ServiceDistributionView row, AttributeWithValueVisitor v) {
        v.accept(0, "serviceId", IgniteUuid.class, row.getServiceId());
        v.accept(1, "nodeId", UUID.class, row.getNodeId());
        v.acceptInt(2, "servicesCount", row.getServicesCount());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 3;
    }
}
