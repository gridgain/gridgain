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

package org.apache.ignite.internal.managers.systemview;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.ignite.internal.processors.service.ServiceInfo;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.systemview.view.ServiceDistributionView;

/**
 * Responsibility of this class it to expose distribution by nodes from {@link ServiceInfo}.
 */
public class ServiceDistributionIterable implements Iterable<ServiceDistributionView> {
    /** Service ID. */
    private final IgniteUuid srvcId;

    /** Topology snapshot. */
    @GridToStringInclude
    private final Iterator<Map.Entry<UUID, Integer>> iters;

    /**
     * @param serviceInfo Service descriptor.
     */
    public ServiceDistributionIterable(ServiceInfo serviceInfo) {
        srvcId = serviceInfo.serviceId();
        iters = serviceInfo.topologySnapshot().entrySet().iterator();
    }

    /** {@inheritDoc} */
    @Override public Iterator<ServiceDistributionView> iterator() {
        return new Iterator<ServiceDistributionView>() {
            private ServiceDistributionView next;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                advance();

                return next != null;
            }

            private void advance() {
                if (next != null)
                    return;

                if (iters.hasNext()) {
                    Map.Entry<UUID, Integer> curr = iters.next();

                    next = new ServiceDistributionView(
                        srvcId,
                        curr.getKey(),
                        curr.getValue()
                    );
                }
            }

            /** {@inheritDoc} */
            @Override public ServiceDistributionView next() {
                if (next == null)
                    advance();

                ServiceDistributionView next0 = next;

                if (next0 == null)
                    throw new NoSuchElementException();

                next = null;

                return next0;
            }
        };
    }
}
