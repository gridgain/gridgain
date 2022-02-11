/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.platform.client.service;

import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.services.ServiceDescriptor;

import static org.apache.ignite.internal.processors.platform.client.service.ClientServiceDescriptorsResponse.writeDescriptor;

/** Service descriptor. */
public class ClientServiceDescriptorResponse extends ClientResponse {
    /** Services descriptor. */
    private final ServiceDescriptor svc;

    /**
     * @param reqId Request id.
     * @param svc Services descriptor.
     */
    public ClientServiceDescriptorResponse(long reqId, ServiceDescriptor svc) {
        super(reqId);

        this.svc = svc;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        super.encode(ctx, writer);

        writeDescriptor(writer, svc);
    }
}
