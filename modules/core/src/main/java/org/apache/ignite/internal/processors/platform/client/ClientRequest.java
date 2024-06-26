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

package org.apache.ignite.internal.processors.platform.client;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;

/**
 * Thin client request.
 */
public class ClientRequest implements ClientListenerRequest {
    /** Request id. */
    private final long reqId;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientRequest(BinaryRawReader reader) {
        reqId = reader.readLong();
    }

    /**
     * Constructor.
     *
     * @param reqId Request id.
     */
    public ClientRequest(long reqId) {
        this.reqId = reqId;
    }

    /** {@inheritDoc} */
    @Override public long requestId() {
        return reqId;
    }

    /**
     * Processes the request.
     *
     * @return Response.
     */
    public ClientResponse process(ClientConnectionContext ctx) {
        return new ClientResponse(reqId);
    }

    /**
     * Processes the request asynchronously.
     *
     * @return Future for response.
     */
    public IgniteInternalFuture<ClientResponse> processAsync(ClientConnectionContext ctx) {
        throw new IllegalStateException("Async operation is not implemented for request " + getClass().getName());
    }

    /**
     * @param ctx Client connection context.
     * @return {@code True} if request should be processed asynchronously.
     */
    public boolean isAsync(ClientConnectionContext ctx) {
        return false;
    }
}
