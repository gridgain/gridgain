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

package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Continuous query response.
 */
class ClientCacheQueryContinuousResponse extends ClientResponse {
    /** */
    private final ClientCacheQueryContinuousHandle handle;

    /** */
    private final long qryId;

    /**
     * Ctor.
     * @param reqId Request id.
     * @param handle Handle.
     * @param qryId Continuous query handle id.
     */
    public ClientCacheQueryContinuousResponse(long reqId, ClientCacheQueryContinuousHandle handle, long qryId) {
        super(reqId);
        this.handle = handle;
        this.qryId = qryId;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        super.encode(ctx, writer);

        writer.writeLong(qryId);
    }

    /** {@inheritDoc} */
    @Override public void onSent() {
        super.onSent();

        handle.startNotifications(qryId);
    }
}
