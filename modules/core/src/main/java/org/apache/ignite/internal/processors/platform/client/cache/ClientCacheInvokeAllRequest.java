/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

import java.util.Map;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Cache invokeAll request.
 */
public class ClientCacheInvokeAllRequest extends ClientCacheKeysRequest {
    /** */
    private final ClientCacheInvokeRequest.EntryProcessorReader entryProcReader;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientCacheInvokeAllRequest(BinaryReaderExImpl reader) {
        super(reader);

        entryProcReader = new ClientCacheInvokeRequest.EntryProcessorReader(reader);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        Map<Object, EntryProcessorResult<Object>> val = cache(ctx).invokeAll(keys(),
            entryProcReader.getEntryProcessor(), entryProcReader.getArgs(isKeepBinary()));

        return new ClientCacheInvokeAllResponse(requestId(), val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<ClientResponse> processAsync(ClientConnectionContext ctx) {
        return chainFuture(
            cache(ctx).invokeAllAsync(keys(), entryProcReader.getEntryProcessor(), entryProcReader.getArgs(isKeepBinary())),
            v -> new ClientCacheInvokeAllResponse(requestId(), v));
    }
}
