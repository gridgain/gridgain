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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientProtocolContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Cache configuration request.
 */
public class ClientCacheGetConfigurationRequest extends ClientCacheRequest {
    /** Client protocol context. */
    private final ClientProtocolContext protocolCtx;

    /**
     * Constructor.
     *
     * @param reader Reader.
     * @param protocolCtx Client protocol context.
     */
    public ClientCacheGetConfigurationRequest(BinaryRawReader reader, ClientProtocolContext protocolCtx) {
        super(reader);

        this.protocolCtx = protocolCtx;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        CacheConfiguration cfg = ((IgniteCache<Object, Object>) rawCache(ctx))
                .getConfiguration(CacheConfiguration.class);

        return new ClientCacheGetConfigurationResponse(requestId(), cfg, protocolCtx);
    }
}
