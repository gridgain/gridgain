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

import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientProtocolContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;

/**
 * Cache create with configuration request.
 */
@SuppressWarnings("unchecked")
public class ClientCacheCreateWithConfigurationRequest extends ClientRequest {
    /** Cache configuration. */
    private final CacheConfiguration cacheCfg;

    /**
     * Constructor.
     *
     * @param reader Reader.
     * @param protocolCtx Client protocol context.
     * @param pluginProc Plugin processor.
     */
    public ClientCacheCreateWithConfigurationRequest(
            BinaryReaderExImpl reader,
            ClientProtocolContext protocolCtx,
            IgnitePluginProcessor pluginProc) {
        super(reader);

        cacheCfg = ClientCacheConfigurationSerializer.read(reader, protocolCtx, pluginProc);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        try {
            ctx.kernalContext().grid().createCache(cacheCfg);
        }
        catch (CacheExistsException e) {
            throw new IgniteClientException(ClientStatus.CACHE_EXISTS, e.getMessage());
        }

        return super.process(ctx);
    }
}
