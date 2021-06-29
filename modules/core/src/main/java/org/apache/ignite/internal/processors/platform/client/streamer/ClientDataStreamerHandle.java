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

package org.apache.ignite.internal.processors.platform.client.streamer;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.platform.client.ClientCloseableResource;

/**
 * Streamer handle.
 */
public class ClientDataStreamerHandle implements ClientCloseableResource {
    /** */
    private final IgniteDataStreamer<KeyCacheObject, CacheObject> streamer;

    /**
     * Ctor.
     *
     * @param streamer Streamer instance.
     */
    public ClientDataStreamerHandle(IgniteDataStreamer<KeyCacheObject, CacheObject> streamer) {
        assert streamer != null;

        this.streamer = streamer;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        streamer.close(true);
    }

    /**
     * Gets the wrapped streamer.
     *
     * @return Wrapped streamer.
     */
    public IgniteDataStreamer<KeyCacheObject, CacheObject> getStreamer() {
        return streamer;
    }
}
