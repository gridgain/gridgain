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

package org.apache.ignite.internal.processors.platform.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheManager;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Platform cache manager - delegates functionality to native platforms (.NET, C++, ...).
 */
public class PlatformCacheManager implements GridCacheManager {
    /** */
    private GridCacheContext cctx;

    /** {@inheritDoc} */
    @Override public void start(GridCacheContext cctx) throws IgniteCheckedException {
        assert cctx != null;

        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel, boolean destroy) {
        // TODO: Drop near cache data.
        System.out.println("PlatformCacheManager.stop: " + cctx.name());
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        // TODO: Drop near cache data? Which method is actually called?
        System.out.println("PlatformCacheManager.onKernalStop: " + cctx.name());
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture reconnectFut) {
        // TODO: Drop near cache data.
        System.out.println("PlatformCacheManager.onDisconnected: " + cctx.name());
    }
}
