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

package org.apache.ignite.internal.processors.cache.tree.updatelog;

import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;

/**
 *
 */
public final class CacheIdAwareUpdateLogInnerIO extends AbstractUpdateLogInnerIO {
    /** */
    public static final IOVersions<CacheIdAwareUpdateLogInnerIO> VERSIONS = new IOVersions<>(
        new CacheIdAwareUpdateLogInnerIO(1)
    );

    /**
     * @param ver Page format version.
     */
    private CacheIdAwareUpdateLogInnerIO(int ver) {
        super(T_CACHE_ID_AWARE_UPDATE_LOG_REF_INNER & 0xFFFF, ver, true, 20);
    }

    /** {@inheritDoc} */
    @Override public int getCacheId(long pageAddr, int idx) {
        return PageUtils.getInt(pageAddr, offset(idx) + 16);
    }

    /** {@inheritDoc} */
    @Override protected boolean storeCacheId() {
        return true;
    }
}
