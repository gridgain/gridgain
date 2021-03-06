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

package org.apache.ignite.internal.processors.cache.extras;

import org.apache.ignite.internal.processors.cache.GridCacheMvcc;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Cache extras.
 */
public interface GridCacheEntryExtras {
    /**
     * @return MVCC.
     */
    @Nullable public GridCacheMvcc mvcc();

    /**
     * @param mvcc MVCC.
     * @return Updated extras.
     */
    public GridCacheEntryExtras mvcc(GridCacheMvcc mvcc);

    /**
     * @return Obsolete version.
     */
    @Nullable public GridCacheVersion obsoleteVersion();

    /**
     * @param obsoleteVer Obsolete version.
     * @return Updated extras.
     */
    public GridCacheEntryExtras obsoleteVersion(GridCacheVersion obsoleteVer);

    /**
     * @return TTL.
     */
    public long ttl();

    /**
     * @return Expire time.
     */
    public long expireTime();

    /**
     * @param ttl TTL.
     * @param expireTime Expire time.
     * @return Updated extras.
     */
    public GridCacheEntryExtras ttlAndExpireTime(long ttl, long expireTime);

    /**
     * @return Extras size.
     */
    public int size();
}