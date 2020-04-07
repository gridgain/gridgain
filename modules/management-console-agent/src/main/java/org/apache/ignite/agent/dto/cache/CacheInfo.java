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

package org.apache.ignite.agent.dto.cache;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DTO for cache info.
 */
public class CacheInfo {
    /** Cache name. */
    private String cacheName;

    /** Cache ID. */
    private int cacheId;

    /** Created by sql. */
    private boolean createdBySql;

    /** Cache group. */
    private String cacheGroup;

    /** System cache. */
    private boolean sysCache;

    /**
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Name.
     * @return {@code This} for chaining method calls.
     */
    public CacheInfo setCacheName(String cacheName) {
        this.cacheName = cacheName;

        return this;
    }

    /**
     * @return Cache ID.
     */
    public int getCacheId() {
        return cacheId;
    }

    /**
     * @param cacheId Cache ID.
     * @return {@code This} for chaining method calls.
     */
    public CacheInfo setCacheId(int cacheId) {
        this.cacheId = cacheId;

        return this;
    }

    /**
     * @return Cache group.
     */
    public String getCacheGroup() {
        return cacheGroup;
    }

    /**
     * @param cacheGroup Group.
     * @return {@code This} for chaining method calls.
     */
    public CacheInfo setCacheGroup(String cacheGroup) {
        this.cacheGroup = cacheGroup;

        return this;
    }

    /**
     * @return {@code True} if cache was create by SQL query.
     */
    public boolean isCreatedBySql() {
        return createdBySql;
    }

    /**
     * @param createdBySql Created by sql.
     * @return {@code This} for chaining method calls.
     */
    public CacheInfo setCreatedBySql(boolean createdBySql) {
        this.createdBySql = createdBySql;

        return this;
    }

    /**
     * @return {@code True} if system cache.
     */
    public boolean isSystemCache() {
        return sysCache;
    }

    /**
     * @param sysCache System cache.
     * @return {@code This} for chaining method calls.
     */
    public CacheInfo setSystemCache(boolean sysCache) {
        this.sysCache = sysCache;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheInfo.class, this);
    }
}
