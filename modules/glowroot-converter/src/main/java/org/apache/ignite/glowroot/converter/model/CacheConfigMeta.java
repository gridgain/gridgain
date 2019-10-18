/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.glowroot.converter.model;

/**
 * Cache configuration trace item.
 */
public class CacheConfigMeta
{

    /** Cache name. **/
    private final String cacheName;

    /** Cachne Configuration. **/
    private final String cfg;

    /**
     * Constructor.
     *
     * @param cacheName Cache name.
     * @param cfg Cache configuration.
     */
    public CacheConfigMeta(String cacheName, String cfg) {
        this.cacheName = cacheName;
        this.cfg = cfg;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Cachne Configuration.
     */
    public String config() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CacheConfigMeta{" +
            "cacheName='" + cacheName + '\'' +
            ", cfg='" + cfg + '\'' +
            '}';
    }
}
