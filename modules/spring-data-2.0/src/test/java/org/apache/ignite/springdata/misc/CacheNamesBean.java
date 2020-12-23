/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.springdata.misc;

/**
 * The bean with cache names
 */
public class CacheNamesBean {
    /** Cache name for persons. */
    private String personCacheName;

    /**
     * Get name of the cache for persons.
     *
     * @return Name of cache.
     */
    public String getPersonCacheName() {
        return personCacheName;
    }

    /**
     * Set name of the cache for persons.
     * @param personCacheName Name of cache.
     */
    public void setPersonCacheName(String personCacheName) {
        this.personCacheName = personCacheName;
    }
}
