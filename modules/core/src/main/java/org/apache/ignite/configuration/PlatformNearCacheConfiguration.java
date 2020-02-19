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

package org.apache.ignite.configuration;

import java.io.Serializable;

/**
 * Platform near cache configuration.
 * <p>
 * Additional near caching mechanism on platform side (.NET).
 */
public class PlatformNearCacheConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Key type name. */
    private String keyTypeName;

    /** Value type name. */
    private String valueTypeName;

    /** Whether to cache binary objects. */
    private boolean keepBinary;

    /**
     * Gets fully-qualified platform type name of the cache key used for the local map.
     * When not set, non-generic map is used, which can reduce performance and increase allocations.
     *
     * @return
     */
    public String getKeyTypeName() {
        return keyTypeName;
    }

    /**
     * TODO
     * @param keyTypeName
     */
    public void setKeyTypeName(String keyTypeName) {
        this.keyTypeName = keyTypeName;
    }

    /**
     * TODO
     * @return
     */
    public String getValueTypeName() {
        return valueTypeName;
    }

    /**
     * TODO
     * @param valueTypeName
     */
    public void setValueTypeName(String valueTypeName) {
        this.valueTypeName = valueTypeName;
    }

    /**
     * TODO
     * @return
     */
    public boolean isKeepBinary() {
        return keepBinary;
    }

    /**
     * TODO
     * @param keepBinary
     */
    public void setKeepBinary(boolean keepBinary) {
        this.keepBinary = keepBinary;
    }
}
